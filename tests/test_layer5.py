"""
Layer 5 – Join and report reconciliation tests.

Checks:
  branch join is many-to-one before the report runs
  fact row count preserved after branch enrichment join
  no unmatched branches
  balance total preserved after branch join
  metric reconciliation (report avg vs independent avg)

Root cause vs. downstream symptom:
  Duplicate branch_reference.branch_id values are the ROOT CAUSE.
  Inflated report metrics (overstated avg customer balance) are a
  DOWNSTREAM SYMPTOM; they are caused by the fan-out but cannot be
  fixed at the report layer without first fixing the dimension.

Hand-calculated fixture
-----------------------
A tiny 3-account, 2-date, 2-branch dataset whose expected values are
computed explicitly here, avoiding the risk of validating SQL against
an identical implementation of the same mistake.

Dataset:
  branch 1 (North): accounts A-001 (Alice) + A-002 (Alice)
  branch 2 (South): account  A-003 (Bob)

Balances (cents):
  A-001 | 2024-01-01 |  10 000
  A-001 | 2024-01-02 |  11 000
  A-002 | 2024-01-01 |   5 000
  A-002 | 2024-01-02 |   6 000
  A-003 | 2024-01-01 |  20 000
  A-003 | 2024-01-02 |  22 000

Hand-calculated correct values:
  Alice totals   | 01-01: 15 000  | 01-02: 17 000
  Bob totals     | 01-01: 20 000  | 01-02: 22 000
  Bank avg (all customer-date pairs): (15000+17000+20000+22000)/4 = 18 500

With buggy branch_reference (branch_id=1 duplicated):
  Fan-out: each daily_balances row for A-001/A-002 appears twice.
  Alice after GROUP BY customer_id, balance_date:
    01-01: (10000+10000)+(5000+5000) = 30 000  (was 15 000)
    01-02: (11000+11000)+(6000+6000) = 34 000  (was 17 000)
  Bob: unaffected (branch_id=2, not duplicated)
  Bank avg (buggy): (30000+34000+20000+22000)/4 = 26 500  (was 18 500)
  Overstatement: (26500−18500)/18500 ≈ 43.2 %

  Fact row count before join:  6
  Fact row count after buggy:  A-001×2 + A-002×2 + A-003×1 = 4+4+2 = 10
  Inflation:                   4 extra rows (+66.7 %)

  Balance total before join:   74 000
  Balance total after buggy:   10000×2+11000×2+5000×2+6000×2+20000+22000
                              = 20000+22000+10000+12000+20000+22000 = 106 000
  Overstatement:               32 000 cents (+43.2 %)
"""

from __future__ import annotations

from datetime import date, datetime, timezone

import duckdb
import pytest

from lumen_dq.validators import (
    Severity,
    check_balance_total_preserved_after_join,
    check_branch_join_is_many_to_one,
    check_fact_row_count_after_join,
    check_metric_reconciliation,
    check_unmatched_branches,
)


# ── Hand-calculated fixture ────────────────────────────────────────────────────

# Known correct values (hand-calculated above)
_HC_FACT_ROWS_BEFORE    = 6
_HC_FACT_ROWS_BUGGY     = 10          # 4+4+2
_HC_TOTAL_BEFORE_CENTS  = 74_000      # 10+11+5+6+20+22 × 1000
_HC_TOTAL_BUGGY_CENTS   = 106_000     # fan-out doubles branch-1 rows
_HC_AVG_CORRECT_CENTS   = 18_500      # (15+17+20+22)/4 × 1000
_HC_AVG_BUGGY_CENTS     = 26_500      # (30+34+20+22)/4 × 1000

_INGESTED = datetime(2024, 1, 30, 12, 0, 0, tzinfo=timezone.utc)


def _build_clean_conn() -> duckdb.DuckDBPyConnection:
    """Tiny clean dataset — branch_reference is unique."""
    conn = duckdb.connect()
    conn.execute("""
        CREATE TABLE branch_reference (
            branch_id INTEGER, branch_name VARCHAR, city VARCHAR, state_code VARCHAR
        )
    """)
    conn.executemany(
        "INSERT INTO branch_reference VALUES (?,?,?,?)",
        [(1, "North", "Springfield", "IL"), (2, "South", "Shelbyville", "IL")],
    )

    conn.execute("""
        CREATE TABLE customers (
            customer_id VARCHAR, customer_name VARCHAR, created_date DATE
        )
    """)
    conn.executemany(
        "INSERT INTO customers VALUES (?,?,?)",
        [("C-001", "Alice", date(2020, 1, 1)), ("C-002", "Bob", date(2020, 1, 1))],
    )

    conn.execute("""
        CREATE TABLE accounts (
            account_id VARCHAR, customer_id VARCHAR, branch_id INTEGER,
            product_type VARCHAR, account_status VARCHAR, currency VARCHAR,
            opened_date DATE, closed_date DATE, ingested_at TIMESTAMP
        )
    """)
    conn.executemany(
        "INSERT INTO accounts VALUES (?,?,?,?,?,?,?,?,?)",
        [
            ("A-001", "C-001", 1, "CHECKING", "ACTIVE", "USD",
             date(2020, 1, 2), None, _INGESTED),
            ("A-002", "C-001", 1, "SAVINGS",  "ACTIVE", "USD",
             date(2020, 1, 2), None, _INGESTED),
            ("A-003", "C-002", 2, "CHECKING", "ACTIVE", "USD",
             date(2020, 1, 2), None, _INGESTED),
        ],
    )

    conn.execute("""
        CREATE TABLE daily_balances (
            account_id VARCHAR, balance_date DATE, closing_balance_cents BIGINT
        )
    """)
    conn.executemany(
        "INSERT INTO daily_balances VALUES (?,?,?)",
        [
            ("A-001", date(2024, 1, 1), 10_000),
            ("A-001", date(2024, 1, 2), 11_000),
            ("A-002", date(2024, 1, 1),  5_000),
            ("A-002", date(2024, 1, 2),  6_000),
            ("A-003", date(2024, 1, 1), 20_000),
            ("A-003", date(2024, 1, 2), 22_000),
        ],
    )
    return conn


def _build_buggy_conn() -> duckdb.DuckDBPyConnection:
    """Same dataset but branch_id=1 appears twice in branch_reference."""
    conn = _build_clean_conn()
    # Add duplicate row for branch_id=1
    conn.execute(
        "INSERT INTO branch_reference VALUES (1,'North-DUP','Springfield','IL')"
    )
    return conn


@pytest.fixture(scope="module")
def hc_clean():
    """Hand-calculated clean fixture."""
    return _build_clean_conn()


@pytest.fixture(scope="module")
def hc_buggy():
    """Hand-calculated buggy fixture (branch_id=1 duplicated)."""
    return _build_buggy_conn()


# ── Positive: baseline passes all Layer 5 checks ──────────────────────────────


class TestLayer5Baseline:
    def test_branch_join_many_to_one(self, baseline_conn):
        r = check_branch_join_is_many_to_one(baseline_conn)
        assert r.passed, r.observed

    def test_fact_row_count_preserved(self, baseline_conn):
        r = check_fact_row_count_after_join(baseline_conn)
        assert r.passed, r.observed

    def test_unmatched_branches_zero(self, baseline_conn):
        r = check_unmatched_branches(baseline_conn)
        assert r.passed, r.observed

    def test_balance_total_preserved(self, baseline_conn):
        r = check_balance_total_preserved_after_join(baseline_conn)
        assert r.passed, r.observed

    def test_metric_reconciles(self, baseline_conn):
        r = check_metric_reconciliation(baseline_conn)
        assert r.passed, r.observed


# ── Negative: buggy fixture (baseline + duplicate branch_id) ─────────────────


class TestLayer5BuggyFixture:
    def test_branch_join_not_many_to_one(self, buggy_conn):
        r = check_branch_join_is_many_to_one(buggy_conn)
        assert not r.passed, "Expected FAIL — duplicate branch_ids present"

    def test_branch_join_failing_rows_is_5(self, buggy_conn):
        """The baseline buggy fixture duplicates 5 branch_ids."""
        r = check_branch_join_is_many_to_one(buggy_conn)
        assert r.failing_rows == 5

    def test_branch_join_affected_keys_populated(self, buggy_conn):
        r = check_branch_join_is_many_to_one(buggy_conn)
        assert not r.passed
        assert r.affected_keys

    def test_branch_join_severity_blocking(self, buggy_conn):
        r = check_branch_join_is_many_to_one(buggy_conn)
        assert r.severity == Severity.BLOCKING

    def test_branch_join_pipeline_health_names_root_cause(self, buggy_conn):
        r = check_branch_join_is_many_to_one(buggy_conn)
        assert not r.passed
        health = r.pipeline_health.lower()
        assert "root cause" in health

    def test_fact_row_count_inflated(self, buggy_conn):
        r = check_fact_row_count_after_join(buggy_conn)
        assert not r.passed

    def test_balance_total_inflated(self, buggy_conn):
        r = check_balance_total_preserved_after_join(buggy_conn)
        assert not r.passed

    def test_metric_reconciliation_fails(self, buggy_conn):
        r = check_metric_reconciliation(buggy_conn)
        assert not r.passed


# ── Hand-calculated fixture: exact before/after numbers ───────────────────────


class TestHandCalculatedClean:
    """
    Verify Layer 5 checks on the tiny clean fixture against hand-calculated values.
    This proves the validators produce the correct numbers, not merely SQL that
    agrees with itself.
    """

    def test_branch_join_many_to_one(self, hc_clean):
        r = check_branch_join_is_many_to_one(hc_clean)
        assert r.passed

    def test_fact_row_count_before(self, hc_clean):
        actual = hc_clean.execute(
            "SELECT COUNT(*) FROM daily_balances"
        ).fetchone()[0]
        assert actual == _HC_FACT_ROWS_BEFORE

    def test_fact_row_count_after_join_preserved(self, hc_clean):
        r = check_fact_row_count_after_join(hc_clean)
        assert r.passed
        # observed field reports the count unchanged
        assert str(_HC_FACT_ROWS_BEFORE) in r.observed

    def test_balance_total_before(self, hc_clean):
        total = hc_clean.execute(
            "SELECT SUM(closing_balance_cents) FROM daily_balances"
        ).fetchone()[0]
        assert total == _HC_TOTAL_BEFORE_CENTS, (
            f"Hand-calculated total: {_HC_TOTAL_BEFORE_CENTS}; got {total}"
        )

    def test_balance_total_preserved_after_join(self, hc_clean):
        r = check_balance_total_preserved_after_join(hc_clean)
        assert r.passed

    def test_correct_bank_avg_cents(self, hc_clean):
        """Verify the hand-calculated bank-wide average against SQL."""
        avg = hc_clean.execute("""
            SELECT AVG(cust_total)
            FROM (
                SELECT a.customer_id, db.balance_date,
                       SUM(db.closing_balance_cents) AS cust_total
                FROM   daily_balances db
                JOIN   accounts a ON db.account_id = a.account_id
                GROUP BY a.customer_id, db.balance_date
            ) _
        """).fetchone()[0]
        assert round(avg) == _HC_AVG_CORRECT_CENTS, (
            f"Expected bank avg = {_HC_AVG_CORRECT_CENTS}; got {round(avg)}"
        )

    def test_metric_reconciliation_passes(self, hc_clean):
        r = check_metric_reconciliation(hc_clean)
        assert r.passed


class TestHandCalculatedBuggy:
    """
    Fan-out reproduction: actual before/after numbers from the hand-calculated
    buggy fixture.

    Root cause:  branch_id=1 appears twice in branch_reference.
    Mechanism:   The JOIN fans out every daily_balances row for A-001 and A-002
                 (both in branch 1) by 2×, silently doubling their contribution
                 to every SUM.
    Symptom:     Report avg jumps from 18 500 ¢ to 26 500 ¢ (+43.2 %).

    A-003 (branch 2) is NOT in the duplicated branch, so its contribution is
    unchanged — this proves the inflation is selective and proportional to the
    duplication factor, not a blanket multiplier.
    """

    def test_branch_join_not_many_to_one(self, hc_buggy):
        r = check_branch_join_is_many_to_one(hc_buggy)
        assert not r.passed
        assert r.failing_rows == 1           # one extra row (branch_id=1 duplicated)
        assert 1 in r.affected_keys          # branch_id=1 is the duplicate

    def test_fact_row_count_inflated_by_4(self, hc_buggy):
        """
        Before: 6 rows.
        After:  A-001 × 2 dates × 2 branch rows = 4
              + A-002 × 2 dates × 2 branch rows = 4
              + A-003 × 2 dates × 1 branch row  = 2
              Total = 10 (+4 extra).
        """
        r = check_fact_row_count_after_join(hc_buggy)
        assert not r.passed
        assert r.failing_rows == _HC_FACT_ROWS_BUGGY - _HC_FACT_ROWS_BEFORE  # = 4

    def test_fact_row_count_exact_after_join(self, hc_buggy):
        after = hc_buggy.execute("""
            SELECT COUNT(*)
            FROM daily_balances db
            JOIN accounts a ON db.account_id = a.account_id
            JOIN branch_reference br ON a.branch_id = br.branch_id
        """).fetchone()[0]
        assert after == _HC_FACT_ROWS_BUGGY

    def test_balance_total_inflated(self, hc_buggy):
        r = check_balance_total_preserved_after_join(hc_buggy)
        assert not r.passed

    def test_balance_total_exact_before(self, hc_buggy):
        total = hc_buggy.execute(
            "SELECT SUM(closing_balance_cents) FROM daily_balances"
        ).fetchone()[0]
        assert total == _HC_TOTAL_BEFORE_CENTS

    def test_balance_total_exact_after_buggy_join(self, hc_buggy):
        total = hc_buggy.execute("""
            SELECT SUM(db.closing_balance_cents)
            FROM daily_balances db
            JOIN accounts a ON db.account_id = a.account_id
            JOIN branch_reference br ON a.branch_id = br.branch_id
        """).fetchone()[0]
        assert total == _HC_TOTAL_BUGGY_CENTS, (
            f"Expected post-join total {_HC_TOTAL_BUGGY_CENTS}; got {total}"
        )

    def test_bob_unaffected_by_fan_out(self, hc_buggy):
        """
        A-003 (Bob, branch 2) must contribute the same total before and after the
        buggy join — proving the inflation is caused by the duplicated branch_id=1,
        not by any change to fact data.
        """
        before_bob = hc_buggy.execute("""
            SELECT SUM(closing_balance_cents) FROM daily_balances
            WHERE account_id = 'A-003'
        """).fetchone()[0]

        after_bob = hc_buggy.execute("""
            SELECT SUM(db.closing_balance_cents)
            FROM daily_balances db
            JOIN accounts a ON db.account_id = a.account_id
            JOIN branch_reference br ON a.branch_id = br.branch_id
            WHERE db.account_id = 'A-003'
        """).fetchone()[0]

        assert before_bob == after_bob, (
            "Bob's total must be unaffected because branch_id=2 is not duplicated"
        )

    def test_alice_doubled_by_fan_out(self, hc_buggy):
        """
        A-001 and A-002 (Alice, branch 1) must each be doubled.
        """
        before_alice = hc_buggy.execute("""
            SELECT SUM(closing_balance_cents) FROM daily_balances
            WHERE account_id IN ('A-001','A-002')
        """).fetchone()[0]

        after_alice = hc_buggy.execute("""
            SELECT SUM(db.closing_balance_cents)
            FROM daily_balances db
            JOIN accounts a ON db.account_id = a.account_id
            JOIN branch_reference br ON a.branch_id = br.branch_id
            WHERE db.account_id IN ('A-001','A-002')
        """).fetchone()[0]

        assert after_alice == 2 * before_alice, (
            f"Alice's totals should double due to fan-out; "
            f"before={before_alice} after={after_alice}"
        )

    def test_buggy_bank_avg_exact(self, hc_buggy):
        avg = hc_buggy.execute("""
            SELECT AVG(cust_total)
            FROM (
                SELECT a.customer_id, db.balance_date,
                       SUM(db.closing_balance_cents) AS cust_total
                FROM   daily_balances db
                JOIN   accounts a ON db.account_id = a.account_id
                JOIN   branch_reference br ON a.branch_id = br.branch_id
                GROUP BY a.customer_id, db.balance_date
            ) _
        """).fetchone()[0]
        assert round(avg) == _HC_AVG_BUGGY_CENTS, (
            f"Expected buggy avg = {_HC_AVG_BUGGY_CENTS}; got {round(avg)}"
        )

    def test_metric_reconciliation_fails_with_correct_diff(self, hc_buggy):
        r = check_metric_reconciliation(hc_buggy)
        assert not r.passed
        # observed field uses comma-formatted cents (e.g. "26,500")
        assert f"{_HC_AVG_BUGGY_CENTS:,}" in r.observed
        assert f"{_HC_AVG_CORRECT_CENTS:,}" in r.observed

    def test_overstatement_percentage(self, hc_buggy):
        """
        Overstatement: (26500 − 18500) / 18500 ≈ 43.24 %.
        The pipeline_health field must quantify this.
        """
        r = check_metric_reconciliation(hc_buggy)
        assert not r.passed
        assert "43." in r.pipeline_health or "43%" in r.pipeline_health


# ── Root-cause / symptom framing ──────────────────────────────────────────────


class TestRootCauseFraming:
    def test_root_cause_is_duplicate_branch_id(self, buggy_conn):
        """
        The branch-join check (L5.branch_join_many_to_one) must name the root
        cause explicitly in pipeline_health.
        """
        r = check_branch_join_is_many_to_one(buggy_conn)
        assert not r.passed
        assert "root cause" in r.pipeline_health.lower()

    def test_symptom_is_overstated_metric(self, buggy_conn):
        """
        The metric-reconciliation check must name the root cause as duplicate
        branch_ids, not describe inflation as an independent defect.
        """
        r = check_metric_reconciliation(buggy_conn)
        assert not r.passed
        health = r.pipeline_health.lower()
        assert "root cause" in health or "branch" in health

    def test_next_step_says_block_report(self, buggy_conn):
        """
        When the branch join is unsafe, next_step must advise blocking the report.
        """
        r = check_branch_join_is_many_to_one(buggy_conn)
        assert not r.passed
        assert "block" in r.next_step.lower()

    def test_layer_is_5(self, baseline_conn):
        r = check_branch_join_is_many_to_one(baseline_conn)
        assert r.layer == 5
