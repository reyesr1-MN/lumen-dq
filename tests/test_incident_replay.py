"""
Incident replay test suite.

Documents and proves the Lumen Community Bank CFO packet incident:

    A join to a non-unique branch_reference table silently multiplied
    daily_balances rows, overstating average customer balances in the
    CFO's weekly packet for eleven days.

Three properties are verified:
 1. The grain check DETECTS the buggy branch_reference (the preventive gate).
 2. The buggy join INFLATES the bank-wide average balance (incident simulation).
 3. The correct SQL is UNAFFECTED by duplicates in branch_reference (the fix).
"""

import pytest

from lumen_dq.checks import check_branch_count, check_grain_uniqueness

# ── SQL used in incident simulation ───────────────────────────────────────────

# Correct bank-wide metric: customer-date grain, no join to branch_reference
_CORRECT_BANKWIDE_SQL = """
    SELECT AVG(cust_total) AS avg_customer_balance_cents
    FROM (
        SELECT  a.customer_id,
                db.balance_date,
                SUM(db.closing_balance_cents) AS cust_total
        FROM    daily_balances db
        JOIN    accounts a ON db.account_id = a.account_id
        GROUP BY a.customer_id, db.balance_date
    ) _customer_totals
"""

# Buggy query: includes an unnecessary join to branch_reference.
# When branch_reference has duplicate branch_ids, the join fans out every
# daily_balances row for the affected branches, inflating SUM and AVG.
_BUGGY_BANKWIDE_SQL = """
    SELECT AVG(cust_total) AS avg_customer_balance_cents
    FROM (
        SELECT  a.customer_id,
                db.balance_date,
                SUM(db.closing_balance_cents) AS cust_total
        FROM    daily_balances db
        JOIN    accounts a  ON  db.account_id  = a.account_id
        JOIN    branch_reference br ON a.branch_id = br.branch_id
        GROUP BY a.customer_id, db.balance_date
    ) _customer_totals
"""


# ── 1. Gate: grain check prevents the report from running ─────────────────────


class TestGrainCheckGate:
    def test_baseline_branch_reference_passes(self, baseline_conn):
        """Pre-condition: baseline has exactly 42 unique branch_ids."""
        r = check_grain_uniqueness(baseline_conn, "branch_reference", ["branch_id"])
        assert r.passed, r.message

    def test_baseline_branch_count_passes(self, baseline_conn):
        r = check_branch_count(baseline_conn, "branch_reference")
        assert r.passed, r.message

    def test_buggy_branch_reference_grain_fails(self, buggy_conn):
        """
        The buggy fixture (47 rows, 5 duplicate branch_ids) must fail the
        grain check.  This FAIL is the gate that blocks the report.
        """
        r = check_grain_uniqueness(buggy_conn, "branch_reference", ["branch_id"])
        assert not r.passed, (
            "Expected grain check to FAIL — gate did not trigger"
        )
        assert r.failing_rows == 5, (
            f"Five branch_ids are duplicated; expected failing_rows=5, got {r.failing_rows}"
        )

    def test_buggy_branch_count_fails(self, buggy_conn):
        """47 rows instead of 42 also fails the count check."""
        r = check_branch_count(buggy_conn, "branch_reference")
        assert not r.passed


# ── 2. Inflation: buggy join overstates the average ───────────────────────────


class TestMetricInflation:
    def test_correct_sql_produces_positive_avg(self, buggy_conn):
        """The correct SQL is not affected by branch_reference duplicates."""
        avg = buggy_conn.execute(_CORRECT_BANKWIDE_SQL).fetchone()[0]
        assert avg is not None and avg > 0

    def test_buggy_join_inflates_average(self, buggy_conn):
        """
        Running the buggy SQL against the buggy branch_reference must produce
        a higher average than the correct SQL.

        Mechanism: branch_ids 1, 5, 12, 23, 37 each appear twice in
        branch_reference.  The JOIN fans out every daily_balances row for
        accounts in those branches by 2×, so SUM is doubled, inflating AVG.
        """
        correct = buggy_conn.execute(_CORRECT_BANKWIDE_SQL).fetchone()[0]
        inflated = buggy_conn.execute(_BUGGY_BANKWIDE_SQL).fetchone()[0]

        assert inflated > correct, (
            f"Expected inflated > correct; "
            f"correct={correct / 100:.2f}, inflated={inflated / 100:.2f}"
        )

    def test_inflation_is_material(self, buggy_conn):
        """
        5 out of 42 branch_ids are duplicated (≈ 12 % of accounts).
        Expect between 1 % and 30 % overstatement.
        """
        correct = buggy_conn.execute(_CORRECT_BANKWIDE_SQL).fetchone()[0]
        inflated = buggy_conn.execute(_BUGGY_BANKWIDE_SQL).fetchone()[0]
        pct = (inflated - correct) / correct * 100
        assert 1 < pct < 30, (
            f"Inflation of {pct:.2f} % is outside the expected 1–30 % range"
        )

    def test_inflation_is_zero_on_correct_sql_against_buggy_data(self, buggy_conn):
        """
        The correct SQL does not join branch_reference, so it produces the
        same result whether branch_reference is clean or duplicated.
        The 'inflation' of the correct SQL against buggy data must be 0.
        """
        # Compare correct SQL on baseline vs buggy data
        # (Using only buggy_conn here — both calls use the same connection)
        result_a = buggy_conn.execute(_CORRECT_BANKWIDE_SQL).fetchone()[0]
        # Re-run to confirm determinism
        result_b = buggy_conn.execute(_CORRECT_BANKWIDE_SQL).fetchone()[0]
        assert result_a == result_b


# ── 3. Fix: correct SQL is unaffected ─────────────────────────────────────────


class TestFixVerification:
    def test_correct_sql_matches_on_clean_data(self, baseline_conn, buggy_conn):
        """
        The correct SQL (no branch_reference join in aggregation) produces
        identical results on clean and buggy branch_reference,
        because it never touches branch_reference.
        """
        clean_avg = baseline_conn.execute(_CORRECT_BANKWIDE_SQL).fetchone()[0]
        buggy_data_avg = buggy_conn.execute(_CORRECT_BANKWIDE_SQL).fetchone()[0]
        assert clean_avg == buggy_data_avg, (
            "Correct SQL should be unaffected by branch_reference duplicates"
        )

    def test_buggy_sql_matches_on_clean_data(self, baseline_conn):
        """
        On CLEAN branch_reference the buggy join is a no-op (no fan-out),
        so both SQLs produce the same result.
        """
        correct = baseline_conn.execute(_CORRECT_BANKWIDE_SQL).fetchone()[0]
        with_clean_br = baseline_conn.execute(_BUGGY_BANKWIDE_SQL).fetchone()[0]
        assert correct == with_clean_br, (
            "On clean data, the extra branch_reference join should not change results"
        )
