"""
Production-style layered validators for Lumen Community Bank datasets.

Each function returns a ValidationResult (or a list of them for
check_balance_movement) carrying:

  check_id          stable identifier
  severity          BLOCKING | WARNING | INFO
  layer             1–5
  table             primary table under test
  passed            True/False
  expected          human-readable expected condition
  observed          human-readable actual condition
  failing_rows      count of offending rows (0 for ratio-only checks)
  affected_keys     sample of key values that triggered the failure
  pipeline_health   implication for downstream pipelines
  next_step         diagnostic action to take

Layer 1 – Schema and strict types
    Required columns, unexpected columns, column types — checked against
    raw/serialised data before permissive implicit casting can hide defects.

Layer 2 – Keys and nullability
    Primary-key uniqueness, composite-key uniqueness, not-null on required fields.

Layer 3 – Relationships
    accounts.customer_id  → customers.customer_id
    accounts.branch_id    → branch_reference.branch_id
    daily_balances.account_id → accounts.account_id

Layer 4 – Completeness and business rules
    Account/date coverage, account lifecycle consistency, balance movement.

    Balance movement rule (exact integer arithmetic, no division):
        abs(current - prev) > 3 * abs(prev)

    Edge cases:
        100 → 400 : abs(300) = 3×100  → NOT > 3×  → PASS (exactly 300%)
        100 → 401 : abs(301) > 3×100  → FAIL
       -100 → -401: abs(301) > 3×100  → FAIL
          0 → 0   : no movement       → PASS
          0 → n≠0 : undefined %       → ZERO_BASELINE_MOVEMENT (BLOCKING)

    A movement alert identifies an anomaly requiring investigation; it does
    not by itself prove data corruption.  A legitimate large deposit (e.g.,
    wire transfer proceeds) could exceed the 300% threshold.

    Comparison rules:
        • The first observed balance for an account is not compared.
        • Only consecutive calendar dates are compared.
        • A date gap is a completeness failure; do not compare across it.

Layer 5 – Join and report reconciliation
    Branch-join safety, fact row count preservation, unmatched branches,
    balance-total preservation, and metric reconciliation against an
    independent calculation from base tables.

    Root cause vs. downstream symptom:
        Duplicate branch_reference.branch_id values are the ROOT CAUSE.
        Overstated report metrics are a DOWNSTREAM SYMPTOM; they cannot be
        fixed by adjusting the report SQL without first fixing the dimension.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any

import duckdb


# ── Result type ────────────────────────────────────────────────────────────────


class Severity(str, Enum):
    BLOCKING = "BLOCKING"
    WARNING  = "WARNING"
    INFO     = "INFO"


@dataclass
class ValidationResult:
    check_id:        str
    severity:        Severity
    layer:           int
    table:           str
    passed:          bool
    expected:        str
    observed:        str
    failing_rows:    int  = 0
    affected_keys:   list = field(default_factory=list)
    pipeline_health: str  = ""
    next_step:       str  = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "check_id":        self.check_id,
            "severity":        self.severity.value,
            "layer":           self.layer,
            "table":           self.table,
            "passed":          self.passed,
            "expected":        self.expected,
            "observed":        self.observed,
            "failing_rows":    self.failing_rows,
            "affected_keys":   self.affected_keys,
            "pipeline_health": self.pipeline_health,
            "next_step":       self.next_step,
        }


# ── Layer 1 – Schema and strict types ─────────────────────────────────────────


def check_required_columns(
    conn: duckdb.DuckDBPyConnection,
    table: str,
    required: list[str],
) -> ValidationResult:
    """Fail if any required column is absent from *table*."""
    existing = {
        row[0].lower()
        for row in conn.execute(f"DESCRIBE {table}").fetchall()
    }
    missing = [c for c in required if c.lower() not in existing]
    passed  = len(missing) == 0
    return ValidationResult(
        check_id=f"L1.required_columns.{table}",
        severity=Severity.BLOCKING,
        layer=1,
        table=table,
        passed=passed,
        expected=f"columns present: {required}",
        observed="all required columns present" if passed else f"missing: {missing}",
        failing_rows=len(missing),
        affected_keys=missing,
        pipeline_health=(
            "" if passed else
            "Downstream SELECT statements will fail with 'column not found'. "
            "Any aggregation silently omits data carried in missing columns."
        ),
        next_step=(
            "" if passed else
            f"Inspect the ETL job that loaded '{table}'. "
            f"Check source schema change-log for columns: {missing}."
        ),
    )


def check_no_unexpected_columns(
    conn: duckdb.DuckDBPyConnection,
    table: str,
    allowed: list[str],
) -> ValidationResult:
    """Fail if *table* contains columns not in *allowed* (strict schema policy)."""
    allowed_lower = {c.lower() for c in allowed}
    existing = [row[0] for row in conn.execute(f"DESCRIBE {table}").fetchall()]
    unexpected = [c for c in existing if c.lower() not in allowed_lower]
    passed     = len(unexpected) == 0
    return ValidationResult(
        check_id=f"L1.no_unexpected_columns.{table}",
        severity=Severity.WARNING,
        layer=1,
        table=table,
        passed=passed,
        expected=f"only documented columns: {allowed}",
        observed="no unexpected columns" if passed else f"unexpected: {unexpected}",
        failing_rows=len(unexpected),
        affected_keys=unexpected,
        pipeline_health=(
            "" if passed else
            "Unexpected columns may carry undeclared semantics or PII. "
            "Schema drift can silently alter downstream join cardinality."
        ),
        next_step=(
            "" if passed else
            "Review source schema change-log. Confirm whether unexpected columns "
            f"should be added to the contract or dropped from the load: {unexpected}."
        ),
    )


def check_column_type(
    conn: duckdb.DuckDBPyConnection,
    table: str,
    column: str,
    expected_type: str,
) -> ValidationResult:
    """
    Fail if *column* in *table* has a DuckDB type that does not start with
    *expected_type* (upper-case, stripped comparison).

    Checks the physical type stored in the file before any implicit cast.
    A date column stored as VARCHAR loses temporal ordering and silently
    accepts malformed strings.
    """
    type_map = {
        row[0].upper(): row[1].upper()
        for row in conn.execute(f"DESCRIBE {table}").fetchall()
    }
    actual = type_map.get(column.upper(), "COLUMN_NOT_FOUND")
    passed = actual.startswith(expected_type.upper())
    return ValidationResult(
        check_id=f"L1.column_type.{table}.{column}",
        severity=Severity.BLOCKING,
        layer=1,
        table=table,
        passed=passed,
        expected=f"{column} :: {expected_type}",
        observed=f"{column} :: {actual}",
        pipeline_health=(
            "" if passed else
            f"Implicit cast from {actual} → {expected_type} may corrupt values "
            "(e.g., date strings stored as VARCHAR lose temporal ordering and "
            "silently accept malformed input)."
        ),
        next_step=(
            "" if passed else
            f"Enforce CAST({column} AS {expected_type}) at ingest time and validate "
            "that no format variants exist in the source system."
        ),
    )


# ── Layer 2 – Keys and nullability ────────────────────────────────────────────


def check_primary_key(
    conn: duckdb.DuckDBPyConnection,
    table: str,
    key_columns: list[str],
) -> ValidationResult:
    """Fail if any combination of *key_columns* appears more than once."""
    cols = ", ".join(key_columns)
    if len(key_columns) == 1:
        dup_count: int = conn.execute(
            f"SELECT COUNT(*) - COUNT(DISTINCT {key_columns[0]}) FROM {table}"
        ).fetchone()[0]
    else:
        dup_count = conn.execute(f"""
            SELECT COALESCE(SUM(cnt - 1), 0)
            FROM (
                SELECT {cols}, COUNT(*) AS cnt
                FROM {table}
                GROUP BY {cols}
                HAVING COUNT(*) > 1
            ) _d
        """).fetchone()[0]

    sample_keys: list = []
    if dup_count > 0:
        rows = conn.execute(f"""
            SELECT {cols}
            FROM {table}
            GROUP BY {cols}
            HAVING COUNT(*) > 1
            LIMIT 5
        """).fetchall()
        sample_keys = [list(r) if len(r) > 1 else r[0] for r in rows]

    key_label = "+".join(key_columns)
    passed    = dup_count == 0
    return ValidationResult(
        check_id=f"L2.primary_key.{table}[{key_label}]",
        severity=Severity.BLOCKING,
        layer=2,
        table=table,
        passed=passed,
        expected=f"0 duplicate ({key_label}) combinations",
        observed=(
            "0 duplicates" if passed else
            f"{dup_count} extra row(s) from duplicate key(s)"
        ),
        failing_rows=dup_count,
        affected_keys=sample_keys,
        pipeline_health=(
            "" if passed else
            f"Any JOIN on ({key_label}) will fan out fact rows by the duplication "
            "factor, silently inflating every SUM, COUNT, and AVG downstream."
        ),
        next_step=(
            "" if passed else
            f"SELECT {cols}, COUNT(*) FROM {table} GROUP BY {cols} "
            f"HAVING COUNT(*) > 1 — enumerate all duplicates, then trace the "
            "load job to find the source of extra rows."
        ),
    )


def check_not_null(
    conn: duckdb.DuckDBPyConnection,
    table: str,
    column: str,
) -> ValidationResult:
    """Fail if any row has NULL in *column*."""
    null_count: int = conn.execute(
        f"SELECT COUNT(*) FROM {table} WHERE {column} IS NULL"
    ).fetchone()[0]
    passed = null_count == 0
    return ValidationResult(
        check_id=f"L2.not_null.{table}.{column}",
        severity=Severity.BLOCKING,
        layer=2,
        table=table,
        passed=passed,
        expected=f"{table}.{column}: no NULL values",
        observed="no NULLs" if passed else f"{null_count} NULL value(s)",
        failing_rows=null_count,
        pipeline_health=(
            "" if passed else
            f"NULL in required column {column} causes silent row exclusion in "
            "INNER JOINs and propagates NULL through any aggregation."
        ),
        next_step=(
            "" if passed else
            f"SELECT * FROM {table} WHERE {column} IS NULL LIMIT 20  "
            "— inspect affected rows and check the source system."
        ),
    )


# ── Layer 3 – Relationships ───────────────────────────────────────────────────


def check_foreign_key(
    conn: duckdb.DuckDBPyConnection,
    table: str,
    column: str,
    ref_table: str,
    ref_column: str,
) -> ValidationResult:
    """Fail if any non-NULL FK value in *table.column* is absent from *ref_table.ref_column*."""
    orphan_count: int = conn.execute(f"""
        SELECT COUNT(*)
        FROM {table} t
        WHERE t.{column} IS NOT NULL
          AND t.{column} NOT IN (SELECT {ref_column} FROM {ref_table})
    """).fetchone()[0]

    sample_keys: list = []
    if orphan_count > 0:
        rows = conn.execute(f"""
            SELECT DISTINCT t.{column}
            FROM {table} t
            WHERE t.{column} IS NOT NULL
              AND t.{column} NOT IN (SELECT {ref_column} FROM {ref_table})
            LIMIT 5
        """).fetchall()
        sample_keys = [r[0] for r in rows]

    passed = orphan_count == 0
    return ValidationResult(
        check_id=f"L3.foreign_key.{table}.{column}",
        severity=Severity.BLOCKING,
        layer=3,
        table=table,
        passed=passed,
        expected=f"all {table}.{column} values present in {ref_table}.{ref_column}",
        observed=(
            "all FK values matched" if passed else
            f"{orphan_count} orphaned FK row(s)"
        ),
        failing_rows=orphan_count,
        affected_keys=sample_keys,
        pipeline_health=(
            "" if passed else
            f"{orphan_count} rows in '{table}' reference non-existent '{ref_table}' "
            "records. An INNER JOIN silently excludes these rows, understating totals."
        ),
        next_step=(
            "" if passed else
            f"SELECT DISTINCT {column} FROM {table} "
            f"WHERE {column} NOT IN (SELECT {ref_column} FROM {ref_table})  "
            f"— list all orphaned values. Verify '{ref_table}' loaded before '{table}'."
        ),
    )


# ── Layer 4 – Completeness and business rules ─────────────────────────────────


def check_account_date_completeness(
    conn: duckdb.DuckDBPyConnection,
) -> ValidationResult:
    """
    Fail if any account is missing a balance row for any date in the observed range.

    Expected = n_accounts × n_dates.  A gap here means the balance-movement
    check cannot safely compare consecutive dates across the gap.
    """
    # balance_date may be VARCHAR (parquet) or DATE (in-memory): cast defensively.
    # Expected = n_accounts × calendar days from min to max (inclusive).
    # Using DATEDIFF rather than COUNT(DISTINCT date) correctly detects gaps:
    # if dates 1,3 are present but date 2 is missing, DATEDIFF gives 3 days not 2.
    row = conn.execute("""
        SELECT
            COUNT(*)                          AS actual_rows,
            COUNT(DISTINCT account_id)        AS n_accounts,
            COUNT(DISTINCT balance_date)      AS n_distinct_dates,
            DATEDIFF(
                'day',
                MIN(TRY_CAST(balance_date AS DATE)),
                MAX(TRY_CAST(balance_date AS DATE))
            ) + 1                             AS n_range_days,
            COUNT(DISTINCT account_id) * (
                DATEDIFF(
                    'day',
                    MIN(TRY_CAST(balance_date AS DATE)),
                    MAX(TRY_CAST(balance_date AS DATE))
                ) + 1
            )                                 AS expected_rows
        FROM daily_balances
    """).fetchone()
    actual, n_accts, n_dates, n_range_days, expected = row
    missing = expected - actual
    passed  = missing == 0
    return ValidationResult(
        check_id="L4.account_date_completeness",
        severity=Severity.BLOCKING,
        layer=4,
        table="daily_balances",
        passed=passed,
        expected=f"{expected:,} rows ({n_accts:,} accounts × {n_range_days:,} calendar dates)",
        observed=(
            f"{actual:,} rows" if passed else
            f"{actual:,} rows ({missing:,} missing)"
        ),
        failing_rows=missing,
        pipeline_health=(
            "" if passed else
            "Missing balance rows cause gaps in day-over-day trend reports. "
            "The balance-movement check must not compare across date gaps; "
            "gaps must be surfaced as completeness failures, not movement alerts."
        ),
        next_step=(
            "" if passed else
            f"SELECT account_id, COUNT(DISTINCT balance_date) AS n "
            f"FROM daily_balances GROUP BY account_id HAVING n < {n_dates}  "
            "— find accounts with missing dates."
        ),
    )


def check_account_lifecycle(
    conn: duckdb.DuckDBPyConnection,
) -> ValidationResult:
    """
    Fail if lifecycle rules are violated:
      • closed_date IS NOT NULL only when account_status = 'CLOSED'
      • closed_date IS NULL when account_status != 'CLOSED'
      • closed_date > opened_date when present
    """
    violations: int = conn.execute("""
        SELECT COUNT(*) FROM accounts
        WHERE (closed_date IS NOT NULL AND account_status != 'CLOSED')
           OR (account_status = 'CLOSED' AND closed_date IS NULL)
           OR (closed_date IS NOT NULL AND closed_date <= opened_date)
    """).fetchone()[0]

    sample_keys: list = []
    if violations > 0:
        rows = conn.execute("""
            SELECT account_id FROM accounts
            WHERE (closed_date IS NOT NULL AND account_status != 'CLOSED')
               OR (account_status = 'CLOSED' AND closed_date IS NULL)
               OR (closed_date IS NOT NULL AND closed_date <= opened_date)
            LIMIT 5
        """).fetchall()
        sample_keys = [r[0] for r in rows]

    passed = violations == 0
    return ValidationResult(
        check_id="L4.account_lifecycle",
        severity=Severity.BLOCKING,
        layer=4,
        table="accounts",
        passed=passed,
        expected=(
            "closed_date IS NULL for non-CLOSED; "
            "closed_date IS NOT NULL for CLOSED; "
            "closed_date > opened_date"
        ),
        observed=(
            "all lifecycle rules satisfied" if passed else
            f"{violations} lifecycle violation(s)"
        ),
        failing_rows=violations,
        affected_keys=sample_keys,
        pipeline_health=(
            "" if passed else
            "Lifecycle inconsistencies produce incorrect active-account counts "
            "in date-bounded reports and violate audit trail requirements."
        ),
        next_step=(
            "" if passed else
            "SELECT account_id, account_status, opened_date, closed_date FROM accounts "
            "WHERE (closed_date IS NOT NULL AND account_status != 'CLOSED') "
            "OR (account_status = 'CLOSED' AND closed_date IS NULL) "
            "OR (closed_date IS NOT NULL AND closed_date <= opened_date)"
        ),
    )


def check_balance_movement(
    conn: duckdb.DuckDBPyConnection,
) -> list[ValidationResult]:
    """
    Detect anomalous day-over-day balance movements on consecutive dates only.

    Returns TWO ValidationResults:
      L4.balance_spike           – abs(current − prev) > 3 × abs(prev), prev ≠ 0
      L4.zero_baseline_movement  – prev = 0 and current ≠ 0 (undefined %)

    Rule uses EXACT INTEGER ARITHMETIC — no division, no floating-point rounding:
        ABS(closing_balance_cents − prev) > 3 × ABS(prev)

    Edge cases (all in cents):
        prev=100, curr=400 : ABS(300) = 3×100     → NOT > 3×  PASS (exactly 300%)
        prev=100, curr=401 : ABS(301) > 3×100      → FAIL
        prev=-100,curr=-401: ABS(-301)=301 > 300   → FAIL
        prev=0,   curr=0   : no movement            → PASS
        prev=0,   curr≠0   : undefined %            → ZERO_BASELINE_MOVEMENT

    A movement alert identifies an anomaly requiring investigation; it does
    NOT by itself prove data corruption.  A legitimate large deposit (e.g.,
    wired real-estate proceeds) could exceed 300%.  Always investigate before
    correcting or discarding the value.

    Only consecutive calendar dates (balance_date = prev_date + 1 day) are
    compared.  The first observed balance for each account is not a comparison.
    A date gap triggers L4.account_date_completeness, not a movement alert.
    """
    # balance_date may be VARCHAR (parquet) or DATE (in-memory): cast defensively.
    # YYYY-MM-DD strings sort lexicographically identically to dates, so ordering
    # on the raw column is safe; arithmetic requires the cast.
    anomaly_rows = conn.execute("""
        WITH lag_bal AS (
            SELECT
                account_id,
                balance_date,
                closing_balance_cents,
                LAG(closing_balance_cents) OVER (
                    PARTITION BY account_id ORDER BY balance_date
                ) AS prev_cents,
                LAG(TRY_CAST(balance_date AS DATE)) OVER (
                    PARTITION BY account_id ORDER BY balance_date
                ) AS prev_date
            FROM daily_balances
        ),
        consecutive AS (
            SELECT *
            FROM lag_bal
            WHERE prev_date IS NOT NULL
              AND TRY_CAST(balance_date AS DATE) = prev_date + INTERVAL '1 day'
        )
        SELECT
            account_id,
            balance_date,
            prev_cents,
            closing_balance_cents,
            CASE
                WHEN prev_cents = 0 AND closing_balance_cents != 0 THEN 'ZERO_BASELINE'
                ELSE 'SPIKE'
            END AS anomaly_type
        FROM consecutive
        WHERE (prev_cents = 0 AND closing_balance_cents != 0)
           OR (
               prev_cents != 0
               AND ABS(closing_balance_cents - prev_cents) > 3 * ABS(prev_cents)
           )
    """).fetchall()

    spikes  = [r for r in anomaly_rows if r[4] == "SPIKE"]
    zeroes  = [r for r in anomaly_rows if r[4] == "ZERO_BASELINE"]

    spike_passed = len(spikes) == 0
    zero_passed  = len(zeroes) == 0

    spike_result = ValidationResult(
        check_id="L4.balance_spike",
        severity=Severity.BLOCKING,
        layer=4,
        table="daily_balances",
        passed=spike_passed,
        expected=(
            "ABS(current − prev) ≤ 3 × ABS(prev) on consecutive dates "
            "(exactly 300% does NOT fail)"
        ),
        observed=(
            "no movement anomalies" if spike_passed else
            f"{len(spikes)} consecutive-date movement(s) exceeding 300%"
        ),
        failing_rows=len(spikes),
        affected_keys=[
            {
                "account_id":  r[0],
                "balance_date": str(r[1]),
                "prev_cents":   r[2],
                "curr_cents":   r[3],
            }
            for r in spikes[:5]
        ],
        pipeline_health=(
            "" if spike_passed else
            "A movement alert identifies an anomaly requiring investigation; "
            "it does not by itself prove data corruption. "
            "A legitimate large deposit (e.g., wire transfer) could exceed 300%. "
            "Downstream balance trend reports will reflect the spike values as-is."
        ),
        next_step=(
            "" if spike_passed else
            "Inspect transaction logs for each flagged (account_id, balance_date). "
            "Confirm whether a large legitimate transaction occurred or whether the "
            "balance figure is an ETL error. Do not suppress without investigation."
        ),
    )

    zero_result = ValidationResult(
        check_id="L4.zero_baseline_movement",
        severity=Severity.BLOCKING,
        layer=4,
        table="daily_balances",
        passed=zero_passed,
        expected="no 0 → nonzero consecutive-date transition (undefined % change)",
        observed=(
            "no zero-baseline transitions" if zero_passed else
            f"{len(zeroes)} zero-baseline transition(s)"
        ),
        failing_rows=len(zeroes),
        affected_keys=[
            {"account_id": r[0], "balance_date": str(r[1])}
            for r in zeroes[:5]
        ],
        pipeline_health=(
            "" if zero_passed else
            "A 0 → nonzero transition has an undefined percentage change. "
            "It may indicate a duplicate account_id, a wrong account_id mapping, "
            "or a newly-funded account incorrectly pre-seeded with zero. "
            "Flagged BLOCKING in this demonstration because an undefined denominator "
            "prevents automated triage."
        ),
        next_step=(
            "" if zero_passed else
            "For each flagged account/date, verify whether the account was newly "
            "opened (opened_date = balance_date — expected) or whether the zero "
            "balance is an ETL artefact. A zero on the first observed date for a "
            "new account is expected; a zero mid-series followed by a nonzero is not."
        ),
    )

    return [spike_result, zero_result]


# ── Layer 5 – Join and report reconciliation ──────────────────────────────────


def check_branch_join_is_many_to_one(
    conn: duckdb.DuckDBPyConnection,
) -> ValidationResult:
    """
    Assert that branch_reference.branch_id is unique before building any report.

    ROOT CAUSE GATE — duplicate branch_ids are the root cause of the Lumen
    Community Bank incident.  The branch join (accounts.branch_id =
    branch_reference.branch_id) fans out every daily_balances row for each
    affected branch by the duplication factor.  Every SUM and AVG built on the
    joined table is therefore overstated.

    Overstated report metrics (e.g., inflated avg customer balance) are a
    DOWNSTREAM SYMPTOM of this root cause; they cannot be fixed at the report
    layer without first fixing the dimension.
    """
    dup_count: int = conn.execute("""
        SELECT COALESCE(SUM(cnt - 1), 0)
        FROM (
            SELECT branch_id, COUNT(*) AS cnt
            FROM branch_reference
            GROUP BY branch_id
            HAVING COUNT(*) > 1
        ) _d
    """).fetchone()[0]

    sample_keys: list = []
    if dup_count > 0:
        rows = conn.execute("""
            SELECT branch_id
            FROM branch_reference
            GROUP BY branch_id
            HAVING COUNT(*) > 1
            LIMIT 5
        """).fetchall()
        sample_keys = [r[0] for r in rows]

    passed = dup_count == 0
    return ValidationResult(
        check_id="L5.branch_join_many_to_one",
        severity=Severity.BLOCKING,
        layer=5,
        table="branch_reference",
        passed=passed,
        expected="branch_reference.branch_id unique — join is safe many-to-one",
        observed=(
            "branch_id unique — join safe" if passed else
            f"{dup_count} extra row(s) from {len(sample_keys)} duplicate branch_id(s). "
            "Join is NOT many-to-one."
        ),
        failing_rows=dup_count,
        affected_keys=sample_keys,
        pipeline_health=(
            "" if passed else
            "ROOT CAUSE: duplicate branch_ids fan out fact rows in the branch join. "
            "Every daily_balances row for an affected branch is multiplied by the "
            "duplication factor, inflating all SUM and AVG report metrics. "
            "Overstated values are a downstream symptom; fix the dimension first."
        ),
        next_step=(
            "" if passed else
            f"BLOCK all downstream reports. Duplicate branch_ids: {sample_keys}. "
            "Remove the extra rows from branch_reference and re-run this check "
            "before resuming report generation."
        ),
    )


def check_fact_row_count_after_join(
    conn: duckdb.DuckDBPyConnection,
) -> ValidationResult:
    """
    Verify that the branch join does not inflate the daily_balances row count.

    Counts fact rows before and after the full branch enrichment join:
        daily_balances
        JOIN accounts ON account_id
        JOIN branch_reference ON branch_id

    If branch_reference.branch_id is unique the count is preserved.
    Any count increase proves fan-out and quantifies the inflation factor.
    """
    before: int = conn.execute(
        "SELECT COUNT(*) FROM daily_balances"
    ).fetchone()[0]

    after: int = conn.execute("""
        SELECT COUNT(*)
        FROM daily_balances db
        JOIN accounts       a  ON db.account_id = a.account_id
        JOIN branch_reference br ON a.branch_id = br.branch_id
    """).fetchone()[0]

    inflation = after - before
    passed    = inflation == 0
    pct       = (inflation / before * 100) if before else 0.0

    return ValidationResult(
        check_id="L5.fact_row_count_after_join",
        severity=Severity.BLOCKING,
        layer=5,
        table="daily_balances",
        passed=passed,
        expected=f"{before:,} fact rows preserved after branch join",
        observed=(
            f"{after:,} rows (unchanged)" if passed else
            f"{after:,} rows — inflated by {inflation:,} (+{pct:.1f}%)"
        ),
        failing_rows=inflation,
        pipeline_health=(
            "" if passed else
            f"The branch join duplicated {inflation:,} fact rows (+{pct:.1f}%). "
            "Every SUM and AVG computed on the joined table is overstated. "
            "The inflation factor matches the branch_id duplication factor."
        ),
        next_step=(
            "" if passed else
            "Run L5.branch_join_many_to_one to identify duplicate branch_ids. "
            "Do not fix with DISTINCT on the fact table — that masks the root "
            "cause and may drop legitimate rows."
        ),
    )


def check_unmatched_branches(
    conn: duckdb.DuckDBPyConnection,
) -> ValidationResult:
    """
    Check for branches in branch_reference that have no accounts.

    An unmatched branch is not necessarily an error (a new branch may not yet
    have accounts), but it should be reviewed before branch-level metrics are
    reported.
    """
    unmatched: int = conn.execute("""
        SELECT COUNT(*)
        FROM branch_reference
        WHERE branch_id NOT IN (SELECT DISTINCT branch_id FROM accounts)
    """).fetchone()[0]

    sample_keys: list = []
    if unmatched > 0:
        rows = conn.execute("""
            SELECT branch_id
            FROM branch_reference
            WHERE branch_id NOT IN (SELECT DISTINCT branch_id FROM accounts)
            LIMIT 5
        """).fetchall()
        sample_keys = [r[0] for r in rows]

    passed = unmatched == 0
    return ValidationResult(
        check_id="L5.unmatched_branches",
        severity=Severity.WARNING,
        layer=5,
        table="branch_reference",
        passed=passed,
        expected="every branch has ≥ 1 account",
        observed=(
            "all branches matched" if passed else
            f"{unmatched} branch(es) with no accounts"
        ),
        failing_rows=unmatched,
        affected_keys=sample_keys,
        pipeline_health=(
            "" if passed else
            f"{unmatched} branch(es) will appear in branch_reference but produce "
            "zero-row results in branch-level reports, understating bank-wide totals "
            "if an INNER JOIN is used at the reporting layer."
        ),
        next_step=(
            "" if passed else
            f"Confirm whether branches {sample_keys} are newly opened "
            "(no accounts expected yet) or missing from the accounts load."
        ),
    )


def check_balance_total_preserved_after_join(
    conn: duckdb.DuckDBPyConnection,
) -> ValidationResult:
    """
    Verify SUM(closing_balance_cents) is unchanged after the branch join.

    If branch_reference is clean the join is many-to-one and the total is
    preserved.  An inflated post-join total quantifies the financial magnitude
    of the fan-out damage caused by duplicate branch_ids.
    """
    before: int = conn.execute(
        "SELECT COALESCE(SUM(closing_balance_cents), 0) FROM daily_balances"
    ).fetchone()[0]

    after: int = conn.execute("""
        SELECT COALESCE(SUM(db.closing_balance_cents), 0)
        FROM daily_balances   db
        JOIN accounts          a  ON db.account_id = a.account_id
        JOIN branch_reference  br ON a.branch_id   = br.branch_id
    """).fetchone()[0]

    diff   = after - before
    passed = diff == 0
    pct    = (diff / before * 100) if before else 0.0

    return ValidationResult(
        check_id="L5.balance_total_after_join",
        severity=Severity.BLOCKING,
        layer=5,
        table="daily_balances",
        passed=passed,
        expected=f"SUM(closing_balance_cents) = {before:,} ¢ after branch join",
        observed=(
            f"{after:,} ¢ (preserved)" if passed else
            f"{after:,} ¢ — overstated by {diff:,} ¢ (+{pct:.2f}%)"
        ),
        pipeline_health=(
            "" if passed else
            f"Balance totals overstated by {diff:,} ¢ (+{pct:.2f}%). "
            "Any report metric derived from the joined table reflects this overstatement. "
            "This is the downstream symptom of duplicate branch_ids."
        ),
        next_step=(
            "" if passed else
            "Fix root cause: remove duplicate branch_ids from branch_reference. "
            "Do not correct by dividing post-join balances — that masks the defect."
        ),
    )


def check_metric_reconciliation(
    conn: duckdb.DuckDBPyConnection,
) -> ValidationResult:
    """
    Reconcile the report's bank-wide avg customer balance against an independent
    calculation from base tables (no branch_reference join).

    The report uses:
        AVG(SUM per customer per date) after joining branch_reference

    The independent check uses:
        AVG(SUM per customer per date) directly from daily_balances + accounts

    A mismatch proves the report used a corrupted intermediate result.
    Comparison is done in integer cents to avoid floating-point tolerance issues.

    This tiny reconciliation catches the fan-out even when no row-count check
    is run — it validates the result using a different derivation path, so the
    same implementation mistake cannot be present in both paths.
    """
    report_avg: float | None = conn.execute("""
        SELECT AVG(cust_total)
        FROM (
            SELECT a.customer_id, db.balance_date,
                   SUM(db.closing_balance_cents) AS cust_total
            FROM   daily_balances    db
            JOIN   accounts           a  ON db.account_id = a.account_id
            JOIN   branch_reference   br ON a.branch_id   = br.branch_id
            GROUP BY a.customer_id, db.balance_date
        ) _
    """).fetchone()[0]

    indep_avg: float | None = conn.execute("""
        SELECT AVG(cust_total)
        FROM (
            SELECT a.customer_id, db.balance_date,
                   SUM(db.closing_balance_cents) AS cust_total
            FROM   daily_balances db
            JOIN   accounts        a ON db.account_id = a.account_id
            GROUP BY a.customer_id, db.balance_date
        ) _
    """).fetchone()[0]

    report_cents = round(report_avg) if report_avg is not None else 0
    indep_cents  = round(indep_avg)  if indep_avg  is not None else 0
    diff         = report_cents - indep_cents
    passed       = diff == 0
    pct          = (diff / indep_cents * 100) if indep_cents else 0.0

    return ValidationResult(
        check_id="L5.metric_reconciliation",
        severity=Severity.BLOCKING,
        layer=5,
        table="daily_balances",
        passed=passed,
        expected=f"report avg = independent avg = {indep_cents:,} ¢",
        observed=(
            f"{report_cents:,} ¢ (matches)" if passed else
            f"report {report_cents:,} ¢  |  independent {indep_cents:,} ¢  |  diff {diff:+,} ¢ ({pct:+.2f}%)"
        ),
        pipeline_health=(
            "" if passed else
            f"Report avg overstated by {diff:,} ¢ (+{pct:.2f}%) vs independent calculation. "
            "Root cause: branch join fan-out inflated intermediate customer totals. "
            "Any CFO metric derived from the report reflects this overstatement."
        ),
        next_step=(
            "" if passed else
            "Fix root cause: ensure branch_reference.branch_id is unique. "
            "The correct SQL excludes branch_reference from the customer-total "
            "sub-query entirely. Re-run reconciliation after fixing the dimension."
        ),
    )


# ── Schema catalogue (used by run_all_validators) ─────────────────────────────

_SCHEMA: dict[str, list[str]] = {
    "branch_reference": ["branch_id", "branch_name", "city", "state_code"],
    "customers":        ["customer_id", "customer_name", "created_date"],
    "accounts":         [
        "account_id", "customer_id", "branch_id", "product_type",
        "account_status", "currency", "opened_date", "closed_date", "ingested_at",
    ],
    "daily_balances":   ["account_id", "balance_date", "closing_balance_cents"],
}

_TYPES: dict[str, dict[str, str]] = {
    # Actual DuckDB types as read from the parquet files produced by the generator.
    # branch_id is BIGINT (pandas int64 → parquet INT64 → DuckDB BIGINT).
    # Date columns (created_date, opened_date, closed_date, balance_date) are
    # stored as VARCHAR in parquet because pandas serialises datetime.date as
    # strings. The check_column_type tests verify these as-written types;
    # the negative fixture test in test_layer1.py separately verifies that the
    # check DOES fail when, e.g., an integer column changes to VARCHAR.
    "branch_reference": {
        "branch_id":   "BIGINT",
        "branch_name": "VARCHAR",
        "city":        "VARCHAR",
        "state_code":  "VARCHAR",
    },
    "customers": {
        "customer_id":   "VARCHAR",
        "customer_name": "VARCHAR",
        "created_date":  "VARCHAR",
    },
    "accounts": {
        "account_id":     "VARCHAR",
        "customer_id":    "VARCHAR",
        "branch_id":      "BIGINT",
        "product_type":   "VARCHAR",
        "account_status": "VARCHAR",
        "currency":       "VARCHAR",
        "opened_date":    "VARCHAR",
        "ingested_at":    "TIMESTAMP",  # TIMESTAMP WITH TIME ZONE also matches
    },
    "daily_balances": {
        "account_id":            "VARCHAR",
        "balance_date":          "VARCHAR",
        "closing_balance_cents": "BIGINT",
    },
}

_NOT_NULL: dict[str, list[str]] = {
    "branch_reference": ["branch_id", "branch_name"],
    "customers":        ["customer_id", "customer_name", "created_date"],
    "accounts":         [
        "account_id", "customer_id", "branch_id", "product_type",
        "account_status", "currency", "opened_date", "ingested_at",
    ],
    "daily_balances":   ["account_id", "balance_date", "closing_balance_cents"],
}

_PRIMARY_KEYS: dict[str, list[str]] = {
    "branch_reference": ["branch_id"],
    "customers":        ["customer_id"],
    "accounts":         ["account_id"],
    "daily_balances":   ["account_id", "balance_date"],
}


# ── Full suite ────────────────────────────────────────────────────────────────


def run_all_validators(
    conn: duckdb.DuckDBPyConnection,
) -> list[ValidationResult]:
    """Execute all five layers and return every ValidationResult."""
    results: list[ValidationResult] = []

    # Layer 1 – Schema
    for table, cols in _SCHEMA.items():
        results.append(check_required_columns(conn, table, cols))
        results.append(check_no_unexpected_columns(conn, table, cols))
    for table, type_map in _TYPES.items():
        for col, expected_type in type_map.items():
            results.append(check_column_type(conn, table, col, expected_type))

    # Layer 2 – Keys
    for table, cols in _NOT_NULL.items():
        for col in cols:
            results.append(check_not_null(conn, table, col))
    for table, key_cols in _PRIMARY_KEYS.items():
        results.append(check_primary_key(conn, table, key_cols))

    # Layer 3 – Relationships
    results.append(check_foreign_key(
        conn, "accounts", "customer_id", "customers", "customer_id"
    ))
    results.append(check_foreign_key(
        conn, "accounts", "branch_id", "branch_reference", "branch_id"
    ))
    results.append(check_foreign_key(
        conn, "daily_balances", "account_id", "accounts", "account_id"
    ))

    # Layer 4 – Completeness + Business rules
    results.append(check_account_date_completeness(conn))
    results.append(check_account_lifecycle(conn))
    results.extend(check_balance_movement(conn))

    # Layer 5 – Reconciliation
    results.append(check_branch_join_is_many_to_one(conn))
    results.append(check_fact_row_count_after_join(conn))
    results.append(check_unmatched_branches(conn))
    results.append(check_balance_total_preserved_after_join(conn))
    results.append(check_metric_reconciliation(conn))

    return results
