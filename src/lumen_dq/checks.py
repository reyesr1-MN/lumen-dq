"""
Layered data-quality checks for Lumen Community Bank.

Layer 1 – Schema      : non-null constraints on PK / FK columns
Layer 2 – Grain       : primary-key uniqueness on all four tables
Layer 3 – Referential : FK integrity (accounts→customers, accounts→branch_reference,
                        daily_balances→accounts)
Layer 4 – Business    : exact branch count (42), row-count bounds, balance ≥ 0,
                        allowed account_type values
Layer 5 – Metric      : no NULLs in the computed report columns

Each check function returns a CheckResult dataclass.
``run_all_checks`` executes the full baseline suite and returns a list.
``print_results`` renders a table and returns the failure count.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

import duckdb

# ── Result dataclass ───────────────────────────────────────────────────────────


@dataclass
class CheckResult:
    name: str
    layer: str  # schema | grain | referential | business | metric
    passed: bool
    message: str
    failing_rows: int = 0


# ── Layer 1 — Schema ───────────────────────────────────────────────────────────


def check_not_null(
    conn: duckdb.DuckDBPyConnection,
    table: str,
    column: str,
) -> CheckResult:
    """Fail if any row has NULL in *column*."""
    null_count: int = conn.execute(
        f"SELECT COUNT(*) FROM {table} WHERE {column} IS NULL"
    ).fetchone()[0]
    passed = null_count == 0
    return CheckResult(
        name=f"not_null:{table}.{column}",
        layer="schema",
        passed=passed,
        message="OK" if passed else f"{null_count} NULL value(s)",
        failing_rows=null_count,
    )


# ── Layer 2 — Grain ────────────────────────────────────────────────────────────


def check_grain_uniqueness(
    conn: duckdb.DuckDBPyConnection,
    table: str,
    key_columns: list[str],
) -> CheckResult:
    """Fail if any combination of *key_columns* appears more than once."""
    if len(key_columns) == 1:
        col = key_columns[0]
        dup_count: int = conn.execute(
            f"SELECT COUNT(*) - COUNT(DISTINCT {col}) FROM {table}"
        ).fetchone()[0]
    else:
        cols = ", ".join(key_columns)
        dup_count = conn.execute(f"""
            SELECT COALESCE(SUM(cnt - 1), 0)
            FROM (
                SELECT {cols}, COUNT(*) AS cnt
                FROM {table}
                GROUP BY {cols}
                HAVING COUNT(*) > 1
            ) _dupes
        """).fetchone()[0]

    passed = dup_count == 0
    key_label = "+".join(key_columns)
    return CheckResult(
        name=f"grain_uniqueness:{table}[{key_label}]",
        layer="grain",
        passed=passed,
        message="OK" if passed else f"{dup_count} duplicate key combination(s)",
        failing_rows=dup_count,
    )


# ── Layer 3 — Referential ──────────────────────────────────────────────────────


def check_foreign_key(
    conn: duckdb.DuckDBPyConnection,
    table: str,
    column: str,
    ref_table: str,
    ref_column: str,
) -> CheckResult:
    """Fail if any non-NULL FK value in *table.column* is absent from *ref_table.ref_column*."""
    orphan_count: int = conn.execute(f"""
        SELECT COUNT(*)
        FROM {table} t
        WHERE t.{column} IS NOT NULL
          AND t.{column} NOT IN (SELECT {ref_column} FROM {ref_table})
    """).fetchone()[0]
    passed = orphan_count == 0
    return CheckResult(
        name=f"fk:{table}.{column}→{ref_table}.{ref_column}",
        layer="referential",
        passed=passed,
        message="OK" if passed else f"{orphan_count} orphaned FK value(s)",
        failing_rows=orphan_count,
    )


# ── Layer 4 — Business rules ───────────────────────────────────────────────────


def check_branch_count(
    conn: duckdb.DuckDBPyConnection,
    table: str = "branch_reference",
) -> CheckResult:
    """
    Enforce that *table* has exactly 42 rows and 42 unique branch_ids.

    This is the primary gate that would have blocked the incident:
    the buggy branch_reference had 47 rows (5 duplicated branch_ids).
    """
    row = conn.execute(
        f"SELECT COUNT(*) AS total, COUNT(DISTINCT branch_id) AS uniq FROM {table}"
    ).fetchone()
    total, uniq = row
    passed = (total == 42) and (uniq == 42)
    return CheckResult(
        name=f"branch_count:{table}",
        layer="business",
        passed=passed,
        message=(
            "OK (42 rows, 42 unique branch_ids)"
            if passed
            else f"Expected 42 rows / 42 unique branch_ids; got {total} rows / {uniq} unique"
        ),
        failing_rows=max(total - uniq, abs(uniq - 42)),
    )


def check_row_count(
    conn: duckdb.DuckDBPyConnection,
    table: str,
    expected: int,
    tolerance: int = 0,
) -> CheckResult:
    """Fail if the row count deviates from *expected* by more than *tolerance*."""
    actual: int = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    passed = abs(actual - expected) <= tolerance
    return CheckResult(
        name=f"row_count:{table}",
        layer="business",
        passed=passed,
        message=(
            f"OK ({actual:,} rows)"
            if passed
            else f"Expected {expected:,}±{tolerance}, got {actual:,}"
        ),
        failing_rows=0,
    )


def check_non_negative(
    conn: duckdb.DuckDBPyConnection,
    table: str,
    column: str,
) -> CheckResult:
    """Fail if any value in *column* is strictly negative."""
    bad_count: int = conn.execute(
        f"SELECT COUNT(*) FROM {table} WHERE {column} < 0"
    ).fetchone()[0]
    passed = bad_count == 0
    return CheckResult(
        name=f"non_negative:{table}.{column}",
        layer="business",
        passed=passed,
        message="OK" if passed else f"{bad_count} negative value(s)",
        failing_rows=bad_count,
    )


def check_allowed_values(
    conn: duckdb.DuckDBPyConnection,
    table: str,
    column: str,
    allowed: list[str],
) -> CheckResult:
    """Fail if any non-NULL value in *column* is not in *allowed*."""
    quoted = ", ".join(f"'{v}'" for v in allowed)
    bad_count: int = conn.execute(
        f"SELECT COUNT(*) FROM {table} WHERE {column} NOT IN ({quoted})"
    ).fetchone()[0]
    passed = bad_count == 0
    return CheckResult(
        name=f"allowed_values:{table}.{column}",
        layer="business",
        passed=passed,
        message="OK" if passed else f"{bad_count} disallowed value(s)",
        failing_rows=bad_count,
    )


# ── Layer 5 — Metric sanity ────────────────────────────────────────────────────


def check_metric_no_nulls(
    conn: duckdb.DuckDBPyConnection,
    metric_sql: str,
    metric_column: str,
) -> CheckResult:
    """Fail if the computed metric column contains any NULLs."""
    null_count: int = conn.execute(f"""
        SELECT COUNT(*) FROM ({metric_sql}) _m
        WHERE {metric_column} IS NULL
    """).fetchone()[0]
    passed = null_count == 0
    return CheckResult(
        name=f"metric_no_nulls:{metric_column}",
        layer="metric",
        passed=passed,
        message="OK" if passed else f"{null_count} NULL metric value(s)",
        failing_rows=null_count,
    )


# ── Baseline check suite ───────────────────────────────────────────────────────

# Each entry: (check_fn, positional_args_tuple)
_BASELINE_CHECKS: list[tuple] = [
    # Layer 2 – Grain
    (check_grain_uniqueness, ("branch_reference", ["branch_id"])),
    (check_grain_uniqueness, ("customers", ["customer_id"])),
    (check_grain_uniqueness, ("accounts", ["account_id"])),
    (check_grain_uniqueness, ("daily_balances", ["account_id", "balance_date"])),
    # Layer 3 – Referential
    (check_foreign_key, ("accounts", "customer_id", "customers", "customer_id")),
    (check_foreign_key, ("accounts", "branch_id", "branch_reference", "branch_id")),
    (check_foreign_key, ("daily_balances", "account_id", "accounts", "account_id")),
    # Layer 4 – Business
    (check_branch_count, ("branch_reference",)),
    (check_row_count, ("customers", 1_000)),
    (check_row_count, ("accounts", 1_500)),
    (check_row_count, ("daily_balances", 45_000)),
    (check_non_negative, ("daily_balances", "closing_balance_cents")),
    (check_allowed_values, ("accounts", "product_type",
                            ["CHECKING", "SAVINGS", "CD", "MONEY_MARKET"])),
    (check_allowed_values, ("accounts", "account_status",
                            ["ACTIVE", "FROZEN", "CLOSED"])),
    (check_allowed_values, ("accounts", "currency", ["USD"])),
    # Layer 1 – Schema (not-null)
    (check_not_null, ("customers", "customer_id")),
    (check_not_null, ("accounts", "account_id")),
    (check_not_null, ("accounts", "customer_id")),
    (check_not_null, ("accounts", "branch_id")),
    (check_not_null, ("accounts", "product_type")),
    (check_not_null, ("accounts", "account_status")),
    (check_not_null, ("accounts", "currency")),
    (check_not_null, ("accounts", "opened_date")),
    (check_not_null, ("daily_balances", "account_id")),
    (check_not_null, ("daily_balances", "balance_date")),
    (check_not_null, ("daily_balances", "closing_balance_cents")),
]


def run_all_checks(conn: duckdb.DuckDBPyConnection) -> list[CheckResult]:
    """Execute every check in the baseline suite and return the results."""
    return [fn(conn, *args) for fn, args in _BASELINE_CHECKS]


def print_results(results: list[CheckResult]) -> int:
    """
    Print a formatted results table.

    Returns the number of failed checks (0 = all passed).
    """
    failures = 0
    print(f"\n{'CHECK':<58} {'LAYER':<13} STATUS")
    print("─" * 90)
    for r in results:
        status = "PASS" if r.passed else f"FAIL  ({r.message})"
        print(f"{r.name:<58} {r.layer:<13} {status}")
        if not r.passed:
            failures += 1
    print("─" * 90)
    total = len(results)
    print(f"Result: {total - failures}/{total} checks passed"
          + (f", {failures} FAILURE(S)" if failures else " — all green"))
    return failures
