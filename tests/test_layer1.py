"""
Layer 1 – Schema and strict type tests.

Validates raw serialized data before permissive casting can hide defects.
Checks: required columns, unexpected columns, column types.

Negative-fixture tests assert that the check FAILS (detected) when the
expected defect is present.
"""

from __future__ import annotations

import pytest
import duckdb

from lumen_dq.validators import (
    Severity,
    check_column_type,
    check_no_unexpected_columns,
    check_required_columns,
)
from lumen_dq.generate import generate_scenario
from lumen_dq.db import open_connection


# ── Helpers ────────────────────────────────────────────────────────────────────


def _mem() -> duckdb.DuckDBPyConnection:
    """In-memory connection for isolated fixtures."""
    return duckdb.connect()


# ── Positive: baseline passes all Layer 1 checks ──────────────────────────────


class TestRequiredColumnsBaseline:
    def test_branch_reference(self, baseline_conn):
        r = check_required_columns(
            baseline_conn, "branch_reference",
            ["branch_id", "branch_name", "city", "state_code"],
        )
        assert r.passed, r.observed

    def test_customers(self, baseline_conn):
        r = check_required_columns(
            baseline_conn, "customers",
            ["customer_id", "customer_name", "created_date"],
        )
        assert r.passed, r.observed

    def test_accounts(self, baseline_conn):
        r = check_required_columns(
            baseline_conn, "accounts",
            [
                "account_id", "customer_id", "branch_id", "product_type",
                "account_status", "currency", "opened_date", "closed_date",
                "ingested_at",
            ],
        )
        assert r.passed, r.observed

    def test_daily_balances(self, baseline_conn):
        r = check_required_columns(
            baseline_conn, "daily_balances",
            ["account_id", "balance_date", "closing_balance_cents"],
        )
        assert r.passed, r.observed


class TestNoUnexpectedColumnsBaseline:
    def test_branch_reference(self, baseline_conn):
        r = check_no_unexpected_columns(
            baseline_conn, "branch_reference",
            ["branch_id", "branch_name", "city", "state_code"],
        )
        assert r.passed, r.observed

    def test_accounts(self, baseline_conn):
        r = check_no_unexpected_columns(
            baseline_conn, "accounts",
            [
                "account_id", "customer_id", "branch_id", "product_type",
                "account_status", "currency", "opened_date", "closed_date",
                "ingested_at",
            ],
        )
        assert r.passed, r.observed

    def test_daily_balances(self, baseline_conn):
        r = check_no_unexpected_columns(
            baseline_conn, "daily_balances",
            ["account_id", "balance_date", "closing_balance_cents"],
        )
        assert r.passed, r.observed


class TestColumnTypesBaseline:
    """
    Verify the as-written column types in parquet match the type catalogue.

    branch_id is BIGINT (pandas int64 → parquet INT64 → DuckDB BIGINT).
    Date-typed columns (created_date, opened_date, balance_date) are stored as
    VARCHAR because pandas serialises datetime.date objects as strings in parquet.
    The negative tests below separately verify that type mismatches ARE detected.
    """

    @pytest.mark.parametrize("column,expected", [
        ("branch_id",   "BIGINT"),
        ("branch_name", "VARCHAR"),
        ("city",        "VARCHAR"),
        ("state_code",  "VARCHAR"),
    ])
    def test_branch_reference_types(self, baseline_conn, column, expected):
        r = check_column_type(baseline_conn, "branch_reference", column, expected)
        assert r.passed, r.observed

    @pytest.mark.parametrize("column,expected", [
        ("customer_id",   "VARCHAR"),
        ("created_date",  "VARCHAR"),   # date stored as string in parquet
    ])
    def test_customers_types(self, baseline_conn, column, expected):
        r = check_column_type(baseline_conn, "customers", column, expected)
        assert r.passed, r.observed

    @pytest.mark.parametrize("column,expected", [
        ("account_id",     "VARCHAR"),
        ("branch_id",      "BIGINT"),   # int64 in parquet
        ("opened_date",    "VARCHAR"),  # date stored as string in parquet
        ("ingested_at",    "TIMESTAMP"),
    ])
    def test_accounts_types(self, baseline_conn, column, expected):
        r = check_column_type(baseline_conn, "accounts", column, expected)
        assert r.passed, r.observed

    @pytest.mark.parametrize("column,expected", [
        ("account_id",             "VARCHAR"),
        ("balance_date",           "VARCHAR"),  # date stored as string in parquet
        ("closing_balance_cents",  "BIGINT"),
    ])
    def test_daily_balances_types(self, baseline_conn, column, expected):
        r = check_column_type(baseline_conn, "daily_balances", column, expected)
        assert r.passed, r.observed


# ── Negative: missing_accounts_column scenario ────────────────────────────────


@pytest.fixture(scope="module")
def missing_col_conn(tmp_path_factory):
    """accounts.parquet with the 'currency' column dropped."""
    d = generate_scenario("missing_accounts_column", tmp_path_factory.mktemp("s_mc"))
    return open_connection(d)


class TestMissingColumnDetected:
    def test_missing_currency_column_fails(self, missing_col_conn):
        """
        The missing_accounts_column scenario drops 'currency'.
        check_required_columns must detect it.
        """
        r = check_required_columns(
            missing_col_conn, "accounts",
            [
                "account_id", "customer_id", "branch_id", "product_type",
                "account_status", "currency", "opened_date", "closed_date",
                "ingested_at",
            ],
        )
        assert not r.passed, "Expected FAIL — currency column is missing"
        assert "currency" in r.affected_keys

    def test_affected_keys_populated(self, missing_col_conn):
        r = check_required_columns(missing_col_conn, "accounts", ["currency"])
        assert not r.passed
        assert r.affected_keys  # must be non-empty

    def test_severity_is_blocking(self, missing_col_conn):
        r = check_required_columns(missing_col_conn, "accounts", ["currency"])
        assert r.severity == Severity.BLOCKING

    def test_pipeline_health_populated(self, missing_col_conn):
        r = check_required_columns(missing_col_conn, "accounts", ["currency"])
        assert not r.passed
        assert len(r.pipeline_health) > 10

    def test_next_step_populated(self, missing_col_conn):
        r = check_required_columns(missing_col_conn, "accounts", ["currency"])
        assert not r.passed
        assert len(r.next_step) > 10


# ── Negative: unexpected column ───────────────────────────────────────────────


class TestUnexpectedColumnDetected:
    def test_extra_column_is_flagged(self):
        """Add an undocumented column and confirm it is detected."""
        conn = _mem()
        conn.execute(
            "CREATE TABLE t (id INTEGER, name VARCHAR, secret_field VARCHAR)"
        )
        r = check_no_unexpected_columns(conn, "t", ["id", "name"])
        assert not r.passed
        assert "secret_field" in r.affected_keys

    def test_severity_is_warning(self):
        """Unexpected columns are WARNING, not BLOCKING (schema drift, not corruption)."""
        conn = _mem()
        conn.execute("CREATE TABLE t (id INTEGER, extra DOUBLE)")
        r = check_no_unexpected_columns(conn, "t", ["id"])
        assert r.severity == Severity.WARNING


# ── Negative: wrong column type ───────────────────────────────────────────────


class TestColumnTypeDetected:
    def test_varchar_where_integer_expected(self):
        """branch_id stored as VARCHAR — type check must fail."""
        conn = _mem()
        conn.execute("CREATE TABLE br (branch_id VARCHAR, branch_name VARCHAR)")
        r = check_column_type(conn, "br", "branch_id", "INTEGER")
        assert not r.passed
        assert "INTEGER" in r.expected
        assert "VARCHAR" in r.observed

    def test_varchar_where_date_expected(self):
        """balance_date stored as VARCHAR — malformed dates hide until implicit cast."""
        conn = _mem()
        conn.execute(
            "CREATE TABLE db (account_id VARCHAR, balance_date VARCHAR, "
            "closing_balance_cents BIGINT)"
        )
        r = check_column_type(conn, "db", "balance_date", "DATE")
        assert not r.passed

    def test_severity_is_blocking(self):
        conn = _mem()
        conn.execute("CREATE TABLE t (val VARCHAR)")
        r = check_column_type(conn, "t", "val", "INTEGER")
        assert r.severity == Severity.BLOCKING

    def test_check_id_is_stable(self, baseline_conn):
        r = check_column_type(baseline_conn, "accounts", "branch_id", "INTEGER")
        assert r.check_id == "L1.column_type.accounts.branch_id"

    def test_layer_is_1(self, baseline_conn):
        r = check_column_type(baseline_conn, "accounts", "branch_id", "INTEGER")
        assert r.layer == 1

    def test_to_dict_has_all_keys(self, baseline_conn):
        r = check_required_columns(
            baseline_conn, "accounts", ["account_id"]
        )
        d = r.to_dict()
        for key in (
            "check_id", "severity", "layer", "table", "passed",
            "expected", "observed", "failing_rows", "affected_keys",
            "pipeline_health", "next_step",
        ):
            assert key in d, f"Missing key: {key}"
