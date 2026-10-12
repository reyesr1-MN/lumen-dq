"""
Layer 2 – Keys and nullability tests.

Checks: primary-key uniqueness, composite-key uniqueness, not-null constraints.

Negative-fixture tests use generated defect scenarios to confirm that every
violation is detected, with populated affected_keys samples.
"""

from __future__ import annotations

import pytest
import duckdb

from lumen_dq.validators import Severity, check_not_null, check_primary_key
from lumen_dq.generate import generate_scenario
from lumen_dq.db import open_connection


# ── Fixtures ───────────────────────────────────────────────────────────────────


@pytest.fixture(scope="module")
def dup_branch_conn(tmp_path_factory):
    """branch_reference with 5 duplicate branch_ids."""
    d = generate_scenario(
        "dup_branch_id_conflict_name", tmp_path_factory.mktemp("s_db")
    )
    return open_connection(d)


@pytest.fixture(scope="module")
def dup_account_conn(tmp_path_factory):
    """accounts with a duplicate account_id."""
    d = generate_scenario("dup_account_id", tmp_path_factory.mktemp("s_da"))
    return open_connection(d)


@pytest.fixture(scope="module")
def dup_balance_conn(tmp_path_factory):
    """daily_balances with duplicate (account_id, balance_date) pairs."""
    d = generate_scenario("dup_balance_grain", tmp_path_factory.mktemp("s_dg"))
    return open_connection(d)


@pytest.fixture(scope="module")
def null_field_conn(tmp_path_factory):
    """accounts with NULLs in branch_id (null_required_field scenario)."""
    d = generate_scenario("null_required_field", tmp_path_factory.mktemp("s_nf"))
    return open_connection(d)


# ── Positive: baseline passes all Layer 2 checks ──────────────────────────────


class TestPrimaryKeyBaseline:
    @pytest.mark.parametrize("table,keys", [
        ("branch_reference", ["branch_id"]),
        ("customers",        ["customer_id"]),
        ("accounts",         ["account_id"]),
        ("daily_balances",   ["account_id", "balance_date"]),
    ])
    def test_pk_passes(self, baseline_conn, table, keys):
        r = check_primary_key(baseline_conn, table, keys)
        assert r.passed, r.observed


class TestNotNullBaseline:
    @pytest.mark.parametrize("table,column", [
        ("branch_reference", "branch_id"),
        ("branch_reference", "branch_name"),
        ("customers",        "customer_id"),
        ("customers",        "customer_name"),
        ("customers",        "created_date"),
        ("accounts",         "account_id"),
        ("accounts",         "customer_id"),
        ("accounts",         "branch_id"),
        ("accounts",         "product_type"),
        ("accounts",         "account_status"),
        ("accounts",         "currency"),
        ("accounts",         "opened_date"),
        ("accounts",         "ingested_at"),
        ("daily_balances",   "account_id"),
        ("daily_balances",   "balance_date"),
        ("daily_balances",   "closing_balance_cents"),
    ])
    def test_not_null_passes(self, baseline_conn, table, column):
        r = check_not_null(baseline_conn, table, column)
        assert r.passed, r.observed


# ── Negative: duplicate primary keys ──────────────────────────────────────────


class TestDuplicateBranchId:
    def test_check_fails(self, dup_branch_conn):
        r = check_primary_key(dup_branch_conn, "branch_reference", ["branch_id"])
        assert not r.passed, "Expected FAIL — duplicate branch_id present"

    def test_failing_rows_is_1(self, dup_branch_conn):
        """dup_branch_id_conflict_name scenario adds one extra duplicate branch_id."""
        r = check_primary_key(dup_branch_conn, "branch_reference", ["branch_id"])
        assert r.failing_rows == 1

    def test_affected_keys_non_empty(self, dup_branch_conn):
        r = check_primary_key(dup_branch_conn, "branch_reference", ["branch_id"])
        assert r.affected_keys, "affected_keys should list sample duplicate branch_ids"

    def test_severity_blocking(self, dup_branch_conn):
        r = check_primary_key(dup_branch_conn, "branch_reference", ["branch_id"])
        assert r.severity == Severity.BLOCKING

    def test_pipeline_health_mentions_fan_out(self, dup_branch_conn):
        r = check_primary_key(dup_branch_conn, "branch_reference", ["branch_id"])
        assert not r.passed
        assert "fan out" in r.pipeline_health.lower() or "inflate" in r.pipeline_health.lower()


class TestDuplicateAccountId:
    def test_check_fails(self, dup_account_conn):
        r = check_primary_key(dup_account_conn, "accounts", ["account_id"])
        assert not r.passed

    def test_failing_rows_positive(self, dup_account_conn):
        r = check_primary_key(dup_account_conn, "accounts", ["account_id"])
        assert r.failing_rows > 0

    def test_affected_keys_are_account_ids(self, dup_account_conn):
        r = check_primary_key(dup_account_conn, "accounts", ["account_id"])
        assert not r.passed
        assert r.affected_keys


class TestDuplicateBalanceGrain:
    def test_composite_key_fails(self, dup_balance_conn):
        r = check_primary_key(
            dup_balance_conn, "daily_balances", ["account_id", "balance_date"]
        )
        assert not r.passed

    def test_failing_rows_positive(self, dup_balance_conn):
        r = check_primary_key(
            dup_balance_conn, "daily_balances", ["account_id", "balance_date"]
        )
        assert r.failing_rows > 0

    def test_check_id_contains_composite_key(self, dup_balance_conn):
        r = check_primary_key(
            dup_balance_conn, "daily_balances", ["account_id", "balance_date"]
        )
        assert "account_id+balance_date" in r.check_id


# ── Negative: null required field ─────────────────────────────────────────────


class TestNullRequiredField:
    def test_null_branch_id_fails(self, null_field_conn):
        r = check_not_null(null_field_conn, "accounts", "branch_id")
        assert not r.passed, "Expected FAIL — branch_id has NULLs"

    def test_failing_rows_positive(self, null_field_conn):
        r = check_not_null(null_field_conn, "accounts", "branch_id")
        assert r.failing_rows > 0

    def test_severity_blocking(self, null_field_conn):
        r = check_not_null(null_field_conn, "accounts", "branch_id")
        assert r.severity == Severity.BLOCKING

    def test_pipeline_health_populated(self, null_field_conn):
        r = check_not_null(null_field_conn, "accounts", "branch_id")
        assert not r.passed
        assert len(r.pipeline_health) > 10

    def test_next_step_populated(self, null_field_conn):
        r = check_not_null(null_field_conn, "accounts", "branch_id")
        assert not r.passed
        assert "branch_id" in r.next_step


# ── In-memory isolation tests ─────────────────────────────────────────────────


class TestIsolatedFixtures:
    def test_pk_single_column_duplicate(self):
        conn = duckdb.connect()
        conn.execute("CREATE TABLE t (id INTEGER, val VARCHAR)")
        conn.execute("INSERT INTO t VALUES (1,'a'),(1,'b'),(2,'c')")
        r = check_primary_key(conn, "t", ["id"])
        assert not r.passed
        assert r.failing_rows == 1

    def test_pk_composite_duplicate(self):
        conn = duckdb.connect()
        conn.execute("CREATE TABLE t (a INTEGER, b INTEGER, val INTEGER)")
        conn.execute("INSERT INTO t VALUES (1,1,10),(1,1,20),(1,2,30)")
        r = check_primary_key(conn, "t", ["a", "b"])
        assert not r.passed
        assert r.failing_rows == 1

    def test_pk_clean_passes(self):
        conn = duckdb.connect()
        conn.execute("CREATE TABLE t (id INTEGER)")
        conn.execute("INSERT INTO t VALUES (1),(2),(3)")
        r = check_primary_key(conn, "t", ["id"])
        assert r.passed

    def test_not_null_detects_null(self):
        conn = duckdb.connect()
        conn.execute("CREATE TABLE t (x INTEGER)")
        conn.execute("INSERT INTO t VALUES (1),(NULL),(3)")
        r = check_not_null(conn, "t", "x")
        assert not r.passed
        assert r.failing_rows == 1

    def test_not_null_passes_on_zeros(self):
        """0 and empty string are not NULL."""
        conn = duckdb.connect()
        conn.execute("CREATE TABLE t (x INTEGER, s VARCHAR)")
        conn.execute("INSERT INTO t VALUES (0,''),(1,'a')")
        r_x = check_not_null(conn, "t", "x")
        r_s = check_not_null(conn, "t", "s")
        assert r_x.passed
        assert r_s.passed

    def test_result_check_id_format(self):
        conn = duckdb.connect()
        conn.execute("CREATE TABLE br (branch_id INTEGER)")
        r = check_primary_key(conn, "br", ["branch_id"])
        assert r.check_id == "L2.primary_key.br[branch_id]"

    def test_result_layer_is_2(self):
        conn = duckdb.connect()
        conn.execute("CREATE TABLE t (id INTEGER)")
        r = check_primary_key(conn, "t", ["id"])
        assert r.layer == 2

    def test_to_dict_serialisable(self):
        conn = duckdb.connect()
        conn.execute("CREATE TABLE t (id INTEGER)")
        conn.execute("INSERT INTO t VALUES (1),(1)")
        r = check_primary_key(conn, "t", ["id"])
        import json
        d = r.to_dict()
        _ = json.dumps(d)  # must not raise
