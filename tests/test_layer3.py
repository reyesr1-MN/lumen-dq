"""
Layer 3 – Relationship tests.

Checks:
  accounts.customer_id  → customers.customer_id
  accounts.branch_id    → branch_reference.branch_id
  daily_balances.account_id → accounts.account_id

Negative-fixture tests confirm orphaned FK values are detected, with
sample keys populated in the result.
"""

from __future__ import annotations

import pytest
import duckdb

from lumen_dq.validators import Severity, check_foreign_key
from lumen_dq.generate import generate_scenario
from lumen_dq.db import open_connection


# ── Scenario fixtures ──────────────────────────────────────────────────────────


@pytest.fixture(scope="module")
def orphan_branch_conn(tmp_path_factory):
    """accounts contains branch_ids not in branch_reference."""
    d = generate_scenario("orphan_account_branch", tmp_path_factory.mktemp("s_ob"))
    return open_connection(d)


@pytest.fixture(scope="module")
def orphan_customer_conn(tmp_path_factory):
    """accounts contains customer_ids not in customers."""
    d = generate_scenario("orphan_account_customer", tmp_path_factory.mktemp("s_oc"))
    return open_connection(d)


@pytest.fixture(scope="module")
def orphan_balance_conn(tmp_path_factory):
    """daily_balances contains account_ids not in accounts."""
    d = generate_scenario("orphan_balance_account", tmp_path_factory.mktemp("s_oba"))
    return open_connection(d)


# ── Positive: all FK checks pass on baseline ──────────────────────────────────


class TestForeignKeyBaseline:
    def test_accounts_customer_id(self, baseline_conn):
        r = check_foreign_key(
            baseline_conn, "accounts", "customer_id", "customers", "customer_id"
        )
        assert r.passed, r.observed

    def test_accounts_branch_id(self, baseline_conn):
        r = check_foreign_key(
            baseline_conn, "accounts", "branch_id", "branch_reference", "branch_id"
        )
        assert r.passed, r.observed

    def test_daily_balances_account_id(self, baseline_conn):
        r = check_foreign_key(
            baseline_conn, "daily_balances", "account_id", "accounts", "account_id"
        )
        assert r.passed, r.observed


# ── Negative: orphan_account_branch ───────────────────────────────────────────


class TestOrphanAccountBranch:
    def test_check_fails(self, orphan_branch_conn):
        r = check_foreign_key(
            orphan_branch_conn, "accounts", "branch_id",
            "branch_reference", "branch_id",
        )
        assert not r.passed, "Expected FAIL — accounts reference non-existent branch_ids"

    def test_failing_rows_positive(self, orphan_branch_conn):
        r = check_foreign_key(
            orphan_branch_conn, "accounts", "branch_id",
            "branch_reference", "branch_id",
        )
        assert r.failing_rows > 0

    def test_affected_keys_non_empty(self, orphan_branch_conn):
        r = check_foreign_key(
            orphan_branch_conn, "accounts", "branch_id",
            "branch_reference", "branch_id",
        )
        assert not r.passed
        assert r.affected_keys

    def test_severity_is_blocking(self, orphan_branch_conn):
        r = check_foreign_key(
            orphan_branch_conn, "accounts", "branch_id",
            "branch_reference", "branch_id",
        )
        assert r.severity == Severity.BLOCKING

    def test_pipeline_health_mentions_inner_join(self, orphan_branch_conn):
        r = check_foreign_key(
            orphan_branch_conn, "accounts", "branch_id",
            "branch_reference", "branch_id",
        )
        assert not r.passed
        assert "inner join" in r.pipeline_health.lower() or "excludes" in r.pipeline_health.lower()


# ── Negative: orphan_account_customer ─────────────────────────────────────────


class TestOrphanAccountCustomer:
    def test_check_fails(self, orphan_customer_conn):
        r = check_foreign_key(
            orphan_customer_conn, "accounts", "customer_id",
            "customers", "customer_id",
        )
        assert not r.passed

    def test_failing_rows_positive(self, orphan_customer_conn):
        r = check_foreign_key(
            orphan_customer_conn, "accounts", "customer_id",
            "customers", "customer_id",
        )
        assert r.failing_rows > 0

    def test_affected_keys_are_orphaned_ids(self, orphan_customer_conn):
        r = check_foreign_key(
            orphan_customer_conn, "accounts", "customer_id",
            "customers", "customer_id",
        )
        assert not r.passed
        assert r.affected_keys
        # These are the fresh ACCT-ORPH-* customer ids — they won't be in customers
        for key in r.affected_keys:
            assert key is not None


# ── Negative: orphan_balance_account ──────────────────────────────────────────


class TestOrphanBalanceAccount:
    def test_check_fails(self, orphan_balance_conn):
        r = check_foreign_key(
            orphan_balance_conn, "daily_balances", "account_id",
            "accounts", "account_id",
        )
        assert not r.passed

    def test_failing_rows_positive(self, orphan_balance_conn):
        r = check_foreign_key(
            orphan_balance_conn, "daily_balances", "account_id",
            "accounts", "account_id",
        )
        assert r.failing_rows > 0

    def test_severity_blocking(self, orphan_balance_conn):
        r = check_foreign_key(
            orphan_balance_conn, "daily_balances", "account_id",
            "accounts", "account_id",
        )
        assert r.severity == Severity.BLOCKING


# ── In-memory isolation tests ─────────────────────────────────────────────────


class TestIsolatedFKFixtures:
    def _make_conn(self) -> duckdb.DuckDBPyConnection:
        conn = duckdb.connect()
        conn.execute("CREATE TABLE ref (id INTEGER)")
        conn.execute("INSERT INTO ref VALUES (1),(2),(3)")
        conn.execute("CREATE TABLE fact (id INTEGER, ref_id INTEGER)")
        conn.execute("INSERT INTO fact VALUES (10,1),(11,2),(12,4)")  # 4 is orphan
        return conn

    def test_detects_orphan(self):
        conn = self._make_conn()
        r = check_foreign_key(conn, "fact", "ref_id", "ref", "id")
        assert not r.passed
        assert r.failing_rows == 1
        assert 4 in r.affected_keys

    def test_passes_when_all_matched(self):
        conn = duckdb.connect()
        conn.execute("CREATE TABLE ref (id INTEGER)")
        conn.execute("INSERT INTO ref VALUES (1),(2)")
        conn.execute("CREATE TABLE fact (id INTEGER, ref_id INTEGER)")
        conn.execute("INSERT INTO fact VALUES (10,1),(11,2)")
        r = check_foreign_key(conn, "fact", "ref_id", "ref", "id")
        assert r.passed

    def test_null_fk_not_treated_as_orphan(self):
        """NULL FK values should not be counted as orphans."""
        conn = duckdb.connect()
        conn.execute("CREATE TABLE ref (id INTEGER)")
        conn.execute("INSERT INTO ref VALUES (1)")
        conn.execute("CREATE TABLE fact (id INTEGER, ref_id INTEGER)")
        conn.execute("INSERT INTO fact VALUES (10,1),(11,NULL)")
        r = check_foreign_key(conn, "fact", "ref_id", "ref", "id")
        assert r.passed

    def test_check_id_format(self):
        conn = duckdb.connect()
        conn.execute("CREATE TABLE customers (customer_id VARCHAR)")
        conn.execute("CREATE TABLE accounts (customer_id VARCHAR)")
        r = check_foreign_key(
            conn, "accounts", "customer_id", "customers", "customer_id"
        )
        assert "L3.foreign_key" in r.check_id
        assert "accounts" in r.check_id
        assert "customer_id" in r.check_id

    def test_layer_is_3(self):
        conn = duckdb.connect()
        conn.execute("CREATE TABLE r (id INTEGER)")
        conn.execute("CREATE TABLE f (ref_id INTEGER)")
        r = check_foreign_key(conn, "f", "ref_id", "r", "id")
        assert r.layer == 3
