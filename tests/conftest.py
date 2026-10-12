"""
Shared pytest fixtures for the Lumen Community Bank DQ test suite.

Session-scoped fixtures generate data once per test run to keep the suite fast.

Fixtures
--------
baseline_dir      Path to a temp dir containing the four valid parquet files.
baseline_conn     DuckDB connection over baseline_dir.
buggy_dir         Path to a temp dir with a branch_reference containing 5 duplicate branch_ids.
buggy_conn        DuckDB connection over buggy_dir.
dup_balances_dir  Path to a temp dir with duplicate (account_id, balance_date) pairs.
orphan_fk_dir     Path to a temp dir with accounts referencing a non-existent customer_id.
"""

import shutil

import pytest

from lumen_dq.db import open_connection
from lumen_dq.generate import (
    generate_all,
    generate_buggy_branch_reference,
    generate_invalid_accounts_orphan_fk,
    generate_invalid_daily_balances,
)


@pytest.fixture(scope="session")
def baseline_dir(tmp_path_factory):
    """Generate the four baseline parquet files into a temp directory."""
    d = tmp_path_factory.mktemp("baseline")
    generate_all(d)
    return d


@pytest.fixture(scope="session")
def baseline_conn(baseline_dir):
    """DuckDB connection with views over the valid baseline tables."""
    return open_connection(baseline_dir)


@pytest.fixture(scope="session")
def buggy_dir(baseline_dir, tmp_path_factory):
    """
    Branch reference with 5 duplicate branch_ids (the incident fixture).
    All other tables are identical to baseline.
    """
    d = tmp_path_factory.mktemp("buggy")
    for f in baseline_dir.glob("*.parquet"):
        shutil.copy(f, d)
    generate_buggy_branch_reference(baseline_dir).to_parquet(
        d / "branch_reference.parquet", index=False
    )
    return d


@pytest.fixture(scope="session")
def buggy_conn(buggy_dir):
    """DuckDB connection over the buggy branch_reference fixture."""
    return open_connection(buggy_dir)


@pytest.fixture(scope="session")
def dup_balances_dir(baseline_dir, tmp_path_factory):
    """Daily balances with 10 duplicate (account_id, balance_date) pairs."""
    d = tmp_path_factory.mktemp("dup_bal")
    for f in baseline_dir.glob("*.parquet"):
        shutil.copy(f, d)
    generate_invalid_daily_balances(baseline_dir).to_parquet(
        d / "daily_balances.parquet", index=False
    )
    return d


@pytest.fixture(scope="session")
def orphan_fk_dir(baseline_dir, tmp_path_factory):
    """Accounts with 5 rows referencing a non-existent customer_id."""
    d = tmp_path_factory.mktemp("orphan_fk")
    for f in baseline_dir.glob("*.parquet"):
        shutil.copy(f, d)
    generate_invalid_accounts_orphan_fk(baseline_dir).to_parquet(
        d / "accounts.parquet", index=False
    )
    return d
