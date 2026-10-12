"""
Layer 2 — Grain uniqueness tests.

Each of the four core tables must have a unique primary key.
Invalid fixtures must cause the grain check to FAIL (negative testing).
"""

import pytest

from lumen_dq.checks import check_grain_uniqueness
from lumen_dq.db import open_connection


# ── Baseline: all four tables must have unique grain ──────────────────────────


def test_branch_reference_grain_unique(baseline_conn):
    r = check_grain_uniqueness(baseline_conn, "branch_reference", ["branch_id"])
    assert r.passed, r.message


def test_customers_grain_unique(baseline_conn):
    r = check_grain_uniqueness(baseline_conn, "customers", ["customer_id"])
    assert r.passed, r.message


def test_accounts_grain_unique(baseline_conn):
    r = check_grain_uniqueness(baseline_conn, "accounts", ["account_id"])
    assert r.passed, r.message


def test_daily_balances_grain_unique(baseline_conn):
    r = check_grain_uniqueness(
        baseline_conn, "daily_balances", ["account_id", "balance_date"]
    )
    assert r.passed, r.message


# ── Negative tests: invalid fixtures must fail ────────────────────────────────


def test_buggy_branch_reference_grain_fails(buggy_conn):
    """
    The buggy fixture has 5 duplicate branch_ids (47 rows, 42 unique).
    The grain check must detect this and report 5 failing rows.
    This is the gate that would have blocked the 11-day incident.
    """
    r = check_grain_uniqueness(buggy_conn, "branch_reference", ["branch_id"])
    assert not r.passed, "Grain check should FAIL on the buggy branch_reference fixture"
    assert r.failing_rows == 5, (
        f"Expected 5 duplicate branch rows, got {r.failing_rows}"
    )


def test_dup_daily_balances_grain_fails(dup_balances_dir):
    """Fixture with 10 duplicate (account_id, balance_date) pairs must fail."""
    conn = open_connection(dup_balances_dir)
    r = check_grain_uniqueness(conn, "daily_balances", ["account_id", "balance_date"])
    assert not r.passed, "Grain check should FAIL on duplicate daily_balances fixture"
    assert r.failing_rows == 10, (
        f"Expected 10 duplicate balance rows, got {r.failing_rows}"
    )
