"""
Tests for the YAML data contract validator (contracts/accounts.yml).
"""

from pathlib import Path

import pytest

from lumen_dq.db import open_connection
from lumen_dq.validate_contract import load_contract, validate

CONTRACT_PATH = Path(__file__).parent.parent / "contracts" / "accounts.yml"


def test_contract_file_exists():
    assert CONTRACT_PATH.exists(), f"Contract not found at {CONTRACT_PATH}"


def test_contract_loads_and_has_required_keys():
    contract = load_contract(CONTRACT_PATH)
    assert contract["table"] == "accounts"
    assert "grain" in contract
    assert "columns" in contract
    assert "foreign_keys" in contract


def test_contract_grain_is_account_id():
    contract = load_contract(CONTRACT_PATH)
    assert contract["grain"] == ["account_id"]


def test_contract_passes_on_baseline(baseline_conn):
    """All rules in the accounts contract must pass against baseline data."""
    contract = load_contract(CONTRACT_PATH)
    results = validate(baseline_conn, contract)
    failures = [r for r in results if not r.passed]
    assert not failures, (
        "Contract validation failed on baseline:\n"
        + "\n".join(f"  {r.name}: {r.message}" for r in failures)
    )


def test_contract_fk_fails_on_orphan_accounts(orphan_fk_dir):
    """
    The fixture has 5 accounts with customer_id='CUST-XXXX'.
    The FK check for customer_id→customers.customer_id must fail.
    """
    conn = open_connection(orphan_fk_dir)
    contract = load_contract(CONTRACT_PATH)
    results = validate(conn, contract)
    fk_failures = [
        r for r in results
        if not r.passed and "customer_id" in r.name and "fk:" in r.name
    ]
    assert fk_failures, (
        "Expected FK check for customer_id to fail with orphan accounts fixture"
    )
    assert fk_failures[0].failing_rows == 5


def test_contract_grain_fails_on_orphan_accounts(orphan_fk_dir):
    """
    The orphan fixture appends 5 extra rows, but each has a unique account_id,
    so the grain check should still pass (orphan = FK violation, not grain violation).
    """
    conn = open_connection(orphan_fk_dir)
    contract = load_contract(CONTRACT_PATH)
    results = validate(conn, contract)
    grain_results = [r for r in results if "grain_uniqueness" in r.name]
    assert grain_results, "Expected at least one grain result"
    # The grain check should still pass — orphan rows have unique account_ids
    assert grain_results[0].passed, (
        "Orphan FK rows have unique account_ids; grain check should not fail"
    )
