"""
tests/test_accounts_contract.py — Test suite for contracts/accounts.yaml
and scripts/validate_contract.py.

Tests
-----
  Structure     : contract YAML loads and validates structurally.
  Baseline      : clean data passes all checks with --now pinned.
  Freshness     : stale data fails; future timestamps fail; empty snapshot fails.
  Data rules    : missing columns, nulls, enum violations, malformed dates.
  Breaking-change detector: column removal, nullability widening, enum narrowing,
                  additive column (non-breaking), freshness weakening.
  Exit codes    : subprocess runs verify exit 0 / 1 / 2 codes.

Single command that runs all blocking checks (operational):
  python scripts/validate_contract.py contracts/accounts.yaml \\
      --data-dir data/baseline

CI synthetic freshness test:
  python scripts/validate_contract.py contracts/accounts.yaml \\
      --data-dir data/baseline --now 2024-01-30T12:00:00Z
  (Output will be labeled: SYNTHETIC FRESHNESS TEST — not proof of
   source-system timeliness.)
"""

from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd
import pytest

# ── Import the standalone validator script ────────────────────────────────────

_SCRIPTS_DIR = Path(__file__).parent.parent / "scripts"
_CONTRACTS_DIR = Path(__file__).parent.parent / "contracts"

def _load_vc_module():
    spec = importlib.util.spec_from_file_location(
        "validate_contract_script",
        _SCRIPTS_DIR / "validate_contract.py",
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod

vc = _load_vc_module()

CONTRACT_PATH = _CONTRACTS_DIR / "accounts.yaml"

# Pinned clock for deterministic baseline tests.
# Baseline ingested_at values range from 2024-01-30T00:00Z to ~07:00Z.
# Clock at noon means max age is 12 h → well within the 24 h SLA.
_BASELINE_NOW = datetime(2024, 1, 30, 12, 0, 0, tzinfo=timezone.utc)

# ── Helpers ───────────────────────────────────────────────────────────────────


def _one_valid_row(ingested_at: pd.Timestamp | None = None) -> dict:
    """Return one valid accounts row as a dict."""
    return {
        "account_id": "ACCT-0001",
        "customer_id": "CUST-0001",
        "branch_id": 1,
        "product_type": "CHECKING",
        "account_status": "ACTIVE",
        "currency": "USD",
        "opened_date": "2024-01-01",
        "closed_date": None,
        "ingested_at": ingested_at or pd.Timestamp("2024-01-30 06:00:00", tz="UTC"),
    }


def _minimal_conn(rows: list[dict], ref_tables: bool = True) -> duckdb.DuckDBPyConnection:
    """
    Build an in-memory DuckDB connection with an accounts table and minimal
    customers / branch_reference tables for FK checks.
    """
    conn = duckdb.connect()
    df = pd.DataFrame(rows)
    conn.register("accounts", df)

    if ref_tables:
        # Minimal customers table — just the customer_ids present in accounts
        cids = df["customer_id"].dropna().unique().tolist()
        cust_df = pd.DataFrame({
            "customer_id": cids,
            "customer_name": [f"Test {i}" for i in range(len(cids))],
            "created_date": ["2020-01-01"] * len(cids),
        })
        conn.register("customers", cust_df)

        # Minimal branch_reference
        bids = df["branch_id"].dropna().unique().tolist()
        br_df = pd.DataFrame({
            "branch_id": bids,
            "branch_name": [f"Branch {b}" for b in bids],
            "city": ["Testville"] * len(bids),
            "state_code": ["LM"] * len(bids),
        })
        conn.register("branch_reference", br_df)

    return conn


def _load_contract():
    return vc.load_contract(CONTRACT_PATH)


# ── Contract structure tests ──────────────────────────────────────────────────


class TestContractStructure:
    def test_contract_file_exists(self):
        assert CONTRACT_PATH.exists(), f"Contract not found: {CONTRACT_PATH}"

    def test_contract_file_has_yaml_extension(self):
        assert CONTRACT_PATH.suffix == ".yaml"

    def test_contract_loads_without_error(self):
        contract = _load_contract()
        assert isinstance(contract, dict)

    def test_format_is_lumen_dq_contract(self):
        contract = _load_contract()
        assert contract["contract_format"] == "lumen-dq-contract"

    def test_format_version_is_supported(self):
        contract = _load_contract()
        assert str(contract["format_version"]) in vc.SUPPORTED_FORMAT_VERSIONS

    def test_structure_validates_cleanly(self):
        contract = _load_contract()
        errors = vc.validate_contract_structure(contract)
        assert not errors, f"Contract structure errors:\n" + "\n".join(errors)

    def test_contract_name_is_accounts(self):
        contract = _load_contract()
        assert contract["name"] == "accounts"

    def test_version_is_semantic(self):
        """Version must have at least a major component."""
        contract = _load_contract()
        parts = str(contract["version"]).split(".")
        assert len(parts) >= 2
        assert parts[0].isdigit()

    def test_grain_is_account_id(self):
        contract = _load_contract()
        assert contract["grain"]["columns"] == ["account_id"]

    def test_producer_has_required_keys(self):
        contract = _load_contract()
        for k in vc.REQUIRED_PRODUCER_KEYS:
            assert k in contract["producer"], f"producer missing key: {k}"

    def test_consumer_has_required_keys(self):
        contract = _load_contract()
        for k in vc.REQUIRED_CONSUMER_KEYS:
            assert k in contract["consumer"], f"consumer missing key: {k}"

    def test_all_columns_have_required_keys(self):
        contract = _load_contract()
        for col in contract["columns"]:
            for k in vc.REQUIRED_COLUMN_KEYS:
                assert k in col, f"Column {col.get('name')!r} missing key: {k}"

    def test_every_constraint_key_is_supported(self):
        """Unknown constraint keys must not be silently ignored."""
        contract = _load_contract()
        for col in contract["columns"]:
            constraints = col.get("constraints", {})
            for k in constraints:
                if not k.startswith("_"):
                    assert k in vc.SUPPORTED_CONSTRAINT_KEYS, (
                        f"Column {col['name']!r} has unsupported constraint key: {k!r}"
                    )

    def test_freshness_has_required_keys(self):
        contract = _load_contract()
        for k in vc.REQUIRED_FRESHNESS_KEYS:
            assert k in contract["freshness"], f"freshness missing key: {k}"

    def test_freshness_measurement_uses_ingested_at(self):
        contract = _load_contract()
        assert contract["freshness"]["measurement"]["source_column"] == "ingested_at"

    def test_freshness_sla_is_24h(self):
        contract = _load_contract()
        assert contract["freshness"]["sla_hours"] == 24

    def test_freshness_reject_future_is_5_min(self):
        contract = _load_contract()
        assert contract["freshness"]["reject_future_minutes"] == 5

    def test_freshness_reject_empty_snapshot(self):
        contract = _load_contract()
        assert contract["freshness"]["reject_empty_snapshot"] is True

    def test_column_names_expected(self):
        contract = _load_contract()
        names = [c["name"] for c in contract["columns"]]
        expected = [
            "account_id", "customer_id", "branch_id", "product_type",
            "account_status", "currency", "opened_date", "closed_date", "ingested_at",
        ]
        assert names == expected

    def test_product_type_allowed_values(self):
        contract = _load_contract()
        col = next(c for c in contract["columns"] if c["name"] == "product_type")
        assert set(col["allowed_values"]) == {"CHECKING", "SAVINGS", "CD", "MONEY_MARKET"}

    def test_account_status_allowed_values(self):
        contract = _load_contract()
        col = next(c for c in contract["columns"] if c["name"] == "account_status")
        assert set(col["allowed_values"]) == {"ACTIVE", "FROZEN", "CLOSED"}

    def test_currency_allowed_values(self):
        contract = _load_contract()
        col = next(c for c in contract["columns"] if c["name"] == "currency")
        assert col["allowed_values"] == ["USD"]

    def test_closed_date_is_nullable(self):
        contract = _load_contract()
        col = next(c for c in contract["columns"] if c["name"] == "closed_date")
        assert col["nullable"] is True

    def test_ingested_at_is_not_nullable(self):
        contract = _load_contract()
        col = next(c for c in contract["columns"] if c["name"] == "ingested_at")
        assert col["nullable"] is False

    def test_foreign_keys_declared(self):
        contract = _load_contract()
        fk_cols = {fk["column"] for fk in contract["foreign_keys"]}
        assert "customer_id" in fk_cols
        assert "branch_id" in fk_cols

    def test_breaking_changes_section_present(self):
        contract = _load_contract()
        assert "breaking_changes" in contract

    def test_migration_policy_section_present(self):
        contract = _load_contract()
        assert "migration_policy" in contract

    def test_unsupported_constraint_key_detected(self):
        """validate_contract_structure must reject unknown constraint keys."""
        contract = _load_contract()
        # Inject an unsupported key into the first column that has constraints
        for col in contract["columns"]:
            if col.get("constraints"):
                col["constraints"]["_unsupported_future_key"] = "value"
                break
        # _-prefixed keys are meta keys and ignored — add a real unsupported key
        for col in contract["columns"]:
            if col.get("constraints"):
                col["constraints"]["xyzzy_unsupported"] = "value"
                break
        errors = vc.validate_contract_structure(contract)
        assert any("unsupported constraint key" in e.lower() for e in errors), (
            f"Expected unsupported-constraint error; got: {errors}"
        )


# ── Baseline data passes all checks ──────────────────────────────────────────


class TestBaselinePasses:
    """All checks must pass on one valid row with the pinned clock."""

    def test_single_valid_row_passes_all_checks(self):
        contract = _load_contract()
        conn = _minimal_conn([_one_valid_row()])
        diags = vc.run_all_checks(conn, contract, _BASELINE_NOW)
        failures = [d for d in diags if not d.passed]
        assert not failures, (
            "Expected no failures on valid row; got:\n"
            + "\n".join(f"  {d.check_id}: {d.message}" for d in failures)
        )

    def test_closed_account_with_closed_date_passes(self):
        contract = _load_contract()
        row = _one_valid_row()
        row["account_status"] = "CLOSED"
        row["closed_date"] = "2024-02-15"
        conn = _minimal_conn([row])
        diags = vc.run_all_checks(conn, contract, _BASELINE_NOW)
        lifecycle_failures = [
            d for d in diags
            if not d.passed and "lifecycle" in d.check_id
        ]
        assert not lifecycle_failures, (
            "CLOSED account with valid closed_date should pass lifecycle checks"
        )


# ── Missing column ────────────────────────────────────────────────────────────


class TestMissingColumn:
    def test_missing_currency_column_fails(self):
        contract = _load_contract()
        row = _one_valid_row()
        df = pd.DataFrame([row]).drop(columns=["currency"])
        conn = duckdb.connect()
        conn.register("accounts", df)
        # Add ref tables
        conn.register("customers", pd.DataFrame({
            "customer_id": ["CUST-0001"],
            "customer_name": ["Test"],
            "created_date": ["2020-01-01"],
        }))
        conn.register("branch_reference", pd.DataFrame({
            "branch_id": [1], "branch_name": ["B"], "city": ["C"], "state_code": ["LM"],
        }))
        diags = vc.run_all_checks(conn, contract, _BASELINE_NOW)
        missing_col_fails = [
            d for d in diags
            if not d.passed and "required_column" in d.check_type
        ]
        assert missing_col_fails, "Expected required_column failure for missing currency"
        assert any("currency" in d.check_id for d in missing_col_fails)

    def test_missing_column_is_blocking(self):
        contract = _load_contract()
        row = _one_valid_row()
        df = pd.DataFrame([row]).drop(columns=["product_type"])
        conn = duckdb.connect()
        conn.register("accounts", df)
        conn.register("customers", pd.DataFrame({
            "customer_id": ["CUST-0001"],
            "customer_name": ["Test"],
            "created_date": ["2020-01-01"],
        }))
        conn.register("branch_reference", pd.DataFrame({
            "branch_id": [1], "branch_name": ["B"], "city": ["C"], "state_code": ["LM"],
        }))
        diags = vc.run_all_checks(conn, contract, _BASELINE_NOW)
        col_fails = [
            d for d in diags
            if not d.passed and "required_column" in d.check_type
        ]
        assert all(d.severity == "BLOCKING" for d in col_fails)


# ── Null violations ───────────────────────────────────────────────────────────


class TestNullViolations:
    def test_null_account_id_fails(self):
        contract = _load_contract()
        row = _one_valid_row()
        row["account_id"] = None
        conn = _minimal_conn([row])
        diags = vc.run_all_checks(conn, contract, _BASELINE_NOW)
        nn_fails = [
            d for d in diags
            if not d.passed and d.check_type == "not_null" and "account_id" in d.check_id
        ]
        assert nn_fails, "Expected not_null failure for NULL account_id"

    def test_null_branch_id_fails(self):
        contract = _load_contract()
        row = _one_valid_row()
        row["branch_id"] = None
        conn = _minimal_conn([row])
        diags = vc.run_all_checks(conn, contract, _BASELINE_NOW)
        nn_fails = [
            d for d in diags
            if not d.passed and d.check_type == "not_null" and "branch_id" in d.check_id
        ]
        assert nn_fails, "Expected not_null failure for NULL branch_id"

    def test_null_violation_is_blocking(self):
        contract = _load_contract()
        row = _one_valid_row()
        row["currency"] = None
        conn = _minimal_conn([row])
        diags = vc.run_all_checks(conn, contract, _BASELINE_NOW)
        nn_fails = [
            d for d in diags
            if not d.passed and d.check_type == "not_null" and "currency" in d.check_id
        ]
        assert nn_fails
        assert all(d.severity == "BLOCKING" for d in nn_fails)

    def test_null_ingested_at_fails(self):
        contract = _load_contract()
        row = _one_valid_row()
        row["ingested_at"] = None
        conn = _minimal_conn([row])
        diags = vc.run_all_checks(conn, contract, _BASELINE_NOW)
        nn_fails = [
            d for d in diags
            if not d.passed and d.check_type == "not_null" and "ingested_at" in d.check_id
        ]
        assert nn_fails, "Expected not_null failure for NULL ingested_at"


# ── Enum violations ───────────────────────────────────────────────────────────


class TestEnumViolations:
    def test_invalid_product_type_fails(self):
        contract = _load_contract()
        row = _one_valid_row()
        row["product_type"] = "PREMIUM_CHECKING"  # not in allowed_values
        conn = _minimal_conn([row])
        diags = vc.run_all_checks(conn, contract, _BASELINE_NOW)
        av_fails = [
            d for d in diags
            if not d.passed and d.check_type == "allowed_values" and "product_type" in d.check_id
        ]
        assert av_fails, "Expected allowed_values failure for PREMIUM_CHECKING"
        assert av_fails[0].failing_rows >= 1

    def test_invalid_account_status_fails(self):
        contract = _load_contract()
        row = _one_valid_row()
        row["account_status"] = "PENDING"
        conn = _minimal_conn([row])
        diags = vc.run_all_checks(conn, contract, _BASELINE_NOW)
        av_fails = [
            d for d in diags
            if not d.passed and d.check_type == "allowed_values"
            and "account_status" in d.check_id
        ]
        assert av_fails, "Expected allowed_values failure for PENDING account_status"

    def test_invalid_currency_fails(self):
        contract = _load_contract()
        row = _one_valid_row()
        row["currency"] = "EUR"
        conn = _minimal_conn([row])
        diags = vc.run_all_checks(conn, contract, _BASELINE_NOW)
        av_fails = [
            d for d in diags
            if not d.passed and d.check_type == "allowed_values" and "currency" in d.check_id
        ]
        assert av_fails, "Expected allowed_values failure for EUR currency"

    def test_enum_violation_is_blocking(self):
        contract = _load_contract()
        row = _one_valid_row()
        row["product_type"] = "GOLD"
        conn = _minimal_conn([row])
        diags = vc.run_all_checks(conn, contract, _BASELINE_NOW)
        av_fails = [d for d in diags if not d.passed and d.check_type == "allowed_values"]
        assert av_fails
        assert all(d.severity == "BLOCKING" for d in av_fails)


# ── Malformed date ────────────────────────────────────────────────────────────


class TestMalformedDate:
    def test_malformed_opened_date_fails(self):
        """opened_date declared date_format: YYYY-MM-DD must reject unparseable strings."""
        contract = _load_contract()
        row = _one_valid_row()
        row["opened_date"] = "NOT-A-DATE"
        conn = _minimal_conn([row])
        diags = vc.run_all_checks(conn, contract, _BASELINE_NOW)
        fmt_fails = [
            d for d in diags
            if not d.passed and d.check_type == "date_format" and "opened_date" in d.check_id
        ]
        assert fmt_fails, (
            "Expected date_format failure for opened_date='NOT-A-DATE'"
        )
        assert fmt_fails[0].failing_rows >= 1

    def test_malformed_date_is_blocking(self):
        contract = _load_contract()
        row = _one_valid_row()
        row["opened_date"] = "2024/01/01"  # wrong separator
        conn = _minimal_conn([row])
        diags = vc.run_all_checks(conn, contract, _BASELINE_NOW)
        fmt_fails = [
            d for d in diags
            if not d.passed and d.check_type == "date_format" and "opened_date" in d.check_id
        ]
        assert fmt_fails
        assert all(d.severity == "BLOCKING" for d in fmt_fails)

    def test_valid_date_string_passes(self):
        contract = _load_contract()
        row = _one_valid_row()
        row["opened_date"] = "2023-06-15"
        conn = _minimal_conn([row])
        diags = vc.run_all_checks(conn, contract, _BASELINE_NOW)
        fmt_fails = [
            d for d in diags
            if not d.passed and d.check_type == "date_format" and "opened_date" in d.check_id
        ]
        assert not fmt_fails, "ISO-8601 date string should pass date_format check"


# ── Freshness: stale data ─────────────────────────────────────────────────────


class TestFreshnessStale:
    """
    Stale data: ingested_at more than sla_hours before the validation clock.

    With --now=2024-01-30T12:00:00Z and ingested_at=2024-01-28T00:00:00Z,
    the age is 60 hours — well past the 24-hour SLA.
    """

    def _stale_conn(self) -> duckdb.DuckDBPyConnection:
        """One row with ingested_at 48 h before the validation clock."""
        row = _one_valid_row(
            ingested_at=pd.Timestamp("2024-01-28 00:00:00", tz="UTC")
        )
        return _minimal_conn([row])

    def test_stale_data_fails_freshness_check(self):
        contract = _load_contract()
        conn = self._stale_conn()
        diags = vc.run_all_checks(conn, contract, _BASELINE_NOW)
        sla_fails = [
            d for d in diags
            if not d.passed and "freshness.sla" in d.check_id
        ]
        assert sla_fails, (
            "Expected freshness.sla to fail when ingested_at is 60h before clock"
        )
        assert sla_fails[0].failing_rows == 1

    def test_stale_freshness_is_blocking(self):
        contract = _load_contract()
        conn = self._stale_conn()
        diags = vc.run_all_checks(conn, contract, _BASELINE_NOW)
        sla_fails = [d for d in diags if not d.passed and "freshness" in d.check_id]
        assert sla_fails
        assert all(d.severity == "BLOCKING" for d in sla_fails)

    def test_stale_diagnostic_mentions_oldest_timestamp(self):
        contract = _load_contract()
        conn = self._stale_conn()
        diags = vc.run_all_checks(conn, contract, _BASELINE_NOW)
        sla_fails = [d for d in diags if not d.passed and "freshness.sla" in d.check_id]
        assert sla_fails
        # Message must mention the oldest timestamp, not the validation clock
        assert "2024-01-28" in sla_fails[0].message, (
            "Diagnostic should mention the oldest ingested_at value"
        )

    def test_fresh_data_passes_freshness(self):
        """Data ingested 6 h before clock passes the 24 h SLA."""
        contract = _load_contract()
        # 6 h before noon = 06:00 UTC on 2024-01-30 — same as baseline
        row = _one_valid_row(ingested_at=pd.Timestamp("2024-01-30 06:00:00", tz="UTC"))
        conn = _minimal_conn([row])
        diags = vc.run_all_checks(conn, contract, _BASELINE_NOW)
        sla_fails = [d for d in diags if not d.passed and "freshness.sla" in d.check_id]
        assert not sla_fails, "6h-old data should pass the 24h SLA"


# ── Freshness: future timestamps ─────────────────────────────────────────────


class TestFreshnessFuture:
    """
    Future timestamps: ingested_at more than reject_future_minutes ahead of clock.
    """

    def test_far_future_timestamp_fails(self):
        """ingested_at 10 minutes in the future → rejected."""
        contract = _load_contract()
        # Clock is noon; ingested_at is 10 min later → exceeds 5-min threshold
        now = datetime(2024, 1, 30, 12, 0, 0, tzinfo=timezone.utc)
        row = _one_valid_row(
            ingested_at=pd.Timestamp("2024-01-30 12:10:00", tz="UTC")
        )
        conn = _minimal_conn([row])
        diags = vc.run_all_checks(conn, contract, now)
        future_fails = [
            d for d in diags
            if not d.passed and "future_timestamps" in d.check_id
        ]
        assert future_fails, (
            "Expected freshness.future_timestamps to fail for 10-min-future timestamp"
        )
        assert future_fails[0].failing_rows == 1

    def test_within_tolerance_timestamp_passes(self):
        """ingested_at 3 minutes in the future is within the 5-min tolerance."""
        contract = _load_contract()
        now = datetime(2024, 1, 30, 12, 0, 0, tzinfo=timezone.utc)
        row = _one_valid_row(
            ingested_at=pd.Timestamp("2024-01-30 12:03:00", tz="UTC")
        )
        conn = _minimal_conn([row])
        diags = vc.run_all_checks(conn, contract, now)
        future_fails = [
            d for d in diags
            if not d.passed and "future_timestamps" in d.check_id
        ]
        assert not future_fails, (
            "3-min-future timestamp is within the 5-min tolerance and should pass"
        )

    def test_future_timestamp_is_blocking(self):
        contract = _load_contract()
        now = datetime(2024, 1, 30, 12, 0, 0, tzinfo=timezone.utc)
        row = _one_valid_row(
            ingested_at=pd.Timestamp("2024-01-30 12:30:00", tz="UTC")
        )
        conn = _minimal_conn([row])
        diags = vc.run_all_checks(conn, contract, now)
        future_fails = [d for d in diags if not d.passed and "future" in d.check_id]
        assert future_fails
        assert all(d.severity == "BLOCKING" for d in future_fails)


# ── Freshness: empty snapshot ─────────────────────────────────────────────────


class TestFreshnessEmptySnapshot:
    def test_empty_snapshot_fails(self):
        contract = _load_contract()
        conn = duckdb.connect()
        # Create empty accounts table with correct schema
        conn.execute(
            "CREATE TABLE accounts ("
            "account_id VARCHAR, customer_id VARCHAR, branch_id BIGINT, "
            "product_type VARCHAR, account_status VARCHAR, currency VARCHAR, "
            "opened_date VARCHAR, closed_date VARCHAR, ingested_at TIMESTAMP"
            ")"
        )
        conn.execute(
            "CREATE TABLE customers (customer_id VARCHAR, customer_name VARCHAR, created_date VARCHAR)"
        )
        conn.execute(
            "CREATE TABLE branch_reference (branch_id BIGINT, branch_name VARCHAR, "
            "city VARCHAR, state_code VARCHAR)"
        )
        diags = vc.run_all_checks(conn, contract, _BASELINE_NOW)
        empty_fails = [
            d for d in diags
            if not d.passed and "empty_snapshot" in d.check_id
        ]
        assert empty_fails, "Empty snapshot must be rejected"

    def test_empty_snapshot_is_blocking(self):
        contract = _load_contract()
        conn = duckdb.connect()
        conn.execute(
            "CREATE TABLE accounts ("
            "account_id VARCHAR, customer_id VARCHAR, branch_id BIGINT, "
            "product_type VARCHAR, account_status VARCHAR, currency VARCHAR, "
            "opened_date VARCHAR, closed_date VARCHAR, ingested_at TIMESTAMP"
            ")"
        )
        conn.execute(
            "CREATE TABLE customers (customer_id VARCHAR, customer_name VARCHAR, created_date VARCHAR)"
        )
        conn.execute(
            "CREATE TABLE branch_reference (branch_id BIGINT, branch_name VARCHAR, "
            "city VARCHAR, state_code VARCHAR)"
        )
        diags = vc.run_all_checks(conn, contract, _BASELINE_NOW)
        empty_fails = [d for d in diags if not d.passed and "empty_snapshot" in d.check_id]
        assert empty_fails
        assert all(d.severity == "BLOCKING" for d in empty_fails)


# ── Lifecycle constraints ─────────────────────────────────────────────────────


class TestLifecycleConstraints:
    def test_active_account_with_closed_date_fails(self):
        """ACTIVE account must not have a closed_date."""
        contract = _load_contract()
        row = _one_valid_row()
        row["account_status"] = "ACTIVE"
        row["closed_date"] = "2024-03-01"
        conn = _minimal_conn([row])
        diags = vc.run_all_checks(conn, contract, _BASELINE_NOW)
        lc_fails = [
            d for d in diags
            if not d.passed and "lifecycle" in d.check_id and "non_closed" in d.check_id
        ]
        assert lc_fails, "ACTIVE account with closed_date should fail lifecycle check"

    def test_closed_account_without_closed_date_fails(self):
        """CLOSED account must have a closed_date."""
        contract = _load_contract()
        row = _one_valid_row()
        row["account_status"] = "CLOSED"
        row["closed_date"] = None
        conn = _minimal_conn([row])
        diags = vc.run_all_checks(conn, contract, _BASELINE_NOW)
        lc_fails = [
            d for d in diags
            if not d.passed and "lifecycle" in d.check_id and "required_for_closed" in d.check_id
        ]
        assert lc_fails, "CLOSED account without closed_date should fail lifecycle check"

    def test_closed_date_before_opened_date_fails(self):
        """closed_date must be strictly after opened_date."""
        contract = _load_contract()
        row = _one_valid_row()
        row["account_status"] = "CLOSED"
        row["opened_date"] = "2024-06-01"
        row["closed_date"] = "2024-05-01"  # before opened_date
        conn = _minimal_conn([row])
        diags = vc.run_all_checks(conn, contract, _BASELINE_NOW)
        lc_fails = [
            d for d in diags
            if not d.passed and "lifecycle" in d.check_id
            and "after_opened" in d.check_id
        ]
        assert lc_fails, "closed_date before opened_date should fail lifecycle check"


# ── Breaking-change detector ──────────────────────────────────────────────────


def _make_contract(overrides: dict | None = None) -> dict:
    """Return a minimal but structurally valid contract for breaking-change tests."""
    base = {
        "contract_format": "lumen-dq-contract",
        "format_version": "1",
        "name": "accounts",
        "version": "2.0.0",
        "producer": {
            "name": "core-banking",
            "contact_role": "de-oncall",
            "escalation_role": "de-lead",
        },
        "consumer": {
            "name": "reports",
            "contact_role": "analytics-oncall",
            "escalation_role": "analytics-lead",
        },
        "table": "accounts",
        "grain": {"columns": ["account_id"]},
        "primary_key": {"columns": ["account_id"]},
        "columns": [
            {
                "name": "account_id",
                "logical_type": "identifier",
                "physical_type": "VARCHAR",
                "nullable": False,
            },
            {
                "name": "product_type",
                "logical_type": "enum",
                "physical_type": "VARCHAR",
                "nullable": False,
                "allowed_values": ["CHECKING", "SAVINGS", "CD", "MONEY_MARKET"],
            },
        ],
        "foreign_keys": [
            {
                "column": "customer_id",
                "references": {"table": "customers", "column": "customer_id"},
                "severity": "BLOCKING",
            }
        ],
        "freshness": {
            "measurement": {"source_column": "ingested_at", "source_table": "accounts"},
            "sla_hours": 24,
            "reject_future_minutes": 5,
            "reject_empty_snapshot": True,
        },
    }
    if overrides:
        base.update(overrides)
    return base


class TestBreakingChanges:
    def test_no_breaking_changes_returns_empty(self):
        base = _make_contract()
        current = _make_contract()
        breaking = vc.check_breaking_changes(base, current)
        assert breaking == [], "Identical contracts → no breaking changes"

    def test_removing_column_is_breaking(self):
        base = _make_contract()
        current = _make_contract()
        # Remove account_id from current
        current["columns"] = [c for c in current["columns"] if c["name"] != "account_id"]
        breaking = vc.check_breaking_changes(base, current)
        assert any(bc.category == "removed_column" for bc in breaking), (
            f"Expected removed_column breaking change; got: {breaking}"
        )

    def test_widening_nullable_is_breaking(self):
        """Changing nullable: false → nullable: true is breaking."""
        base = _make_contract()
        current = _make_contract()
        # Widen account_id to nullable
        for col in current["columns"]:
            if col["name"] == "account_id":
                col["nullable"] = True
        breaking = vc.check_breaking_changes(base, current)
        assert any(bc.category == "nullability_widened" for bc in breaking), (
            f"Expected nullability_widened breaking change; got: {breaking}"
        )

    def test_narrowing_allowed_values_is_breaking(self):
        """Removing a value from allowed_values is breaking."""
        base = _make_contract()
        current = _make_contract()
        for col in current["columns"]:
            if col["name"] == "product_type":
                col["allowed_values"] = ["CHECKING", "SAVINGS"]  # removed CD, MONEY_MARKET
        breaking = vc.check_breaking_changes(base, current)
        assert any(bc.category == "allowed_values_narrowed" for bc in breaking), (
            f"Expected allowed_values_narrowed breaking change; got: {breaking}"
        )

    def test_type_change_is_breaking(self):
        base = _make_contract()
        current = _make_contract()
        for col in current["columns"]:
            if col["name"] == "account_id":
                col["physical_type"] = "INTEGER"  # was VARCHAR
        breaking = vc.check_breaking_changes(base, current)
        assert any(bc.category == "type_changed" for bc in breaking)

    def test_grain_change_is_breaking(self):
        base = _make_contract()
        current = _make_contract()
        current["grain"] = {"columns": ["account_id", "product_type"]}  # composite now
        breaking = vc.check_breaking_changes(base, current)
        assert any(bc.category == "grain_changed" for bc in breaking)

    def test_freshness_weakened_is_breaking(self):
        """Increasing sla_hours is a breaking change."""
        base = _make_contract()
        current = _make_contract()
        current["freshness"]["sla_hours"] = 48  # was 24
        breaking = vc.check_breaking_changes(base, current)
        assert any(bc.category == "freshness_weakened" for bc in breaking)

    def test_additive_nullable_column_is_not_breaking(self):
        """Adding a nullable column with no allowed_values is non-breaking."""
        base = _make_contract()
        current = _make_contract()
        current["columns"].append({
            "name": "notes",
            "logical_type": "text",
            "physical_type": "VARCHAR",
            "nullable": True,  # explicitly nullable = non-breaking
        })
        breaking = vc.check_breaking_changes(base, current)
        assert not breaking, (
            f"Adding a nullable column should be non-breaking; got: {breaking}"
        )

    def test_strengthening_constraint_is_not_breaking(self):
        """Changing nullable: true → nullable: false is non-breaking (stricter producer)."""
        base = _make_contract()
        # Make account_id nullable in base
        for col in base["columns"]:
            if col["name"] == "account_id":
                col["nullable"] = True
        current = _make_contract()
        # Current has nullable: false (stricter)
        breaking = vc.check_breaking_changes(base, current)
        nullability_breaks = [bc for bc in breaking if bc.category == "nullability_widened"]
        assert not nullability_breaks, (
            "Tightening nullability (true → false) is non-breaking"
        )

    def test_major_version_bump_required_for_breaking(self):
        """Breaking changes without a major version bump → enforcement error."""
        base = _make_contract({"version": "2.0.0"})
        current = _make_contract({"version": "2.1.0"})  # only minor bump
        # Remove a column — breaking
        current["columns"] = [c for c in current["columns"] if c["name"] != "account_id"]
        breaking = vc.check_breaking_changes(base, current)
        enforcement = vc.require_major_version_bump(base, current, breaking)
        assert enforcement, (
            "Expected enforcement error when breaking change present without major bump"
        )

    def test_major_version_bump_satisfies_requirement(self):
        """If major version is bumped, enforcement passes (no enforcement errors)."""
        base = _make_contract({"version": "2.0.0"})
        current = _make_contract({"version": "3.0.0"})  # major bump
        # Remove a column — breaking
        current["columns"] = [c for c in current["columns"] if c["name"] != "account_id"]
        breaking = vc.check_breaking_changes(base, current)
        enforcement = vc.require_major_version_bump(base, current, breaking)
        assert not enforcement, (
            "Major version bump should satisfy enforcement; no enforcement errors expected"
        )

    def test_foreign_key_removal_is_breaking(self):
        base = _make_contract()
        current = _make_contract({"foreign_keys": []})  # removed all FKs
        breaking = vc.check_breaking_changes(base, current)
        assert any(bc.category == "foreign_key_removed" for bc in breaking)

    def test_reducing_sla_hours_is_not_breaking(self):
        """Decreasing sla_hours (faster freshness delivery) is non-breaking."""
        base = _make_contract()
        base["freshness"]["sla_hours"] = 24
        current = _make_contract()
        current["freshness"]["sla_hours"] = 12  # faster — better for consumers
        breaking = vc.check_breaking_changes(base, current)
        freshness_breaks = [bc for bc in breaking if bc.category == "freshness_weakened"]
        assert not freshness_breaks, "Reducing sla_hours is non-breaking"


# ── Identifier constraints ────────────────────────────────────────────────────


class TestIdentifierConstraints:
    def test_malformed_account_id_fails(self):
        """account_id must match pattern ^ACCT-[0-9]{4,}$."""
        contract = _load_contract()
        row = _one_valid_row()
        row["account_id"] = "ACC-1"  # wrong prefix, too few digits
        conn = _minimal_conn([row])
        diags = vc.run_all_checks(conn, contract, _BASELINE_NOW)
        pattern_fails = [
            d for d in diags
            if not d.passed and d.check_type == "pattern" and "account_id" in d.check_id
        ]
        assert pattern_fails, "Malformed account_id should fail pattern check"

    def test_valid_account_id_format_passes(self):
        contract = _load_contract()
        row = _one_valid_row()
        row["account_id"] = "ACCT-0042"
        conn = _minimal_conn([row])
        diags = vc.run_all_checks(conn, contract, _BASELINE_NOW)
        pattern_fails = [
            d for d in diags
            if not d.passed and d.check_type == "pattern" and "account_id" in d.check_id
        ]
        assert not pattern_fails

    def test_malformed_customer_id_fails(self):
        contract = _load_contract()
        row = _one_valid_row()
        row["customer_id"] = "CUST-X"  # not numeric
        conn = _minimal_conn([row], ref_tables=False)
        # Minimal ref tables with the customer ID
        conn.register("customers", pd.DataFrame({
            "customer_id": ["CUST-X"],
            "customer_name": ["Test"],
            "created_date": ["2020-01-01"],
        }))
        conn.register("branch_reference", pd.DataFrame({
            "branch_id": [1], "branch_name": ["B"], "city": ["C"], "state_code": ["LM"],
        }))
        diags = vc.run_all_checks(conn, contract, _BASELINE_NOW)
        pattern_fails = [
            d for d in diags
            if not d.passed and d.check_type == "pattern" and "customer_id" in d.check_id
        ]
        assert pattern_fails, "Malformed customer_id should fail pattern check"


# ── Exit-code tests (subprocess) ──────────────────────────────────────────────


def _run_validate(data_dir: Path, extra_args: list[str] | None = None) -> subprocess.CompletedProcess:
    """Run scripts/validate_contract.py as a subprocess."""
    import os
    env = os.environ.copy()
    src_dir = Path(__file__).parent.parent / "src"
    existing = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = str(src_dir) + (":" + existing if existing else "")

    script = Path(__file__).parent.parent / "scripts" / "validate_contract.py"
    contract = Path(__file__).parent.parent / "contracts" / "accounts.yaml"
    cmd = [sys.executable, str(script), str(contract), "--data-dir", str(data_dir)]
    if extra_args:
        cmd.extend(extra_args)
    return subprocess.run(cmd, capture_output=True, text=True, env=env)


class TestExitCodes:
    def test_exit_0_on_valid_data(self, baseline_dir):
        """Baseline data with --now pinned to noon on as-of date → exit 0."""
        result = _run_validate(
            baseline_dir,
            ["--now", "2024-01-30T12:00:00Z"],
        )
        assert result.returncode == 0, (
            f"Expected exit 0; got {result.returncode}\n"
            f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )

    def test_exit_2_on_missing_contract(self, tmp_path):
        """Non-existent contract file → exit 2."""
        import os
        env = os.environ.copy()
        src_dir = Path(__file__).parent.parent / "src"
        env["PYTHONPATH"] = str(src_dir)
        script = Path(__file__).parent.parent / "scripts" / "validate_contract.py"
        result = subprocess.run(
            [sys.executable, str(script), str(tmp_path / "no_contract.yaml"),
             "--data-dir", str(tmp_path)],
            capture_output=True, text=True, env=env,
        )
        assert result.returncode == 2

    def test_exit_2_on_missing_data_dir(self):
        """Non-existent data directory → exit 2."""
        import os
        env = os.environ.copy()
        src_dir = Path(__file__).parent.parent / "src"
        env["PYTHONPATH"] = str(src_dir)
        script = Path(__file__).parent.parent / "scripts" / "validate_contract.py"
        contract = Path(__file__).parent.parent / "contracts" / "accounts.yaml"
        result = subprocess.run(
            [sys.executable, str(script), str(contract),
             "--data-dir", "/does/not/exist/xyz123"],
            capture_output=True, text=True, env=env,
        )
        assert result.returncode == 2

    def test_exit_0_no_data_flag(self):
        """--no-data flag validates contract structure only → exit 0."""
        import os
        env = os.environ.copy()
        src_dir = Path(__file__).parent.parent / "src"
        env["PYTHONPATH"] = str(src_dir)
        script = Path(__file__).parent.parent / "scripts" / "validate_contract.py"
        contract = Path(__file__).parent.parent / "contracts" / "accounts.yaml"
        result = subprocess.run(
            [sys.executable, str(script), str(contract), "--no-data"],
            capture_output=True, text=True, env=env,
        )
        assert result.returncode == 0, (
            f"--no-data on valid contract should exit 0; got {result.returncode}\n"
            f"stdout: {result.stdout}\nstderr: {result.stderr}"
        )

    def test_json_output_written_to_file(self, baseline_dir, tmp_path):
        """--out writes valid JSON with required keys."""
        out_file = tmp_path / "results.json"
        result = _run_validate(
            baseline_dir,
            ["--now", "2024-01-30T12:00:00Z", "--out", str(out_file)],
        )
        assert result.returncode == 0
        assert out_file.exists()
        data = json.loads(out_file.read_text())
        for key in ("contract", "version", "total", "passed", "failed",
                    "blocking_failures", "checks", "validation_clock"):
            assert key in data, f"Missing key in JSON output: {key}"

    def test_json_checks_have_required_fields(self, baseline_dir, tmp_path):
        out_file = tmp_path / "check_fields.json"
        _run_validate(
            baseline_dir,
            ["--now", "2024-01-30T12:00:00Z", "--out", str(out_file)],
        )
        data = json.loads(out_file.read_text())
        for check in data["checks"]:
            for field in ("check_id", "check_type", "severity", "passed", "message"):
                assert field in check, f"Missing field {field!r} in check {check}"

    def test_synthetic_freshness_label_in_stderr(self, baseline_dir):
        """--now must be labeled as SYNTHETIC in stderr."""
        result = _run_validate(
            baseline_dir,
            ["--now", "2024-01-30T12:00:00Z"],
        )
        assert "SYNTHETIC" in result.stderr, (
            f"Expected SYNTHETIC label in stderr when --now is used; "
            f"got: {result.stderr}"
        )
