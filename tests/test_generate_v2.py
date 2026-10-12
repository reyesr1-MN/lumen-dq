"""
Tests for the v2 generator: CLI args, schema, baseline guarantees,
scenario generation, mutation manifests, and dataset inventory.
"""

from __future__ import annotations

import json
import shutil
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from lumen_dq.generate import (
    ANOMALY_FACTOR,
    DEFAULT_AS_OF_DATE,
    N_ACCOUNTS,
    N_BRANCHES,
    N_CUSTOMERS,
    N_DATES,
    PRODUCT_TYPES,
    ACCOUNT_STATUSES,
    SCENARIO_NAMES,
    _build_baseline,
    generate_dataset,
    generate_scenario,
)
from lumen_dq.inventory import (
    compute_inventory,
    verify_inventory,
    write_inventory,
    _count_balance_anomalies,
)


# ── Fixtures ──────────────────────────────────────────────────────────────────


@pytest.fixture(scope="module")
def tables():
    """Baseline tables built with default seed/date (in-memory, no disk I/O)."""
    return _build_baseline()


@pytest.fixture(scope="module")
def dataset_dir(tmp_path_factory):
    """Full baseline written to a temp directory; returns the baseline/ subdir."""
    d = tmp_path_factory.mktemp("dataset")
    generate_dataset(d)
    return d


# ── CLI reproducibility ───────────────────────────────────────────────────────


def test_same_seed_same_output():
    """Identical seed + as_of_date must produce bit-identical DataFrames."""
    t1 = _build_baseline(seed=42, as_of_date=date(2024, 1, 30))
    t2 = _build_baseline(seed=42, as_of_date=date(2024, 1, 30))
    for name in ("branch_reference", "customers", "accounts", "daily_balances"):
        pd.testing.assert_frame_equal(t1[name], t2[name])


def test_different_seed_different_output():
    """Different seeds must produce different balance data."""
    t1 = _build_baseline(seed=42)
    t2 = _build_baseline(seed=99)
    assert not t1["daily_balances"]["closing_balance_cents"].equals(
        t2["daily_balances"]["closing_balance_cents"]
    )


def test_different_as_of_date_shifts_dates():
    """Different as_of_date must shift the balance_date window."""
    t1 = _build_baseline(as_of_date=date(2024, 1, 30))
    t2 = _build_baseline(as_of_date=date(2024, 2, 29))
    dates1 = set(t1["daily_balances"]["balance_date"])
    dates2 = set(t2["daily_balances"]["balance_date"])
    assert dates1 != dates2
    assert "2024-01-01" in dates1
    assert "2024-02-01" in dates2


# ── Schema: minimum required columns ─────────────────────────────────────────


def test_branch_reference_columns(tables):
    assert {"branch_id", "branch_name", "state_code"} <= set(tables["branch_reference"].columns)


def test_customers_columns(tables):
    assert {"customer_id", "customer_name", "created_date"} <= set(tables["customers"].columns)


def test_accounts_columns(tables):
    required = {
        "account_id", "customer_id", "branch_id", "product_type",
        "account_status", "currency", "opened_date", "closed_date", "ingested_at",
    }
    assert required <= set(tables["accounts"].columns)


def test_daily_balances_columns(tables):
    assert {"account_id", "balance_date", "closing_balance_cents"} <= set(
        tables["daily_balances"].columns
    )


# ── Schema: enums and values ──────────────────────────────────────────────────


def test_product_type_enum(tables):
    bad = ~tables["accounts"]["product_type"].isin(PRODUCT_TYPES)
    assert not bad.any(), f"Invalid product_type: {tables['accounts'].loc[bad, 'product_type'].unique()}"


def test_account_status_enum(tables):
    bad = ~tables["accounts"]["account_status"].isin(ACCOUNT_STATUSES)
    assert not bad.any()


def test_currency_is_usd(tables):
    assert (tables["accounts"]["currency"] == "USD").all()


def test_closing_balance_cents_positive(tables):
    assert (tables["daily_balances"]["closing_balance_cents"] > 0).all()


# ── Grain: uniqueness ─────────────────────────────────────────────────────────


def test_branch_reference_pk_unique(tables):
    assert tables["branch_reference"]["branch_id"].nunique() == N_BRANCHES


def test_customers_pk_unique(tables):
    assert tables["customers"]["customer_id"].nunique() == N_CUSTOMERS


def test_accounts_pk_unique(tables):
    assert tables["accounts"]["account_id"].nunique() == N_ACCOUNTS


def test_daily_balances_grain_unique(tables):
    grain = tables["daily_balances"].duplicated(["account_id", "balance_date"])
    assert not grain.any()


# ── Baseline size guarantees ──────────────────────────────────────────────────


def test_branch_reference_row_count(tables):
    assert len(tables["branch_reference"]) == N_BRANCHES


def test_customers_row_count(tables):
    assert len(tables["customers"]) == N_CUSTOMERS


def test_accounts_row_count(tables):
    assert len(tables["accounts"]) == N_ACCOUNTS


def test_daily_balances_row_count(tables):
    assert len(tables["daily_balances"]) == N_ACCOUNTS * N_DATES


# ── Baseline validity guarantees ─────────────────────────────────────────────


def test_every_customer_has_at_least_one_account(tables):
    account_customers = set(tables["accounts"]["customer_id"])
    all_customers = set(tables["customers"]["customer_id"])
    missing = all_customers - account_customers
    assert not missing, f"{len(missing)} customers have no account"


def test_every_branch_has_at_least_one_account(tables):
    account_branches = set(tables["accounts"]["branch_id"].dropna())
    all_branches = set(tables["branch_reference"]["branch_id"])
    missing = all_branches - account_branches
    assert not missing, f"{len(missing)} branches have no account"


def test_every_account_has_all_balance_dates(tables):
    """Every account_id should have exactly N_DATES balance rows."""
    counts = tables["daily_balances"].groupby("account_id").size()
    wrong = counts[counts != N_DATES]
    assert len(wrong) == 0, f"{len(wrong)} accounts have ≠ {N_DATES} balance rows"


def test_all_fk_relationships_resolve(tables):
    accts = tables["accounts"]
    custs = set(tables["customers"]["customer_id"])
    branches = set(tables["branch_reference"]["branch_id"])
    bals = tables["daily_balances"]
    acct_ids = set(tables["accounts"]["account_id"])

    orphan_cust = ~accts["customer_id"].isin(custs)
    orphan_branch = ~accts["branch_id"].dropna().isin(branches)
    orphan_balance = ~bals["account_id"].isin(acct_ids)

    assert not orphan_cust.any(), "Orphan account→customer FK"
    assert not orphan_branch.any(), "Orphan account→branch FK"
    assert not orphan_balance.any(), "Orphan balance→account FK"


def test_dates_and_lifecycle_consistent(tables):
    """opened_date must be < first balance date; created_date <= opened_date."""
    accts = tables["accounts"]
    bal_start = DEFAULT_AS_OF_DATE - timedelta(days=N_DATES - 1)

    opened = pd.to_datetime(accts["opened_date"]).dt.date
    # All accounts opened before balance window starts
    assert (opened < bal_start).all(), "Some accounts opened on/after balance start"

    # customers: created_date <= opened_date (account can't predate customer)
    cust_dates = tables["customers"].set_index("customer_id")["created_date"]
    for _, row in accts.iterrows():
        cc = date.fromisoformat(str(cust_dates[row["customer_id"]]))
        od = date.fromisoformat(str(row["opened_date"]))
        assert cc <= od, f"Account {row['account_id']}: opened before customer created"


def test_closed_date_only_for_closed_accounts(tables):
    accts = tables["accounts"]
    closed_rows = accts[accts["account_status"] == "CLOSED"]
    active_rows = accts[accts["account_status"] != "CLOSED"]

    assert closed_rows["closed_date"].notna().all(), "CLOSED account missing closed_date"
    assert active_rows["closed_date"].isna().all(), "Non-CLOSED account has closed_date"


def test_customers_with_multiple_accounts_exist(tables):
    counts = tables["accounts"].groupby("customer_id").size()
    multi = (counts > 1).sum()
    assert multi > 0, "No customers with multiple accounts"


def test_customers_with_cross_branch_accounts_exist(tables):
    branches_per_cust = tables["accounts"].groupby("customer_id")["branch_id"].nunique()
    cross_branch = (branches_per_cust > 1).sum()
    assert cross_branch > 0, "No customers with accounts in multiple branches"


def test_balance_variation_within_anomaly_threshold(tables):
    """No single day-over-day move should exceed ANOMALY_FACTOR."""
    count = _count_balance_anomalies(tables["daily_balances"], ANOMALY_FACTOR)
    assert count == 0, f"{count} balance anomalies in baseline (should be 0)"


def test_balance_date_range(tables):
    dates = pd.to_datetime(tables["daily_balances"]["balance_date"]).dt.date
    assert dates.min() == DEFAULT_AS_OF_DATE - timedelta(days=N_DATES - 1)
    assert dates.max() == DEFAULT_AS_OF_DATE


def test_ingested_at_is_utc(tables):
    ts = pd.to_datetime(tables["accounts"]["ingested_at"], utc=True)
    assert ts.dt.tz is not None


# ── Inventory ─────────────────────────────────────────────────────────────────


def test_inventory_baseline_checks_pass(tables):
    inv = compute_inventory(tables, seed=42, as_of_date=DEFAULT_AS_OF_DATE)
    failures = [k for k, v in inv["checks"].items() if not v]
    assert not failures, f"Inventory checks failed: {failures}"


def test_inventory_writes_and_verifies(tmp_path, tables):
    inv = compute_inventory(tables, seed=42, as_of_date=DEFAULT_AS_OF_DATE)
    path = tmp_path / "inventory.json"
    write_inventory(inv, path)
    ok, failures = verify_inventory(path)
    assert ok, f"Inventory verification failed: {failures}"


def test_inventory_row_counts_match(tables):
    inv = compute_inventory(tables, seed=42, as_of_date=DEFAULT_AS_OF_DATE)
    assert inv["tables"]["branch_reference"]["row_count"] == N_BRANCHES
    assert inv["tables"]["customers"]["row_count"] == N_CUSTOMERS
    assert inv["tables"]["accounts"]["row_count"] == N_ACCOUNTS
    assert inv["tables"]["daily_balances"]["row_count"] == N_ACCOUNTS * N_DATES


def test_inventory_checksums_are_deterministic():
    t1 = _build_baseline(seed=42)
    t2 = _build_baseline(seed=42)
    inv1 = compute_inventory(t1, seed=42, as_of_date=DEFAULT_AS_OF_DATE)
    inv2 = compute_inventory(t2, seed=42, as_of_date=DEFAULT_AS_OF_DATE)
    for table in ("branch_reference", "customers", "accounts", "daily_balances"):
        c1 = inv1["tables"][table].get("checksum_sha256")
        c2 = inv2["tables"][table].get("checksum_sha256")
        assert c1 == c2, f"Checksum mismatch for {table}: {c1} vs {c2}"


# ── generate_dataset writes files ────────────────────────────────────────────


def test_dataset_writes_parquet_files(dataset_dir):
    baseline = dataset_dir / "baseline"
    for name in ("branch_reference", "customers", "accounts", "daily_balances"):
        assert (baseline / f"{name}.parquet").exists(), f"Missing {name}.parquet"


def test_dataset_writes_inventory_json(dataset_dir):
    baseline = dataset_dir / "baseline"
    assert (baseline / "inventory.json").exists()
    data = json.loads((baseline / "inventory.json").read_text())
    assert data["seed"] == 42


# ── Scenario generation ───────────────────────────────────────────────────────


@pytest.mark.parametrize("name", SCENARIO_NAMES)
def test_each_scenario_writes_manifest(tmp_path, name):
    """Every scenario must produce a manifest.json."""
    scen_dir = generate_scenario(name, tmp_path)
    manifest_path = scen_dir / "manifest.json"
    assert manifest_path.exists(), f"No manifest.json for scenario {name}"
    manifest = json.loads(manifest_path.read_text())
    assert manifest["scenario"] == name
    assert len(manifest["mutations"]) >= 1


@pytest.mark.parametrize("name", SCENARIO_NAMES)
def test_each_scenario_writes_inventory(tmp_path, name):
    scen_dir = generate_scenario(name, tmp_path)
    assert (scen_dir / "inventory.json").exists()


@pytest.mark.parametrize("name", SCENARIO_NAMES)
def test_manifest_has_required_keys(tmp_path, name):
    scen_dir = generate_scenario(name, tmp_path)
    manifest = json.loads((scen_dir / "manifest.json").read_text())
    for mut in manifest["mutations"]:
        for key in (
            "table", "affected_keys", "before", "after",
            "intended_defect", "expected_primary_check", "downstream_symptoms",
        ):
            assert key in mut, f"Scenario {name}: mutation missing key '{key}'"


@pytest.mark.parametrize("name", SCENARIO_NAMES)
def test_scenario_is_independent_of_baseline(tmp_path, name):
    """Scenarios must not mutate the baseline DataFrames (fresh copy per scenario)."""
    baseline = _build_baseline()
    generate_scenario(name, tmp_path)
    after = _build_baseline()
    for table in ("branch_reference", "customers", "accounts", "daily_balances"):
        pd.testing.assert_frame_equal(
            baseline[table], after[table],
            check_like=False,
        )


# ── Individual scenario defect properties ────────────────────────────────────


def test_dup_branch_id_conflict_name_has_duplicate(tmp_path):
    scen_dir = generate_scenario("dup_branch_id_conflict_name", tmp_path)
    br = pd.read_parquet(scen_dir / "branch_reference.parquet")
    dup_count = int(len(br) - br["branch_id"].nunique())
    assert dup_count > 0, "Expected at least one duplicate branch_id"


def test_dup_branch_id_inflates_metric(tmp_path):
    """
    An unguarded join to the duplicate branch_reference doubles the
    customer-branch total for the affected branch → inflated average.
    """
    from lumen_dq.db import open_connection

    scen_dir = generate_scenario("dup_branch_id_conflict_name", tmp_path)
    baseline = _build_baseline()

    # Build baseline connection (in-memory, no disk)
    import duckdb
    def make_conn(tbls):
        conn = duckdb.connect(":memory:")
        for n, df in tbls.items():
            conn.register(n, df)
        return conn

    scen_tables = {
        n: pd.read_parquet(scen_dir / f"{n}.parquet")
        for n in ("branch_reference", "accounts", "daily_balances", "customers")
    }

    SQL = """
    SELECT AVG(cust_total) AS avg
    FROM (
        SELECT a.customer_id, db.balance_date,
               SUM(db.closing_balance_cents) AS cust_total
        FROM daily_balances db
        JOIN accounts a ON db.account_id = a.account_id
        JOIN branch_reference br ON a.branch_id = br.branch_id
        GROUP BY a.customer_id, db.balance_date
    ) t
    """

    correct = make_conn(baseline).execute(SQL).fetchone()[0]
    inflated = make_conn(scen_tables).execute(SQL).fetchone()[0]
    assert inflated > correct, (
        f"Expected inflated > correct; correct={correct:.0f}, inflated={inflated:.0f}"
    )


def test_orphan_account_branch_has_orphan(tmp_path):
    scen_dir = generate_scenario("orphan_account_branch", tmp_path)
    accts = pd.read_parquet(scen_dir / "accounts.parquet")
    br = pd.read_parquet(scen_dir / "branch_reference.parquet")
    orphans = ~accts["branch_id"].isin(br["branch_id"])
    assert orphans.sum() >= 1


def test_orphan_account_customer_has_orphan(tmp_path):
    scen_dir = generate_scenario("orphan_account_customer", tmp_path)
    accts = pd.read_parquet(scen_dir / "accounts.parquet")
    custs = pd.read_parquet(scen_dir / "customers.parquet")
    orphans = ~accts["customer_id"].isin(custs["customer_id"])
    assert orphans.sum() >= 1


def test_orphan_balance_account_has_orphan(tmp_path):
    scen_dir = generate_scenario("orphan_balance_account", tmp_path)
    bal = pd.read_parquet(scen_dir / "daily_balances.parquet")
    accts = pd.read_parquet(scen_dir / "accounts.parquet")
    orphans = ~bal["account_id"].isin(accts["account_id"])
    assert orphans.sum() >= 1


def test_dup_account_id_has_duplicate_grain(tmp_path):
    scen_dir = generate_scenario("dup_account_id", tmp_path)
    accts = pd.read_parquet(scen_dir / "accounts.parquet")
    dup = accts.duplicated("account_id").sum()
    assert dup >= 1


def test_dup_balance_grain_has_duplicate(tmp_path):
    scen_dir = generate_scenario("dup_balance_grain", tmp_path)
    bal = pd.read_parquet(scen_dir / "daily_balances.parquet")
    dup = bal.duplicated(["account_id", "balance_date"]).sum()
    assert dup >= 1


def test_null_required_field_has_null_branch_id(tmp_path):
    scen_dir = generate_scenario("null_required_field", tmp_path)
    accts = pd.read_parquet(scen_dir / "accounts.parquet")
    assert accts["branch_id"].isna().sum() >= 1


def test_invalid_product_type_has_bad_value(tmp_path):
    scen_dir = generate_scenario("invalid_product_type", tmp_path)
    accts = pd.read_parquet(scen_dir / "accounts.parquet")
    from lumen_dq.generate import PRODUCT_TYPES
    bad = ~accts["product_type"].isin(PRODUCT_TYPES)
    assert bad.sum() >= 1


def test_malformed_raw_field_writes_csv(tmp_path):
    scen_dir = generate_scenario("malformed_raw_field", tmp_path)
    csv_path = scen_dir / "accounts_raw.csv"
    assert csv_path.exists(), "Expected accounts_raw.csv"
    content = csv_path.read_text()
    assert "NOT-A-DATE" in content


def test_missing_accounts_column_drops_currency(tmp_path):
    scen_dir = generate_scenario("missing_accounts_column", tmp_path)
    accts = pd.read_parquet(scen_dir / "accounts.parquet")
    assert "currency" not in accts.columns


def test_stale_ingestion_timestamp(tmp_path):
    scen_dir = generate_scenario("stale_ingestion_timestamp", tmp_path)
    accts = pd.read_parquet(scen_dir / "accounts.parquet")
    ts = pd.to_datetime(accts["ingested_at"], utc=True)
    as_of = pd.Timestamp(DEFAULT_AS_OF_DATE, tz="UTC")
    lag_hours = (as_of - ts).dt.total_seconds() / 3600
    assert (lag_hours > 24).all(), "Expected all ingested_at to be stale (>24 h)"


def test_balance_spike_exceeds_anomaly_threshold(tmp_path):
    scen_dir = generate_scenario("balance_spike", tmp_path)
    bal = pd.read_parquet(scen_dir / "daily_balances.parquet")
    count = _count_balance_anomalies(bal, ANOMALY_FACTOR)
    assert count >= 1, f"Expected ≥1 anomaly, found {count}"


def test_missing_balance_date_has_gap(tmp_path):
    scen_dir = generate_scenario("missing_balance_date", tmp_path)
    bal = pd.read_parquet(scen_dir / "daily_balances.parquet")
    accts = pd.read_parquet(scen_dir / "accounts.parquet")
    counts = bal.groupby("account_id").size()
    short = counts[counts < N_DATES]
    assert len(short) >= 1, "Expected at least one account with missing balance date"


# ── All scenarios leave other tables intact ───────────────────────────────────


_TABLES_MUTATED_BY_SCENARIO = {
    "dup_branch_id_conflict_name": {"branch_reference"},
    "orphan_account_branch": {"accounts"},
    "orphan_account_customer": {"accounts"},
    "orphan_balance_account": {"daily_balances"},
    "dup_account_id": {"accounts"},
    "dup_balance_grain": {"daily_balances"},
    "null_required_field": {"accounts"},
    "invalid_product_type": {"accounts"},
    "malformed_raw_field": set(),  # accounts_raw is a side-file, main tables unchanged
    "missing_accounts_column": {"accounts"},
    "stale_ingestion_timestamp": {"accounts"},
    "balance_spike": {"daily_balances"},
    "missing_balance_date": {"daily_balances"},
}


@pytest.mark.parametrize("name", SCENARIO_NAMES)
def test_unmutated_tables_are_identical_to_baseline(tmp_path, name):
    """Tables not listed as mutated must be byte-identical to baseline."""
    scen_dir = generate_scenario(name, tmp_path)
    baseline = _build_baseline()
    mutated = _TABLES_MUTATED_BY_SCENARIO[name]

    for table in ("branch_reference", "customers", "accounts", "daily_balances"):
        if table in mutated:
            continue
        path = scen_dir / f"{table}.parquet"
        if not path.exists():
            continue
        scen_df = pd.read_parquet(path)
        pd.testing.assert_frame_equal(
            baseline[table].reset_index(drop=True),
            scen_df.reset_index(drop=True),
            check_like=False,
        )
