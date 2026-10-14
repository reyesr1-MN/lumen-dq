"""
Deterministic synthetic-data generator for Lumen Community Bank.

Given identical arguments (--seed, --as-of-date) the output parquet files
and their checksums are always identical.

Grains
------
branch_reference  one row per branch_id              (42 rows)
customers         one row per customer_id             (1 000 rows)
accounts          one row per account_id              (1 500 rows)
daily_balances    one row per (account_id,balance_date)(45 000 rows)

Column schema (minimum required columns)
----------------------------------------
branch_reference : branch_id, branch_name, state_code
customers        : customer_id, customer_name, created_date
accounts         : account_id, customer_id, branch_id, product_type,
                   account_status, currency, opened_date, closed_date,
                   ingested_at
daily_balances   : account_id, balance_date, closing_balance_cents

Money is integer cents.  Timestamps are UTC.  All identifiers are fictional.

CLI
---
  python -m lumen_dq.generate                        # baseline only
  python -m lumen_dq.generate --scenario <name>      # one named scenario
  python -m lumen_dq.generate --scenario all         # all 13 scenarios

Outputs
-------
  {output_dir}/baseline/          baseline parquet files + inventory.json
  {output_dir}/scenarios/<name>/  mutated parquet files + manifest.json
                                   + inventory.json
"""

from __future__ import annotations

import argparse
import io
import json
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

# ── Module constants ──────────────────────────────────────────────────────────

SEED: int = 42
DEFAULT_AS_OF_DATE: date = date(2024, 1, 30)

N_CUSTOMERS: int = 1_000
N_ACCOUNTS: int = 1_500
N_BRANCHES: int = 42
N_DATES: int = 30

PRODUCT_TYPES: list[str] = ["CHECKING", "SAVINGS", "CD", "MONEY_MARKET"]
PRODUCT_WEIGHTS: list[float] = [0.45, 0.35, 0.12, 0.08]

ACCOUNT_STATUSES: list[str] = ["ACTIVE", "FROZEN", "CLOSED"]
STATUS_WEIGHTS: list[float] = [0.82, 0.08, 0.10]

# Maximum intra-day balance movement allowed (300 % = factor of 4 from prev day)
ANOMALY_FACTOR: float = 4.0

# ── Fictional name tables (all identifiers are fictional) ─────────────────────

_FIRST = [
    "Alexis", "Blair", "Cameron", "Dakota", "Elliott", "Finley", "Greer",
    "Harper", "Indigo", "Jordan", "Kendall", "Logan", "Morgan", "Nolan",
    "Oakley", "Parker", "Quinn", "Reese", "Sage", "Taylor", "Undine",
    "Vance", "Waverly", "Xander", "Yael", "Zephyr", "Avery", "Brynn",
    "Casey", "Drew",
]

_LAST = [
    "Alderton", "Bramwell", "Crestwood", "Dunmore", "Emberly", "Foxworth",
    "Glenmore", "Hartwell", "Ivywood", "Jarvis", "Kincaid", "Larkspur",
    "Merrifield", "Nightingale", "Ormsby", "Pendleton", "Quincey",
    "Ravensworth", "Stonehaven", "Thistlewood", "Upshaw", "Vanthorpe",
    "Wycliffe", "Xerxes", "Yarborough", "Zelwick", "Ashford", "Beckworth",
    "Collingwood", "Davenport", "Evershaw", "Fairmont", "Greystone",
    "Holloway", "Islington", "Juniper", "Kirkwood", "Lockwood",
    "Moorefield", "Northcote", "Overbury", "Prescott", "Rainsworth",
    "Selbourne", "Thorngate", "Underhill", "Varley", "Whitmore",
    "Yardley", "Zuber",
]

# Exactly 42 fictional branch cities (fictional state: Lumenshire, code LM)
_BRANCH_CITIES: list[str] = [
    "Millbrook", "Westport", "Riverdale", "Cedarville", "Oakton",
    "Pinehurst", "Maplewood", "Fairview", "Stonegate", "Lakeshore",
    "Elmwood", "Cliffside", "Harborview", "Sycamore", "Hillcrest",
    "Greenfield", "Ashwood", "Springdale", "Brookside", "Edgewood",
    "Northgate", "Southfield", "Westwood", "Eastbrook", "Clearwater",
    "Redwood", "Birchwood", "Foxridge", "Sunnyside", "Willowbrook",
    "Glendale", "Ridgeview", "Parkview", "Lakewood", "Hillside",
    "Meadowview", "Forestville", "Copperfield", "Rivermont", "Stonehaven",
    "Pinecrest", "Bayview",
]
assert len(_BRANCH_CITIES) == N_BRANCHES

_BRANCH_SUFFIXES: list[str] = [
    "Main Branch", "Branch", "Branch", "Branch", "Express Branch",
]


# ── Private generators ────────────────────────────────────────────────────────


def _gen_branches() -> pd.DataFrame:
    """42 unique branch rows.  No RNG needed — fully deterministic."""
    rows = []
    for i, city in enumerate(_BRANCH_CITIES, start=1):
        suffix = _BRANCH_SUFFIXES[i % len(_BRANCH_SUFFIXES)]
        rows.append(
            {
                "branch_id": i,
                "branch_name": f"Lumen {city} {suffix}",
                "city": city,
                "state_code": "LM",
            }
        )
    df = pd.DataFrame(rows)
    assert df["branch_id"].nunique() == N_BRANCHES
    return df


def _gen_customers(rng: np.random.Generator, as_of_date: date) -> pd.DataFrame:
    """
    1 000 customers.
    created_date is at least 90 days before the first balance date so there
    is always room to open an account before the window starts.
    """
    balance_start = as_of_date - timedelta(days=N_DATES - 1)
    n = N_CUSTOMERS
    fi = rng.integers(0, len(_FIRST), n)
    li = rng.integers(0, len(_LAST), n)
    # 90 – 3 650 days before balance_start
    since_offsets = rng.integers(90, 3_650, n)

    rows = []
    for i in range(n):
        fname, lname = _FIRST[fi[i]], _LAST[li[i]]
        created = balance_start - timedelta(days=int(since_offsets[i]))
        rows.append(
            {
                "customer_id": f"CUST-{i + 1:04d}",
                "customer_name": f"{fname} {lname}",
                "created_date": str(created),
            }
        )
    df = pd.DataFrame(rows)
    assert df["customer_id"].nunique() == N_CUSTOMERS
    return df


def _gen_accounts(
    rng: np.random.Generator,
    customers: pd.DataFrame,
    branches: pd.DataFrame,
    as_of_date: date,
) -> pd.DataFrame:
    """
    1 500 accounts with two-phase construction:

    Phase 1 – 1 000 accounts (one per customer, round-robin branch assignment)
              Guarantees: every customer has ≥ 1 account; every branch has ≥ 1 account.
    Phase 2 – 500 extra accounts (random customer + branch)
              Creates customers with multiple accounts and cross-branch customers.

    Lifecycle invariants (all relative to the balance window):
    - opened_date < first balance date AND >= customer's created_date + 1 day
    - CLOSED accounts have closed_date = as_of_date + 1..90 days (closed after window)
    - ACTIVE / FROZEN accounts have closed_date = NULL
    - ingested_at = UTC midnight of as_of_date + 0..7 hours (from RNG)
    """
    balance_start = as_of_date - timedelta(days=N_DATES - 1)
    cust_ids = customers["customer_id"].tolist()
    branch_ids = branches["branch_id"].tolist()
    cust_created_map: dict[str, date] = {
        row.customer_id: date.fromisoformat(row.created_date)
        for row in customers.itertuples(index=False)
    }

    n1, n2 = N_CUSTOMERS, N_ACCOUNTS - N_CUSTOMERS

    # ── Phase 1 pre-generate ──────────────────────────────────────────────────
    p1_cust_perm = rng.permutation(N_CUSTOMERS)       # shuffle customer order
    p1_branch_perm = rng.permutation(N_BRANCHES)      # shuffle branch order
    p1_open_offsets = rng.integers(30, 180, n1)       # days after customer created_date
    p1_type_idx = rng.choice(len(PRODUCT_TYPES), n1, p=PRODUCT_WEIGHTS)
    p1_status_idx = rng.choice(len(ACCOUNT_STATUSES), n1, p=STATUS_WEIGHTS)
    p1_closed_offsets = rng.integers(1, 91, n1)       # days after as_of_date (CLOSED only)
    p1_ingest_hours = rng.integers(0, 8, n1)

    # ── Phase 2 pre-generate ──────────────────────────────────────────────────
    p2_cust_idx = rng.integers(0, N_CUSTOMERS, n2)
    p2_branch_idx = rng.integers(0, N_BRANCHES, n2)
    p2_open_offsets = rng.integers(30, 180, n2)
    p2_type_idx = rng.choice(len(PRODUCT_TYPES), n2, p=PRODUCT_WEIGHTS)
    p2_status_idx = rng.choice(len(ACCOUNT_STATUSES), n2, p=STATUS_WEIGHTS)
    p2_closed_offsets = rng.integers(1, 91, n2)
    p2_ingest_hours = rng.integers(0, 8, n2)

    rows = []

    def _make_row(acct_num, cid, bid, open_off, type_i, status_i, close_off, ingest_h):
        cc = cust_created_map[cid]
        opened = cc + timedelta(days=int(open_off))
        # Clamp: opened must be strictly before balance_start
        if opened >= balance_start:
            opened = balance_start - timedelta(days=1)

        status_val = ACCOUNT_STATUSES[status_i]
        closed_date = (
            str(as_of_date + timedelta(days=int(close_off)))
            if status_val == "CLOSED"
            else None
        )
        ingested_at = pd.Timestamp(as_of_date, tz="UTC") + pd.Timedelta(
            hours=int(ingest_h)
        )
        return {
            "account_id": f"ACCT-{acct_num:04d}",
            # BUG: customer_id intentionally omitted to break the contract.
            # This simulates an upstream producer dropping a required column.
            "branch_id": int(bid),
            "product_type": PRODUCT_TYPES[type_i],
            "account_status": status_val,
            "currency": "USD",
            "opened_date": str(opened),
            "closed_date": closed_date,
            "ingested_at": ingested_at,
        }

    for i in range(n1):
        rows.append(
            _make_row(
                i + 1,
                cust_ids[p1_cust_perm[i]],
                branch_ids[p1_branch_perm[i % N_BRANCHES]],
                p1_open_offsets[i],
                p1_type_idx[i],
                p1_status_idx[i],
                p1_closed_offsets[i],
                p1_ingest_hours[i],
            )
        )
    for j in range(n2):
        rows.append(
            _make_row(
                N_CUSTOMERS + j + 1,
                cust_ids[p2_cust_idx[j]],
                branch_ids[p2_branch_idx[j]],
                p2_open_offsets[j],
                p2_type_idx[j],
                p2_status_idx[j],
                p2_closed_offsets[j],
                p2_ingest_hours[j],
            )
        )

    df = pd.DataFrame(rows)
    assert df["account_id"].nunique() == N_ACCOUNTS
    return df


def _gen_daily_balances(
    rng: np.random.Generator,
    accounts: pd.DataFrame,
    as_of_date: date,
) -> pd.DataFrame:
    """
    Exactly one row per (account_id, balance_date).

    Balances follow a multiplicative random walk:
      B[0] ~ LogNormal(log(500_000), 0.6)          (start ≈ $5 000)
      B[d] = B[d-1] * exp(N(0, 0.08))              (daily ±≈ 8 %)

    The log-normal daily increments ensure no single-day move exceeds ~30 %
    at 3-sigma, well below the 300 % anomaly threshold.
    """
    balance_start = as_of_date - timedelta(days=N_DATES - 1)
    dates = [str(balance_start + timedelta(days=d)) for d in range(N_DATES)]
    n = len(accounts)

    starts = rng.lognormal(np.log(500_000), 0.6, n)
    log_increments = rng.normal(0.0, 0.08, (n, N_DATES - 1))

    bal = np.empty((n, N_DATES), dtype=np.float64)
    bal[:, 0] = starts
    for d in range(1, N_DATES):
        bal[:, d] = bal[:, d - 1] * np.exp(log_increments[:, d - 1])
    bal = np.maximum(np.round(bal).astype(np.int64), 1)

    acct_ids = accounts["account_id"].to_numpy()
    return pd.DataFrame(
        {
            "account_id": np.repeat(acct_ids, N_DATES),
            "balance_date": np.tile(dates, n),
            "closing_balance_cents": bal.flatten(),
        }
    )


def _build_baseline(
    seed: int = SEED,
    as_of_date: date = DEFAULT_AS_OF_DATE,
) -> dict[str, pd.DataFrame]:
    """Return the four clean baseline DataFrames (no side effects)."""
    rng = np.random.default_rng(seed)
    branches = _gen_branches()
    customers = _gen_customers(rng, as_of_date)
    accounts = _gen_accounts(rng, customers, branches, as_of_date)
    balances = _gen_daily_balances(rng, accounts, as_of_date)
    return {
        "branch_reference": branches,
        "customers": customers,
        "accounts": accounts,
        "daily_balances": balances,
    }


# ── Write helpers ─────────────────────────────────────────────────────────────


def _write_tables(
    tables: dict[str, Any],
    output_dir: Path,
    *,
    csv_tables: frozenset[str] = frozenset(),
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    for name, df in tables.items():
        if df is None:
            continue
        if name in csv_tables:
            if isinstance(df, str):
                (output_dir / f"{name}.csv").write_text(df, encoding="utf-8")
            else:
                df.to_csv(output_dir / f"{name}.csv", index=False)
        else:
            df.to_parquet(output_dir / f"{name}.parquet", index=False)


def _print_counts(tables: dict[str, pd.DataFrame], output_dir: Path) -> None:
    print(f"Data written to {output_dir}/")
    for name, df in tables.items():
        if isinstance(df, pd.DataFrame):
            print(f"  {name:<20} {len(df):>8,} rows")


# ── Public API — backward-compatible ─────────────────────────────────────────


def generate_all(output_dir: Path | str) -> None:
    """Write all four baseline tables as Parquet to output_dir (flat)."""
    tables = _build_baseline(SEED, DEFAULT_AS_OF_DATE)
    output_dir = Path(output_dir)
    _write_tables(tables, output_dir)
    _print_counts(tables, output_dir)


def generate_buggy_branch_reference(baseline_dir: Path | str) -> pd.DataFrame:
    """
    Return branch_reference with 5 duplicate branch_id rows (for legacy tests).
    Duplicated branch_ids: 1, 5, 12, 23, 37.
    """
    br = pd.read_parquet(Path(baseline_dir) / "branch_reference.parquet")
    dupes = br[br["branch_id"].isin([1, 5, 12, 23, 37])].copy()
    dupes["branch_name"] = dupes["branch_name"] + " [duplicate]"
    return pd.concat([br, dupes], ignore_index=True)


def generate_invalid_daily_balances(baseline_dir: Path | str) -> pd.DataFrame:
    """Return daily_balances with 10 duplicate (account_id, balance_date) pairs."""
    db = pd.read_parquet(Path(baseline_dir) / "daily_balances.parquet")
    dupes = db.sample(10, random_state=SEED).copy()
    dupes["closing_balance_cents"] = dupes["closing_balance_cents"] + 1
    return pd.concat([db, dupes], ignore_index=True)


def generate_invalid_accounts_orphan_fk(baseline_dir: Path | str) -> pd.DataFrame:
    """
    Return accounts with 5 extra rows with a non-existent customer_id.
    Orphan rows use fresh ACCT-ORPH-* ids so grain is not violated.
    """
    accts = pd.read_parquet(Path(baseline_dir) / "accounts.parquet")
    orphans = accts.sample(5, random_state=SEED).copy()
    orphans["account_id"] = [f"ACCT-ORPH-{i:04d}" for i in range(5)]
    orphans["customer_id"] = "CUST-XXXX"
    return pd.concat([accts, orphans], ignore_index=True)


# ── New public API ────────────────────────────────────────────────────────────


def generate_dataset(
    output_dir: Path | str = "data",
    *,
    seed: int = SEED,
    as_of_date: date = DEFAULT_AS_OF_DATE,
) -> Path:
    """
    Generate the baseline dataset and write to {output_dir}/baseline/.
    Also writes inventory.json.
    Returns the baseline directory path.
    """
    from lumen_dq.inventory import compute_inventory, write_inventory  # lazy import

    base_dir = Path(output_dir) / "baseline"
    tables = _build_baseline(seed, as_of_date)
    _write_tables(tables, base_dir)
    inv = compute_inventory(tables, seed=seed, as_of_date=as_of_date)
    write_inventory(inv, base_dir / "inventory.json")
    _print_counts(tables, base_dir)
    return base_dir


def generate_scenario(
    name: str,
    output_dir: Path | str = "data",
    *,
    seed: int = SEED,
    as_of_date: date = DEFAULT_AS_OF_DATE,
) -> Path:
    """
    Generate a defect scenario on a fresh copy of the baseline.
    Writes to {output_dir}/scenarios/{name}/.
    Returns the scenario directory path.
    """
    from lumen_dq.inventory import compute_inventory, write_inventory

    if name not in _SCENARIO_BUILDERS:
        raise ValueError(f"Unknown scenario {name!r}. Valid: {list(_SCENARIO_BUILDERS)}")

    baseline_tables = _build_baseline(seed, as_of_date)
    mutated, manifest, csv_tables = _SCENARIO_BUILDERS[name](
        baseline_tables, seed, as_of_date
    )

    scen_dir = Path(output_dir) / "scenarios" / name
    scen_dir.mkdir(parents=True, exist_ok=True)

    # Merge: mutated tables override baseline where present
    all_tables = {**baseline_tables, **mutated}
    _write_tables(all_tables, scen_dir, csv_tables=frozenset(csv_tables))

    (scen_dir / "manifest.json").write_text(
        json.dumps(manifest, indent=2, default=str), encoding="utf-8"
    )

    inv = compute_inventory(all_tables, seed=seed, as_of_date=as_of_date, scenario=name)
    write_inventory(inv, scen_dir / "inventory.json")

    print(f"Scenario {name!r} written to {scen_dir}/")
    return scen_dir


# ── Scenario builders ─────────────────────────────────────────────────────────
#
# Signature: (tables, seed, as_of_date) → (mutated_tables, manifest, csv_tables)
#   mutated_tables : dict of table_name → DataFrame (only the changed tables)
#   manifest       : dict (serialisable to JSON)
#   csv_tables     : list of table names to write as CSV instead of Parquet


def _manifest(
    scenario: str,
    mutations: list[dict],
    description: str = "",
    format_note: str = "",
) -> dict:
    m: dict = {"scenario": scenario, "description": description}
    if format_note:
        m["format_note"] = format_note
    m["mutations"] = mutations
    return m


def _mutation(
    table: str,
    affected_keys: dict,
    before: dict | list,
    after: dict | list,
    intended_defect: str,
    expected_primary_check: str,
    downstream_symptoms: list[str],
) -> dict:
    return {
        "table": table,
        "affected_keys": affected_keys,
        "before": before,
        "after": after,
        "intended_defect": intended_defect,
        "expected_primary_check": expected_primary_check,
        "downstream_symptoms": downstream_symptoms,
    }


# ── 1. Duplicate branch_id with conflicting branch_name ───────────────────────

def _scenario_dup_branch_id_conflict_name(tables, seed, as_of_date):
    """
    Pick the branch with the highest total closing_balance_cents.
    Add one duplicate row with the same branch_id but a conflicting branch_name.

    Quantifies fan-out: joining accounts + daily_balances to this branch_reference
    doubles every row for the affected branch, inflating the customer-balance
    metric (AVG of per-customer sums) by 2×.

    Note: a plain AVG(closing_balance_cents) per account is unchanged because
    both numerator (SUM) and denominator (COUNT) double.  The customer-level
    metric is inflated because the row-fan-out doubles the per-customer SUM
    while the GROUP BY still collapses to the same customer set.
    """
    br = tables["branch_reference"].copy()
    accts = tables["accounts"]
    bal = tables["daily_balances"]

    # Find branch with highest total balance
    branch_totals = (
        bal.merge(accts[["account_id", "branch_id"]], on="account_id")
        .groupby("branch_id")["closing_balance_cents"]
        .sum()
    )
    target_bid = int(branch_totals.idxmax())
    target_row = br[br["branch_id"] == target_bid].iloc[0].to_dict()

    # Compute customer-balance metric for this branch
    branch_accts = accts[accts["branch_id"] == target_bid]["account_id"]
    branch_bal = bal[bal["account_id"].isin(branch_accts)]
    cust_totals = (
        branch_bal.merge(accts[["account_id", "customer_id"]], on="account_id")
        .groupby(["customer_id", "balance_date"])["closing_balance_cents"]
        .sum()
    )
    correct_avg = float(cust_totals.mean())

    # Build duplicate row with conflicting name
    dup_row = {**target_row, "branch_name": target_row["branch_name"] + " [CONFLICT]"}
    mutated_br = pd.concat([br, pd.DataFrame([dup_row])], ignore_index=True)

    n_accts_affected = len(branch_accts)
    n_rows_affected = len(branch_bal)

    mut = _mutation(
        table="branch_reference",
        affected_keys={"branch_id": target_bid},
        before={"branch_id": target_bid, "branch_name": target_row["branch_name"]},
        after=[
            {"branch_id": target_bid, "branch_name": target_row["branch_name"]},
            {"branch_id": target_bid, "branch_name": dup_row["branch_name"]},
        ],
        intended_defect=(
            f"Duplicate branch_id {target_bid} with conflicting branch_name. "
            f"An unguarded JOIN to branch_reference fans out all "
            f"{n_rows_affected:,} daily_balance rows for this branch by 2×."
        ),
        expected_primary_check="check_grain_uniqueness(branch_reference, [branch_id])",
        downstream_symptoms=[
            f"JOIN fan-out doubles {n_rows_affected:,} daily_balances rows "
            f"({n_accts_affected} accounts, {N_DATES} dates).",
            f"customer-branch SUM is doubled → avg_customer_balance_cents "
            f"inflated from {correct_avg / 100:,.2f} to "
            f"{correct_avg * 2 / 100:,.2f} (2× inflation) for branch {target_bid}.",
            "Plain AVG(closing_balance_cents) per account-date is unchanged "
            "(SUM and COUNT both double, ratio unchanged).",
            "Total branch balance (SUM) is overstated by 100 %.",
        ],
    )
    manifest = _manifest(
        "dup_branch_id_conflict_name",
        [mut],
        description=(
            f"Branch {target_bid} (highest total balance, "
            f"{branch_totals[target_bid]:,} cents) has a duplicate row "
            "with a conflicting branch_name."
        ),
    )
    return {"branch_reference": mutated_br}, manifest, []


# ── 2. Account referencing a nonexistent branch ───────────────────────────────

def _scenario_orphan_account_branch(tables, seed, as_of_date):
    accts = tables["accounts"].copy()
    target_idx = int(np.random.default_rng(seed + 2).integers(0, len(accts)))
    target_id = accts.at[target_idx, "account_id"]
    before_bid = int(accts.at[target_idx, "branch_id"])
    accts.at[target_idx, "branch_id"] = 9999  # does not exist

    mut = _mutation(
        table="accounts",
        affected_keys={"account_id": target_id},
        before={"branch_id": before_bid},
        after={"branch_id": 9999},
        intended_defect="account references a branch_id that does not exist in branch_reference",
        expected_primary_check="check_foreign_key(accounts, branch_id → branch_reference.branch_id)",
        downstream_symptoms=[
            "Account is excluded from branch-level reports (no matching branch row).",
            "Branch summary loses the account's balance contribution.",
        ],
    )
    return {"accounts": accts}, _manifest("orphan_account_branch", [mut]), []


# ── 3. Account referencing a nonexistent customer ─────────────────────────────

def _scenario_orphan_account_customer(tables, seed, as_of_date):
    accts = tables["accounts"].copy()
    target_idx = int(np.random.default_rng(seed + 3).integers(0, len(accts)))
    target_id = accts.at[target_idx, "account_id"]
    before_cid = accts.at[target_idx, "customer_id"]
    accts.at[target_idx, "customer_id"] = "CUST-XXXX"

    mut = _mutation(
        table="accounts",
        affected_keys={"account_id": target_id},
        before={"customer_id": before_cid},
        after={"customer_id": "CUST-XXXX"},
        intended_defect="account references a customer_id that does not exist in customers",
        expected_primary_check="check_foreign_key(accounts, customer_id → customers.customer_id)",
        downstream_symptoms=[
            "Customer-level metric excludes this account's balance.",
            "customer-branch totals understate the true balance.",
        ],
    )
    return {"accounts": accts}, _manifest("orphan_account_customer", [mut]), []


# ── 4. Balance referencing a nonexistent account ──────────────────────────────

def _scenario_orphan_balance_account(tables, seed, as_of_date):
    bal = tables["daily_balances"].copy()
    rng2 = np.random.default_rng(seed + 4)
    target_idx = int(rng2.integers(0, len(bal)))
    before_aid = bal.at[target_idx, "account_id"]
    before_date = bal.at[target_idx, "balance_date"]
    bal.at[target_idx, "account_id"] = "ACCT-XXXX"

    mut = _mutation(
        table="daily_balances",
        affected_keys={"account_id": before_aid, "balance_date": before_date},
        before={"account_id": before_aid},
        after={"account_id": "ACCT-XXXX"},
        intended_defect="balance row references an account_id not in accounts",
        expected_primary_check="check_foreign_key(daily_balances, account_id → accounts.account_id)",
        downstream_symptoms=[
            "Orphan balance row excluded from all joins → lost from SUM and AVG.",
            "Account appears to have a missing balance date in the coverage check.",
        ],
    )
    return {"daily_balances": bal}, _manifest("orphan_balance_account", [mut]), []


# ── 5. Duplicate account_id ────────────────────────────────────────────────────

def _scenario_dup_account_id(tables, seed, as_of_date):
    accts = tables["accounts"].copy()
    target_idx = int(np.random.default_rng(seed + 5).integers(0, len(accts)))
    target_id = accts.at[target_idx, "account_id"]
    dup = accts.iloc[[target_idx]].copy()
    dup.at[target_idx, "customer_id"] = "CUST-0001"  # different customer
    dup.at[target_idx, "branch_id"] = int(accts.at[target_idx, "branch_id"]) + 1

    mutated = pd.concat([accts, dup], ignore_index=True)

    mut = _mutation(
        table="accounts",
        affected_keys={"account_id": target_id},
        before=accts.loc[target_idx, ["account_id", "customer_id", "branch_id"]].to_dict(),
        after=[
            accts.loc[target_idx, ["account_id", "customer_id", "branch_id"]].to_dict(),
            dup.loc[target_idx, ["account_id", "customer_id", "branch_id"]].to_dict(),
        ],
        intended_defect="duplicate account_id with different customer_id / branch_id",
        expected_primary_check="check_grain_uniqueness(accounts, [account_id])",
        downstream_symptoms=[
            "JOIN to daily_balances fans out balance rows for this account.",
            "Customer is assigned two conflicting branch associations.",
        ],
    )
    return {"accounts": mutated}, _manifest("dup_account_id", [mut]), []


# ── 6. Duplicate (account_id, balance_date) ───────────────────────────────────

def _scenario_dup_balance_grain(tables, seed, as_of_date):
    bal = tables["daily_balances"].copy()
    rng2 = np.random.default_rng(seed + 6)
    target_idx = int(rng2.integers(0, len(bal)))
    dup = bal.iloc[[target_idx]].copy()
    dup.at[target_idx, "closing_balance_cents"] = int(bal.at[target_idx, "closing_balance_cents"]) + 100
    mutated = pd.concat([bal, dup], ignore_index=True)

    mut = _mutation(
        table="daily_balances",
        affected_keys={
            "account_id": bal.at[target_idx, "account_id"],
            "balance_date": bal.at[target_idx, "balance_date"],
        },
        before={"closing_balance_cents": int(bal.at[target_idx, "closing_balance_cents"])},
        after=[
            {"closing_balance_cents": int(bal.at[target_idx, "closing_balance_cents"])},
            {"closing_balance_cents": int(bal.at[target_idx, "closing_balance_cents"]) + 100},
        ],
        intended_defect="two rows for the same (account_id, balance_date) with different balances",
        expected_primary_check="check_grain_uniqueness(daily_balances, [account_id, balance_date])",
        downstream_symptoms=[
            "SUM(closing_balance_cents) overstated by 100 cents for this account-date.",
            "AVG includes both rows → inflated for that account.",
        ],
    )
    return {"daily_balances": mutated}, _manifest("dup_balance_grain", [mut]), []


# ── 7. Null required account field ────────────────────────────────────────────

def _scenario_null_required_field(tables, seed, as_of_date):
    accts = tables["accounts"].copy()
    target_idx = int(np.random.default_rng(seed + 7).integers(0, len(accts)))
    target_id = accts.at[target_idx, "account_id"]
    before_bid = int(accts.at[target_idx, "branch_id"])
    accts.at[target_idx, "branch_id"] = pd.NA  # NULL required field

    mut = _mutation(
        table="accounts",
        affected_keys={"account_id": target_id},
        before={"branch_id": before_bid},
        after={"branch_id": None},
        intended_defect="required column branch_id is NULL",
        expected_primary_check="check_not_null(accounts, branch_id)",
        downstream_symptoms=[
            "Account cannot be joined to branch_reference → excluded from branch reports.",
            "FK check on branch_id silently skips NULL values (NULL NOT IN … is NULL).",
        ],
    )
    return {"accounts": accts}, _manifest("null_required_field", [mut]), []


# ── 8. Invalid product_type ────────────────────────────────────────────────────

def _scenario_invalid_product_type(tables, seed, as_of_date):
    accts = tables["accounts"].copy()
    target_idx = int(np.random.default_rng(seed + 8).integers(0, len(accts)))
    target_id = accts.at[target_idx, "account_id"]
    before_pt = accts.at[target_idx, "product_type"]
    accts.at[target_idx, "product_type"] = "PREMIUM_CHECKING"  # not in enum

    mut = _mutation(
        table="accounts",
        affected_keys={"account_id": target_id},
        before={"product_type": before_pt},
        after={"product_type": "PREMIUM_CHECKING"},
        intended_defect="product_type value outside the declared enum",
        expected_primary_check=(
            "check_allowed_values(accounts, product_type, "
            "[CHECKING, SAVINGS, CD, MONEY_MARKET])"
        ),
        downstream_symptoms=[
            "Product-level reports have an unrecognised category.",
            "Pivot or CASE expressions silently drop the row into an 'other' bucket.",
        ],
    )
    return {"accounts": accts}, _manifest("invalid_product_type", [mut]), []


# ── 9. Malformed integer or date in raw serialised input ─────────────────────

def _scenario_malformed_raw_field(tables, seed, as_of_date):
    """
    Writes accounts as accounts_raw.csv (the 'raw input' format) with one
    opened_date set to 'NOT-A-DATE'.  The accounts.parquet is normal.

    Any ingest pipeline that reads accounts_raw.csv must handle this error.
    """
    accts = tables["accounts"]
    target_idx = 0  # deterministic: first row
    target_id = accts.at[target_idx, "account_id"]
    before_od = accts.at[target_idx, "opened_date"]

    # Build CSV string with malformed date
    buf = io.StringIO()
    accts.to_csv(buf, index=False)
    lines = buf.getvalue().split("\n")
    header_cols = lines[0].split(",")
    date_col_idx = header_cols.index("opened_date")
    first_data = lines[1].split(",")
    first_data[date_col_idx] = "NOT-A-DATE"
    lines[1] = ",".join(first_data)
    malformed_csv = "\n".join(lines)

    mut = _mutation(
        table="accounts_raw.csv",
        affected_keys={"account_id": target_id},
        before={"opened_date": before_od},
        after={"opened_date": "NOT-A-DATE"},
        intended_defect="malformed ISO-8601 date in raw CSV serialisation of opened_date",
        expected_primary_check="date_parse_validation(accounts.opened_date)",
        downstream_symptoms=[
            "Ingest pipeline raises ValueError / parse error on row 1.",
            "If coerced to NULL: lifecycle consistency check fails "
            "(opened_date IS NULL but account has balances).",
            "Downstream date arithmetic (account age, days-open) produces NaT.",
        ],
    )
    manifest = _manifest(
        "malformed_raw_field",
        [mut],
        description="accounts_raw.csv contains 'NOT-A-DATE' in the opened_date column.",
        format_note="Defect is in accounts_raw.csv. accounts.parquet is unchanged.",
    )
    # Return the CSV string as "accounts_raw" table name (written as CSV)
    return (
        {"accounts_raw": malformed_csv},
        manifest,
        ["accounts_raw"],  # write as CSV
    )


# ── 10. Missing required accounts column ─────────────────────────────────────

def _scenario_missing_accounts_column(tables, seed, as_of_date):
    accts = tables["accounts"].drop(columns=["currency"])

    mut = _mutation(
        table="accounts",
        affected_keys={},
        before={"columns": list(tables["accounts"].columns)},
        after={"columns": list(accts.columns)},
        intended_defect="required column 'currency' is absent from the accounts table",
        expected_primary_check="schema_check: column currency must exist in accounts",
        downstream_symptoms=[
            "Any downstream step that reads currency fails with KeyError / ColumnNotFoundError.",
            "Multi-currency logic defaults silently to an assumed currency.",
        ],
    )
    return {"accounts": accts}, _manifest("missing_accounts_column", [mut]), []


# ── 11. Stale accounts ingestion timestamp ────────────────────────────────────

def _scenario_stale_ingestion_timestamp(tables, seed, as_of_date):
    accts = tables["accounts"].copy()
    stale_ts = pd.Timestamp(as_of_date, tz="UTC") - pd.Timedelta(hours=48)
    accts["ingested_at"] = stale_ts

    mut = _mutation(
        table="accounts",
        affected_keys={"all_rows": True},
        before={"ingested_at": "as_of_date + 0..7 hours (UTC)"},
        after={"ingested_at": str(stale_ts)},
        intended_defect="ingested_at is 48 h before as_of_date, exceeding the 24 h freshness SLA",
        expected_primary_check=(
            "check_ingestion_freshness(accounts, ingested_at, "
            "as_of_date, max_lag_hours=24)"
        ),
        downstream_symptoms=[
            "Accounts file may not reflect same-day account openings or closures.",
            "Reconciliation between balance file and accounts file may show stale status.",
        ],
    )
    return {"accounts": accts}, _manifest("stale_ingestion_timestamp", [mut]), []


# ── 12. Daily balance movement exceeding 300 % ────────────────────────────────

def _scenario_balance_spike(tables, seed, as_of_date):
    """
    For one account, set balance on day 15 to 5× balance on day 14.
    The movement is (5-1)/1 = 400 % which exceeds the 300 % anomaly threshold.
    """
    bal = tables["daily_balances"].copy()
    balance_start = as_of_date - timedelta(days=N_DATES - 1)
    day14 = str(balance_start + timedelta(days=13))  # 14th balance date (0-indexed)
    day15 = str(balance_start + timedelta(days=14))

    target_acct = bal["account_id"].iloc[0]
    mask14 = (bal["account_id"] == target_acct) & (bal["balance_date"] == day14)
    mask15 = (bal["account_id"] == target_acct) & (bal["balance_date"] == day15)

    b14 = int(bal.loc[mask14, "closing_balance_cents"].values[0])
    b15_before = int(bal.loc[mask15, "closing_balance_cents"].values[0])
    b15_after = b14 * 5  # 400% increase = 5× previous

    bal.loc[mask15, "closing_balance_cents"] = b15_after
    movement_pct = (b15_after - b14) / b14 * 100

    mut = _mutation(
        table="daily_balances",
        affected_keys={"account_id": target_acct, "balance_date": day15},
        before={"closing_balance_cents": b15_before},
        after={"closing_balance_cents": b15_after},
        intended_defect=(
            f"Balance on {day15} is {movement_pct:.0f}% higher than {day14} "
            f"({b14:,} → {b15_after:,} cents), exceeding the 300% anomaly threshold."
        ),
        expected_primary_check=(
            "check_balance_movement(daily_balances, "
            "max_day_over_day_factor=4.0)"
        ),
        downstream_symptoms=[
            "Daily average inflated on this date for the affected branch and bank-wide.",
            "Time-series anomaly detection flags the account.",
            "Spike may indicate a data loading error (duplicate credit, wrong scale).",
        ],
    )
    return {"daily_balances": bal}, _manifest("balance_spike", [mut]), []


# ── 13. Missing balance date for an account ───────────────────────────────────

def _scenario_missing_balance_date(tables, seed, as_of_date):
    """Remove all balance rows for one account on one date."""
    bal = tables["daily_balances"]
    balance_start = as_of_date - timedelta(days=N_DATES - 1)
    missing_date = str(balance_start + timedelta(days=14))  # middle of window
    target_acct = bal["account_id"].iloc[5]  # fifth account

    before_count = len(bal)
    mutated = bal[
        ~((bal["account_id"] == target_acct) & (bal["balance_date"] == missing_date))
    ].copy()
    after_count = len(mutated)

    mut = _mutation(
        table="daily_balances",
        affected_keys={"account_id": target_acct, "balance_date": missing_date},
        before={"row_count": before_count},
        after={"row_count": after_count, "removed_rows": before_count - after_count},
        intended_defect=(
            f"Account {target_acct} has no balance row for {missing_date}; "
            "breaks the 'one row per account per date' grain invariant."
        ),
        expected_primary_check=(
            "check_balance_coverage(daily_balances, accounts, expected_dates)"
        ),
        downstream_symptoms=[
            "Account excluded from date-level averages on the missing date.",
            "Time-series completeness check fails for this account.",
            "Interpolation or carry-forward logic needed downstream.",
        ],
    )
    return {"daily_balances": mutated}, _manifest("missing_balance_date", [mut]), []


# ── Scenario registry ─────────────────────────────────────────────────────────

SCENARIO_NAMES: list[str] = [
    "dup_branch_id_conflict_name",
    "orphan_account_branch",
    "orphan_account_customer",
    "orphan_balance_account",
    "dup_account_id",
    "dup_balance_grain",
    "null_required_field",
    "invalid_product_type",
    "malformed_raw_field",
    "missing_accounts_column",
    "stale_ingestion_timestamp",
    "balance_spike",
    "missing_balance_date",
]

_SCENARIO_BUILDERS = {
    "dup_branch_id_conflict_name": _scenario_dup_branch_id_conflict_name,
    "orphan_account_branch": _scenario_orphan_account_branch,
    "orphan_account_customer": _scenario_orphan_account_customer,
    "orphan_balance_account": _scenario_orphan_balance_account,
    "dup_account_id": _scenario_dup_account_id,
    "dup_balance_grain": _scenario_dup_balance_grain,
    "null_required_field": _scenario_null_required_field,
    "invalid_product_type": _scenario_invalid_product_type,
    "malformed_raw_field": _scenario_malformed_raw_field,
    "missing_accounts_column": _scenario_missing_accounts_column,
    "stale_ingestion_timestamp": _scenario_stale_ingestion_timestamp,
    "balance_spike": _scenario_balance_spike,
    "missing_balance_date": _scenario_missing_balance_date,
}

assert set(SCENARIO_NAMES) == set(_SCENARIO_BUILDERS)


# ── CLI ───────────────────────────────────────────────────────────────────────


def _cli(argv=None):
    parser = argparse.ArgumentParser(
        description=(
            "Generate deterministic synthetic data for Lumen Community Bank.\n"
            "Identical --seed and --as-of-date always produce identical output."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=SEED,
        help=f"RNG seed (default: {SEED})",
    )
    parser.add_argument(
        "--as-of-date",
        default=str(DEFAULT_AS_OF_DATE),
        help=f"Last balance date, ISO-8601 (default: {DEFAULT_AS_OF_DATE})",
    )
    parser.add_argument(
        "--output-dir",
        default="data",
        help="Root output directory (default: data)",
    )
    parser.add_argument(
        "--scenario",
        default=None,
        help=(
            "Named scenario to generate, or 'all' for every scenario. "
            f"Valid names: {', '.join(SCENARIO_NAMES)}"
        ),
    )
    args = parser.parse_args(argv)

    as_of = date.fromisoformat(args.as_of_date)
    out = Path(args.output_dir)

    generate_dataset(out, seed=args.seed, as_of_date=as_of)

    if args.scenario == "all":
        for name in SCENARIO_NAMES:
            generate_scenario(name, out, seed=args.seed, as_of_date=as_of)
    elif args.scenario is not None:
        generate_scenario(args.scenario, out, seed=args.seed, as_of_date=as_of)


if __name__ == "__main__":
    _cli(sys.argv[1:])
