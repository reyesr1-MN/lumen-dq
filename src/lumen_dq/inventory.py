"""
Dataset inventory: row counts, date ranges, orphan counts, duplicate-key
counts, and content checksums.

The checksum is SHA-256 of the sorted CSV bytes of each DataFrame,
stable across pandas/pyarrow versions.

Usage
-----
    from lumen_dq.inventory import compute_inventory, write_inventory, verify_inventory
"""

from __future__ import annotations

import hashlib
import io
import json
from datetime import date
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


# ── Checksum ──────────────────────────────────────────────────────────────────


def dataframe_checksum(df: pd.DataFrame, sort_columns: list[str] | None = None) -> str:
    """
    Return the SHA-256 hex digest of the DataFrame serialised as sorted CSV bytes.

    Sorting by the PK columns before serialisation ensures the checksum is
    stable regardless of row insertion order.
    """
    if sort_columns:
        # Only sort by columns that actually exist
        existing = [c for c in sort_columns if c in df.columns]
        df = df.sort_values(existing).reset_index(drop=True)
    buf = io.StringIO()
    df.to_csv(buf, index=False)
    return hashlib.sha256(buf.getvalue().encode("utf-8")).hexdigest()


# ── Per-table analysis ────────────────────────────────────────────────────────


def _analyze_branch_reference(br: pd.DataFrame) -> dict[str, Any]:
    dup_pk = int(len(br) - br["branch_id"].nunique())
    return {
        "row_count": len(br),
        "pk_column": "branch_id",
        "unique_pk_count": int(br["branch_id"].nunique()),
        "duplicate_pk_count": dup_pk,
        "checksum_sha256": dataframe_checksum(br, ["branch_id"]),
    }


def _analyze_customers(cust: pd.DataFrame) -> dict[str, Any]:
    dup_pk = int(len(cust) - cust["customer_id"].nunique())
    created_dates = pd.to_datetime(cust["created_date"], errors="coerce")
    return {
        "row_count": len(cust),
        "pk_column": "customer_id",
        "unique_pk_count": int(cust["customer_id"].nunique()),
        "duplicate_pk_count": dup_pk,
        "created_date_min": str(created_dates.min().date()),
        "created_date_max": str(created_dates.max().date()),
        "checksum_sha256": dataframe_checksum(cust, ["customer_id"]),
    }


def _analyze_accounts(
    accts: pd.DataFrame,
    customers: pd.DataFrame,
    branches: pd.DataFrame,
) -> dict[str, Any]:
    dup_pk = int(len(accts) - accts["account_id"].nunique())

    # FK orphan counts
    valid_custs = set(customers["customer_id"])
    valid_branches = set(branches["branch_id"])
    orphan_cust = int((~accts["customer_id"].isin(valid_custs)).sum())
    branch_col = accts["branch_id"].dropna()
    orphan_branch = int((~branch_col.isin(valid_branches)).sum())

    # Null required fields
    required = ["account_id", "customer_id", "branch_id", "product_type",
                "account_status", "currency", "opened_date"]
    null_counts = {c: int(accts[c].isna().sum()) for c in required if c in accts.columns}
    missing_required_cols = [c for c in required if c not in accts.columns]

    # Product type distribution
    pt_dist = (
        accts["product_type"].value_counts().to_dict()
        if "product_type" in accts.columns
        else {}
    )

    # Status distribution
    st_dist = (
        accts["account_status"].value_counts().to_dict()
        if "account_status" in accts.columns
        else {}
    )

    # Multi-branch customers
    if "customer_id" in accts.columns and "branch_id" in accts.columns:
        branches_per_cust = accts.groupby("customer_id")["branch_id"].nunique()
        multi_branch_customers = int((branches_per_cust > 1).sum())
    else:
        multi_branch_customers = 0

    return {
        "row_count": len(accts),
        "pk_column": "account_id",
        "unique_pk_count": int(accts["account_id"].nunique()),
        "duplicate_pk_count": dup_pk,
        "orphan_customer_fk_count": orphan_cust,
        "orphan_branch_fk_count": orphan_branch,
        "null_required_field_counts": null_counts,
        "missing_required_columns": missing_required_cols,
        "product_type_distribution": pt_dist,
        "account_status_distribution": st_dist,
        "customers_with_multi_branch_accounts": multi_branch_customers,
        "checksum_sha256": dataframe_checksum(accts, ["account_id"])
        if "account_id" in accts.columns
        else None,
    }


def _analyze_daily_balances(
    bal: pd.DataFrame,
    accounts: pd.DataFrame,
) -> dict[str, Any]:
    # Grain duplicates
    grain_dupes = int(
        bal.duplicated(subset=["account_id", "balance_date"]).sum()
    )

    # Orphan account FK
    valid_accts = set(accounts["account_id"])
    orphan_acct = int((~bal["account_id"].isin(valid_accts)).sum())

    # Date range
    dates = pd.to_datetime(bal["balance_date"], errors="coerce")
    date_min = str(dates.min().date())
    date_max = str(dates.max().date())
    n_unique_dates = int(dates.dt.date.nunique())

    # Balance stats
    b = bal["closing_balance_cents"].astype(np.int64)
    neg_count = int((b < 0).sum())

    # Anomaly check: day-over-day ratio > ANOMALY_FACTOR
    from lumen_dq.generate import ANOMALY_FACTOR  # avoid circular at module level
    anomaly_count = _count_balance_anomalies(bal, ANOMALY_FACTOR)

    # Coverage: which accounts are missing a date?
    n_expected = int(accounts["account_id"].nunique()) * n_unique_dates
    missing_coverage = n_expected - len(bal) if n_expected > 0 else 0

    return {
        "row_count": len(bal),
        "grain_columns": ["account_id", "balance_date"],
        "duplicate_grain_count": grain_dupes,
        "orphan_account_fk_count": orphan_acct,
        "balance_date_min": date_min,
        "balance_date_max": date_max,
        "unique_date_count": n_unique_dates,
        "negative_balance_count": neg_count,
        "balance_anomaly_count": anomaly_count,
        "expected_rows": n_expected,
        "missing_coverage_rows": missing_coverage,
        "checksum_sha256": dataframe_checksum(bal, ["account_id", "balance_date"]),
    }


def _count_balance_anomalies(bal: pd.DataFrame, factor: float) -> int:
    """Count rows where closing_balance_cents > factor × previous day's balance."""
    if "closing_balance_cents" not in bal.columns:
        return 0
    df = bal.sort_values(["account_id", "balance_date"]).copy()
    df["prev"] = df.groupby("account_id")["closing_balance_cents"].shift(1)
    df = df.dropna(subset=["prev"])
    df["ratio"] = df["closing_balance_cents"] / df["prev"].replace(0, np.nan)
    anomalies = df["ratio"] > factor
    return int(anomalies.sum())


# ── Composite inventory ───────────────────────────────────────────────────────


def compute_inventory(
    tables: dict[str, pd.DataFrame],
    *,
    seed: int,
    as_of_date: date,
    scenario: str | None = None,
) -> dict[str, Any]:
    """
    Compute a full dataset inventory from in-memory DataFrames.

    Returns a dictionary serialisable to JSON.
    """
    br = tables.get("branch_reference", pd.DataFrame())
    cust = tables.get("customers", pd.DataFrame())
    accts = tables.get("accounts", pd.DataFrame())
    bal = tables.get("daily_balances", pd.DataFrame())

    br_info = _analyze_branch_reference(br) if len(br) > 0 else {}
    cust_info = _analyze_customers(cust) if len(cust) > 0 else {}
    accts_info = (
        _analyze_accounts(accts, cust, br)
        if len(accts) > 0
        else {}
    )
    bal_info = (
        _analyze_daily_balances(bal, accts)
        if len(bal) > 0
        else {}
    )

    # High-level invariant checks
    checks: dict[str, bool | str] = {}

    if len(br) > 0 and len(accts) > 0:
        checks["all_branches_have_accounts"] = (
            set(br["branch_id"]) <= set(accts["branch_id"].dropna())
        )
    if len(cust) > 0 and len(accts) > 0:
        checks["all_customers_have_accounts"] = (
            set(cust["customer_id"]) <= set(accts["customer_id"].dropna())
        )
    if len(accts) > 0 and len(bal) > 0:
        exp_dates = (
            bal_info.get("unique_date_count", 0)
        )
        checks["all_accounts_have_full_balance_coverage"] = (
            bal_info.get("missing_coverage_rows", -1) == 0
        )
    if len(bal) > 0:
        checks["no_balance_anomalies"] = (
            bal_info.get("balance_anomaly_count", -1) == 0
        )
    if len(accts) > 0:
        checks["no_orphan_account_customer_fk"] = (
            accts_info.get("orphan_customer_fk_count", -1) == 0
        )
        checks["no_orphan_account_branch_fk"] = (
            accts_info.get("orphan_branch_fk_count", -1) == 0
        )

    return {
        "generator_version": "2.0",
        "seed": seed,
        "as_of_date": str(as_of_date),
        "scenario": scenario,
        "tables": {
            "branch_reference": br_info,
            "customers": cust_info,
            "accounts": accts_info,
            "daily_balances": bal_info,
        },
        "checks": {k: bool(v) for k, v in checks.items()},
    }


def write_inventory(inventory: dict, path: Path | str) -> None:
    Path(path).write_text(
        json.dumps(inventory, indent=2, default=str), encoding="utf-8"
    )


def verify_inventory(path: Path | str) -> tuple[bool, list[str]]:
    """
    Re-read inventory.json and verify all declared checks passed.

    Returns (all_passed: bool, list_of_failures: list[str]).
    """
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    failures = [k for k, v in data.get("checks", {}).items() if not v]
    return len(failures) == 0, failures
