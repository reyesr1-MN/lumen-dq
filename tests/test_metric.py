"""
Layer 5 — Metric correctness tests.

Validates the precise metric definitions from SPEC.md:
- Branch-level: AVG of customer totals per (branch_id, balance_date)
- Bank-wide: AVG of customer totals per balance_date (NOT avg of branch avgs)
"""

from pathlib import Path

import pytest

_MODELS = Path(__file__).parent.parent / "models" / "reports"

BRANCH_SQL = (_MODELS / "avg_balance_by_branch_date.sql").read_text()
BANKWIDE_SQL = (_MODELS / "avg_balance_bankwide.sql").read_text()
CFO_SQL = (_MODELS / "cfo_weekly_packet.sql").read_text()

N_BRANCHES = 42
N_DATES = 30


# ── Branch-level metric ────────────────────────────────────────────────────────


def test_branch_metric_row_count(baseline_conn):
    """42 branches × 30 dates = 1,260 rows expected."""
    df = baseline_conn.execute(BRANCH_SQL).fetchdf()
    assert len(df) == N_BRANCHES * N_DATES, (
        f"Expected {N_BRANCHES * N_DATES} rows, got {len(df)}"
    )


def test_branch_metric_no_null_avg(baseline_conn):
    df = baseline_conn.execute(BRANCH_SQL).fetchdf()
    assert df["avg_customer_balance_cents"].notna().all(), (
        "avg_customer_balance_cents contains NULLs"
    )


def test_branch_metric_positive_balances(baseline_conn):
    df = baseline_conn.execute(BRANCH_SQL).fetchdf()
    assert (df["avg_customer_balance_cents"] > 0).all()


def test_branch_metric_customer_count_positive(baseline_conn):
    df = baseline_conn.execute(BRANCH_SQL).fetchdf()
    assert (df["customer_count"] > 0).all()


# ── Bank-wide metric ───────────────────────────────────────────────────────────


def test_bankwide_metric_row_count(baseline_conn):
    """One row per date = 30 rows expected."""
    df = baseline_conn.execute(BANKWIDE_SQL).fetchdf()
    assert len(df) == N_DATES, f"Expected {N_DATES} rows, got {len(df)}"


def test_bankwide_metric_no_null_avg(baseline_conn):
    df = baseline_conn.execute(BANKWIDE_SQL).fetchdf()
    assert df["avg_customer_balance_cents"].notna().all()


def test_bankwide_metric_covers_all_dates(baseline_conn):
    """All 30 dates from 2024-01-01 through 2024-01-30 must appear."""
    df = baseline_conn.execute(BANKWIDE_SQL).fetchdf()
    dates = df["balance_date"].astype(str).tolist()
    assert "2024-01-01" in dates
    assert "2024-01-30" in dates


# ── Metric correctness: bank-wide ≠ average of branch averages ────────────────


def test_bankwide_ne_avg_of_branch_avgs(baseline_conn):
    """
    The bank-wide metric is computed at customer-date grain.
    Averaging branch averages produces a different (biased) number when
    branch customer counts are unequal — which they are in this dataset.
    """
    branch_df = baseline_conn.execute(BRANCH_SQL).fetchdf()
    bankwide_df = baseline_conn.execute(BANKWIDE_SQL).fetchdf()

    avg_of_avgs = (
        branch_df.groupby("balance_date")["avg_customer_balance_cents"].mean()
    )
    true_bankwide = bankwide_df.set_index("balance_date")["avg_customer_balance_cents"]

    # Sort both series by index before comparing
    avg_of_avgs = avg_of_avgs.sort_index()
    true_bankwide = true_bankwide.sort_index()

    max_diff = (avg_of_avgs - true_bankwide).abs().max()
    assert max_diff > 0, (
        "Bank-wide avg and avg-of-branch-avgs are identical — "
        "this indicates the metrics are computed the same way, which is incorrect"
    )


# ── CFO packet ─────────────────────────────────────────────────────────────────


def test_cfo_packet_row_count(baseline_conn):
    df = baseline_conn.execute(CFO_SQL).fetchdf()
    assert len(df) == N_BRANCHES * N_DATES


def test_cfo_packet_no_null_branch_avg(baseline_conn):
    df = baseline_conn.execute(CFO_SQL).fetchdf()
    assert df["branch_avg_customer_balance_cents"].notna().all()


def test_cfo_packet_no_null_bank_avg(baseline_conn):
    df = baseline_conn.execute(CFO_SQL).fetchdf()
    assert df["bank_avg_customer_balance_cents"].notna().all()
