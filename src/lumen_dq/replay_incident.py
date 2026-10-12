"""
Incident replay: demonstrates the silent row-multiplication bug.

Run:
    python -m lumen_dq.replay_incident

What this script shows
----------------------
1. Baseline branch_reference passes grain check (42 unique rows).
2. Buggy branch_reference (5 duplicate branch_ids) FAILS grain check —
   this is the gate that would have blocked the CFO report.
3. The buggy join inflates the bank-wide average customer balance.
4. The correct SQL (no branch_reference join in the aggregation) is unaffected.

Incident window: 11 days (2024-01-11 through 2024-01-21).
Duplicated branch_ids: 1, 5, 12, 23, 37 (each appeared twice, 47 total rows).
"""

from __future__ import annotations

import shutil
import sys
import tempfile
from pathlib import Path

from lumen_dq.checks import check_branch_count, check_grain_uniqueness, print_results
from lumen_dq.db import open_connection
from lumen_dq.generate import generate_buggy_branch_reference

_DATA_DIR = Path("data/baseline")

# ── SQL templates ──────────────────────────────────────────────────────────────

# Correct bank-wide metric: customer-date grain, no join to branch_reference
_CORRECT_SQL = """
    SELECT AVG(cust_total) AS avg_customer_balance_cents
    FROM (
        SELECT a.customer_id,
               db.balance_date,
               SUM(db.closing_balance_cents) AS cust_total
        FROM   daily_balances db
        JOIN   accounts a ON db.account_id = a.account_id
        GROUP BY a.customer_id, db.balance_date
    ) _customer_totals
"""

# Buggy query: the production version that also joined to branch_reference.
# When branch_reference has duplicate branch_ids, the join fans out
# daily_balances rows for the affected branches, inflating every SUM.
_BUGGY_SQL = """
    SELECT AVG(cust_total) AS avg_customer_balance_cents
    FROM (
        SELECT a.customer_id,
               db.balance_date,
               SUM(db.closing_balance_cents) AS cust_total
        FROM   daily_balances db
        JOIN   accounts a  ON db.account_id  = a.account_id
        JOIN   branch_reference br ON a.branch_id = br.branch_id  -- INCIDENT LINE
        GROUP BY a.customer_id, db.balance_date
    ) _customer_totals
"""


# ── Main ───────────────────────────────────────────────────────────────────────


def main() -> None:
    print("\n" + "═" * 70)
    print("  INCIDENT REPLAY — Lumen Community Bank")
    print("  Non-unique branch_reference silently multiplied balance rows")
    print("═" * 70)

    if not (_DATA_DIR / "branch_reference.parquet").exists():
        print(
            "\n  ERROR: baseline data not found.\n"
            "  Run first:  make generate   (or: python -m lumen_dq.generate)\n",
            file=sys.stderr,
        )
        sys.exit(1)

    # ── Step 1: Baseline ────────────────────────────────────────────────────────
    print("\n── Step 1: Baseline data (expect PASS) ─────────────────────────────")
    baseline_conn = open_connection(_DATA_DIR)
    baseline_checks = [
        check_grain_uniqueness(baseline_conn, "branch_reference", ["branch_id"]),
        check_branch_count(baseline_conn, "branch_reference"),
    ]
    print_results(baseline_checks)

    correct_avg = baseline_conn.execute(_CORRECT_SQL).fetchone()[0]
    print(f"\n  Correct bank-wide avg customer balance : ${correct_avg / 100:>14,.2f}")

    # ── Step 2: Buggy fixture ───────────────────────────────────────────────────
    print("\n── Step 2: Buggy branch_reference (47 rows, 5 duplicated IDs) ──────")

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        for f in _DATA_DIR.glob("*.parquet"):
            shutil.copy(f, tmp)
        # Overwrite branch_reference with the buggy version
        generate_buggy_branch_reference(_DATA_DIR).to_parquet(
            tmp / "branch_reference.parquet", index=False
        )

        buggy_conn = open_connection(tmp)
        buggy_checks = [
            check_grain_uniqueness(buggy_conn, "branch_reference", ["branch_id"]),
            check_branch_count(buggy_conn, "branch_reference"),
        ]
        print_results(buggy_checks)

        inflated_avg = buggy_conn.execute(_BUGGY_SQL).fetchone()[0]
        overstatement_pct = (inflated_avg - correct_avg) / correct_avg * 100

        print(f"\n  Inflated bank-wide avg customer balance: ${inflated_avg / 100:>14,.2f}")
        print(f"  Overstatement                          : {overstatement_pct:>+.2f}%")

    # ── Step 3: Resolution ──────────────────────────────────────────────────────
    print("\n── Step 3: Resolution ──────────────────────────────────────────────")
    gate_failures = [c for c in buggy_checks if not c.passed]
    if gate_failures:
        print(
            f"\n  The grain check on branch_reference FAILED ({gate_failures[0].message}).\n"
            "  Had this gate been in place, the CFO report would have been\n"
            "  blocked at the quality gate and the 11-day overstatement\n"
            "  would have been caught on day 1.\n"
        )
    else:
        print("  (Unexpected: all buggy checks passed — review test fixture)")

    print("═" * 70 + "\n")


if __name__ == "__main__":
    main()
