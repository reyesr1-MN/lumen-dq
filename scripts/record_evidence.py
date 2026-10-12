"""
Record promotion evidence as a JSON file in evidence/.

Called by the staging_promotion GitHub Actions workflow.
Captures: git commit SHA, timestamp, check results, row counts.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT / "src"))

from lumen_dq.checks import run_all_checks
from lumen_dq.db import open_connection


def _git_sha() -> str:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"], text=True
        ).strip()
    except Exception:
        return os.environ.get("GITHUB_SHA", "unknown")


def main() -> None:
    evidence_dir = ROOT / "evidence"
    evidence_dir.mkdir(exist_ok=True)

    conn = open_connection(ROOT / "data" / "baseline")
    results = run_all_checks(conn)

    row_counts = {
        table: conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        for table in ("branch_reference", "customers", "accounts", "daily_balances")
    }

    payload = {
        "promoted_at": datetime.now(timezone.utc).isoformat(),
        "git_sha": _git_sha(),
        "checks_total": len(results),
        "checks_passed": sum(1 for r in results if r.passed),
        "checks_failed": sum(1 for r in results if not r.passed),
        "row_counts": row_counts,
        "check_results": [
            {
                "name": r.name,
                "layer": r.layer,
                "passed": r.passed,
                "message": r.message,
                "failing_rows": r.failing_rows,
            }
            for r in results
        ],
    }

    out_path = evidence_dir / f"evidence_{payload['promoted_at'].replace(':', '').replace('.', '')}.json"
    out_path.write_text(json.dumps(payload, indent=2))
    print(f"Evidence written to {out_path}")

    if payload["checks_failed"] > 0:
        print(f"ERROR: {payload['checks_failed']} check(s) failed — promotion blocked")
        sys.exit(1)


if __name__ == "__main__":
    main()
