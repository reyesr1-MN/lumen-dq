"""
CLI entrypoint: run the full baseline DQ check suite.

    python -m lumen_dq.run_checks
    python -m lumen_dq.run_checks --data-dir data/baseline --strict

Exit codes: 0 = all checks passed, 1 = at least one failure.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from lumen_dq.checks import run_all_checks, print_results
from lumen_dq.db import open_connection


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(description="Run all Lumen Community Bank DQ checks.")
    parser.add_argument(
        "--data-dir",
        default="data/baseline",
        help="Directory containing parquet files (default: data/baseline)",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Exit 1 on any failure (same as default; kept for CI clarity)",
    )
    args = parser.parse_args(argv)

    conn = open_connection(Path(args.data_dir))
    results = run_all_checks(conn)
    failures = print_results(results)
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
