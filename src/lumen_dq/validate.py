"""
Production validation CLI for Lumen Community Bank datasets.

Usage
-----
    python -m lumen_dq.validate <data_dir> [--json] [--out FILE]
    lumen-validate <data_dir> [--json] [--out FILE]

Arguments
---------
data_dir        Path to a directory containing the four parquet files:
                branch_reference.parquet, customers.parquet,
                accounts.parquet, daily_balances.parquet

Options
-------
--json          Also write machine-readable JSON to stdout (or --out FILE).
--out FILE      Write JSON results to FILE instead of stdout
                (implies --json).

Exit codes
----------
0   All checks passed, or only WARNING / INFO failures.
1   One or more BLOCKING checks failed.
2   Usage error: wrong arguments, missing directory, missing parquet files.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import duckdb

from lumen_dq.db import open_connection
from lumen_dq.validators import (
    Severity,
    ValidationResult,
    run_all_validators,
)


# ── Formatting helpers ─────────────────────────────────────────────────────────


def _human_table(results: list[ValidationResult]) -> str:
    """Return a formatted results table as a string."""
    col_w = 52
    lines: list[str] = []
    lines.append("")
    lines.append("═" * 95)
    lines.append("  LUMEN COMMUNITY BANK — DATASET VALIDATION")
    lines.append("═" * 95)
    lines.append(
        f"  {'CHECK ID':<{col_w}} {'SEV':<9} {'L':<2} {'TABLE':<20} STATUS"
    )
    lines.append("─" * 95)

    by_layer: dict[int, list[ValidationResult]] = {}
    for r in results:
        by_layer.setdefault(r.layer, []).append(r)

    layer_labels = {
        1: "Layer 1 — Schema & Types",
        2: "Layer 2 — Keys & Nullability",
        3: "Layer 3 — Relationships",
        4: "Layer 4 — Completeness & Business Rules",
        5: "Layer 5 — Join & Report Reconciliation",
    }

    for layer_num in sorted(by_layer):
        lines.append(f"\n  ── {layer_labels.get(layer_num, f'Layer {layer_num}')} ──")
        for r in by_layer[layer_num]:
            status = "PASS" if r.passed else f"FAIL  ({r.observed})"
            lines.append(
                f"  {r.check_id:<{col_w}} {r.severity.value:<9} {r.layer:<2} "
                f"{r.table:<20} {status}"
            )
            if not r.passed and r.next_step:
                # Indent the next-step hint
                hint = r.next_step[:120] + ("…" if len(r.next_step) > 120 else "")
                lines.append(f"  {'':>{col_w + 12}}  → {hint}")

    lines.append("")
    lines.append("─" * 95)
    total    = len(results)
    failures = [r for r in results if not r.passed]
    blocking = [r for r in failures if r.severity == Severity.BLOCKING]
    warnings = [r for r in failures if r.severity == Severity.WARNING]

    lines.append(
        f"  {total - len(failures)}/{total} checks passed"
        + (f"   {len(blocking)} BLOCKING" if blocking else "")
        + (f"   {len(warnings)} WARNING"  if warnings else "")
    )
    lines.append("═" * 95)
    lines.append("")
    return "\n".join(lines)


def _to_json_payload(results: list[ValidationResult], data_dir: str) -> dict:
    return {
        "data_dir": str(data_dir),
        "total":    len(results),
        "passed":   sum(1 for r in results if r.passed),
        "failed":   sum(1 for r in results if not r.passed),
        "blocking_failures": sum(
            1 for r in results if not r.passed and r.severity == Severity.BLOCKING
        ),
        "checks": [r.to_dict() for r in results],
    }


# ── Main entry point ──────────────────────────────────────────────────────────


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        prog="lumen-validate",
        description="Run layered data-quality validation on a Lumen dataset directory.",
    )
    parser.add_argument("data_dir", help="Path to directory containing parquet files")
    parser.add_argument(
        "--json", dest="emit_json", action="store_true",
        help="Write JSON results to stdout (or --out FILE)",
    )
    parser.add_argument(
        "--out", metavar="FILE",
        help="Write JSON results to FILE (implies --json)",
    )
    args = parser.parse_args(argv)

    data_dir = Path(args.data_dir)

    # ── Validate directory and files ──
    if not data_dir.is_dir():
        print(f"ERROR: '{data_dir}' is not a directory.", file=sys.stderr)
        sys.exit(2)

    required_tables = (
        "branch_reference", "customers", "accounts", "daily_balances"
    )
    missing_files = [
        t for t in required_tables
        if not (data_dir / f"{t}.parquet").exists()
    ]
    if missing_files:
        print(
            f"ERROR: Missing parquet file(s) in '{data_dir}': "
            + ", ".join(f"{t}.parquet" for t in missing_files),
            file=sys.stderr,
        )
        sys.exit(2)

    # ── Open connection and run validators ──
    try:
        conn = open_connection(data_dir)
    except Exception as exc:
        print(f"ERROR: Could not open DuckDB connection: {exc}", file=sys.stderr)
        sys.exit(2)

    results = run_all_validators(conn)

    # ── Human-readable output ──
    print(_human_table(results))

    # ── JSON output ──
    emit_json = args.emit_json or (args.out is not None)
    if emit_json:
        payload = _to_json_payload(results, args.data_dir)
        json_str = json.dumps(payload, indent=2, default=str)
        if args.out:
            Path(args.out).write_text(json_str)
            print(f"  JSON results written to: {args.out}\n")
        else:
            print(json_str)

    # ── Exit code ──
    has_blocking = any(
        not r.passed and r.severity == Severity.BLOCKING
        for r in results
    )
    sys.exit(1 if has_blocking else 0)


if __name__ == "__main__":
    main()
