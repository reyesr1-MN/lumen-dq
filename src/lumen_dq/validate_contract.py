"""
Executable validator for YAML data contracts.

Usage
-----
    python -m lumen_dq.validate_contract contracts/accounts.yml
    python -m lumen_dq.validate_contract contracts/accounts.yml --data-dir data/baseline

Exit codes: 0 = all rules passed, 1 = at least one failure.

Contract schema (YAML)
----------------------
table: <name>
version: "<semver>"
grain:
  - <column>            # PK / composite grain columns
columns:
  - name: <column>
    type: <VARCHAR|INTEGER|BIGINT|DATE|BOOLEAN>
    nullable: false      # optional, default true
    allowed_values: [A, B, C]  # optional
foreign_keys:
  - column: <col>
    references:
      table: <ref_table>
      column: <ref_col>
row_counts:
  min: <int>
  max: <int>
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import yaml

from lumen_dq.checks import (
    CheckResult,
    check_allowed_values,
    check_foreign_key,
    check_grain_uniqueness,
    check_not_null,
    check_row_count,
    print_results,
)
from lumen_dq.db import open_connection


def load_contract(path: Path | str) -> dict:
    with open(path, encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def validate(conn, contract: dict) -> list[CheckResult]:
    """
    Run all rules declared in *contract* against the DuckDB connection.

    Returns a list of CheckResult objects (one per rule).
    """
    results: list[CheckResult] = []
    table = contract["table"]

    # Grain uniqueness
    grain_cols: list[str] = contract.get("grain", [])
    if grain_cols:
        results.append(check_grain_uniqueness(conn, table, grain_cols))

    # Per-column rules
    for col_spec in contract.get("columns", []):
        col_name = col_spec["name"]

        if not col_spec.get("nullable", True):
            results.append(check_not_null(conn, table, col_name))

        if "allowed_values" in col_spec:
            results.append(
                check_allowed_values(conn, table, col_name, col_spec["allowed_values"])
            )

    # Foreign keys
    for fk in contract.get("foreign_keys", []):
        ref = fk["references"]
        results.append(
            check_foreign_key(conn, table, fk["column"], ref["table"], ref["column"])
        )

    # Row count bounds
    rc = contract.get("row_counts", {})
    if rc:
        actual: int = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        min_rows = rc.get("min", 0)
        max_rows = rc.get("max", 10_000_000)
        passed = min_rows <= actual <= max_rows
        results.append(
            CheckResult(
                name=f"row_count_bounds:{table}",
                layer="business",
                passed=passed,
                message=(
                    f"OK ({actual:,} rows)"
                    if passed
                    else f"Row count {actual:,} outside [{min_rows:,}, {max_rows:,}]"
                ),
            )
        )

    return results


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(
        description="Validate a YAML data contract against DuckDB parquet views."
    )
    parser.add_argument("contract", help="Path to YAML contract file")
    parser.add_argument(
        "--data-dir",
        default="data/baseline",
        help="Directory containing parquet files (default: data/baseline)",
    )
    args = parser.parse_args(argv)

    contract_path = Path(args.contract)
    if not contract_path.exists():
        print(f"ERROR: contract file not found: {contract_path}", file=sys.stderr)
        sys.exit(2)

    contract = load_contract(contract_path)
    print(f"Contract : {contract_path}")
    print(f"Table    : {contract['table']}  (v{contract.get('version', '?')})")

    conn = open_connection(Path(args.data_dir))
    results = validate(conn, contract)
    failures = print_results(results)
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
