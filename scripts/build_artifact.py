#!/usr/bin/env python3
"""
scripts/build_artifact.py — Package validated baseline data for staging.

Copies the four parquet files into a versioned artifact directory,
computes SHA-256 checksums, and writes a manifest.json recording
the commit SHA, contract version, and DQ results summary.

Usage
-----
  python scripts/build_artifact.py \\
      --data-dir data/baseline \\
      --dq-results evidence/dq_results.json \\
      --contract contracts/accounts.yaml \\
      --output-dir artifact \\
      --sha <git-sha> \\
      --contract-version 2.0.0
"""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Build a versioned staging artifact.")
    parser.add_argument("--data-dir", required=True)
    parser.add_argument("--dq-results", required=True)
    parser.add_argument("--contract", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--sha", required=True)
    parser.add_argument("--contract-version", required=True)
    args = parser.parse_args(argv)

    data_dir = Path(args.data_dir)
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    expected_tables = ("branch_reference", "customers", "accounts", "daily_balances")
    missing = [t for t in expected_tables if not (data_dir / f"{t}.parquet").exists()]
    if missing:
        print(f"ERROR: missing parquet files in {data_dir}: {missing}", file=sys.stderr)
        sys.exit(2)

    checksums: dict[str, str] = {}
    for table in expected_tables:
        src = data_dir / f"{table}.parquet"
        dst = out_dir / f"{table}.parquet"
        shutil.copy2(src, dst)
        checksums[f"{table}.parquet"] = _sha256(dst)

    # Load DQ summary
    dq_summary: dict = {}
    dq_path = Path(args.dq_results)
    if dq_path.exists():
        with open(dq_path) as f:
            dq_data = json.load(f)
        dq_summary = {
            "total": dq_data.get("total"),
            "passed": dq_data.get("passed"),
            "failed": dq_data.get("failed"),
            "blocking_failures": dq_data.get("blocking_failures"),
        }

    manifest = {
        "schema_version": "1",
        "built_at": datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "commit_sha": args.sha,
        "contract": args.contract,
        "contract_version": args.contract_version,
        "artifact_name": f"staging-{args.sha}",
        "tables": list(expected_tables),
        "checksums": checksums,
        "dq_summary": dq_summary,
    }

    manifest_path = out_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(f"Artifact written to {out_dir}/")
    print(f"  commit  : {args.sha}")
    print(f"  contract: v{args.contract_version}")
    for fname, digest in checksums.items():
        print(f"  {fname}: {digest[:16]}…")


if __name__ == "__main__":
    main()
