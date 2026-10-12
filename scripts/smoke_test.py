#!/usr/bin/env python3
"""
scripts/smoke_test.py — Artifact smoke test for the staging promotion.

Verifies that a packaged artifact directory contains:
  - The four expected parquet files.
  - A manifest.json with the required fields.
  - SHA-256 checksums in the manifest match the actual files.

Exits 0 on success, 2 on any failure.

Usage
-----
  python scripts/smoke_test.py --artifact-dir artifact
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Smoke-test a staging artifact.")
    parser.add_argument("--artifact-dir", required=True)
    args = parser.parse_args(argv)

    artifact_dir = Path(args.artifact_dir)
    errors: list[str] = []

    # ── 1. Manifest present ───────────────────────────────────────────────────
    manifest_path = artifact_dir / "manifest.json"
    if not manifest_path.exists():
        print(f"FAIL: manifest.json not found in {artifact_dir}", file=sys.stderr)
        sys.exit(2)

    with open(manifest_path) as f:
        manifest = json.load(f)

    for field in ("commit_sha", "contract_version", "tables", "checksums"):
        if field not in manifest:
            errors.append(f"manifest.json missing field: {field!r}")

    # ── 2. Expected parquet files present ────────────────────────────────────
    expected = ("branch_reference", "customers", "accounts", "daily_balances")
    for table in expected:
        p = artifact_dir / f"{table}.parquet"
        if not p.exists():
            errors.append(f"Missing parquet file: {table}.parquet")

    if errors:
        for e in errors:
            print(f"FAIL: {e}", file=sys.stderr)
        sys.exit(2)

    # ── 3. Checksum verification ──────────────────────────────────────────────
    checksums: dict[str, str] = manifest.get("checksums", {})
    checksum_errors: list[str] = []
    for fname, expected_digest in checksums.items():
        fpath = artifact_dir / fname
        if not fpath.exists():
            checksum_errors.append(f"{fname}: file missing")
            continue
        actual = _sha256(fpath)
        if actual != expected_digest:
            checksum_errors.append(
                f"{fname}: checksum mismatch\n"
                f"  expected: {expected_digest}\n"
                f"  actual  : {actual}"
            )

    if checksum_errors:
        for e in checksum_errors:
            print(f"FAIL: {e}", file=sys.stderr)
        sys.exit(2)

    # ── 4. DQ summary must show no blocking failures ──────────────────────────
    dq = manifest.get("dq_summary", {})
    blocking = dq.get("blocking_failures")
    if blocking is not None and blocking > 0:
        print(
            f"FAIL: artifact manifest reports {blocking} blocking DQ failure(s).  "
            "Artifact must not be promoted.",
            file=sys.stderr,
        )
        sys.exit(2)

    print(f"Smoke test PASSED — artifact {artifact_dir}")
    print(f"  commit  : {manifest.get('commit_sha', '?')[:12]}…")
    print(f"  contract: v{manifest.get('contract_version', '?')}")
    print(f"  tables  : {len(checksums)} checksums verified")


if __name__ == "__main__":
    main()
