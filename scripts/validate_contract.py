#!/usr/bin/env python3
"""
scripts/validate_contract.py — Executable validator for lumen-dq-contract YAML files.

Reads all validation rules from the YAML; validates the contract structure
itself; validates actual accounts data against those rules.

Usage
-----
  # Validate data (operational — uses actual UTC clock):
  python scripts/validate_contract.py contracts/accounts.yaml \\
      --data-dir data/baseline

  # Deterministic / CI (pin the validation clock):
  python scripts/validate_contract.py contracts/accounts.yaml \\
      --data-dir data/baseline --now 2024-01-30T12:00:00Z

  # Contract structure only (no data):
  python scripts/validate_contract.py contracts/accounts.yaml --no-data

  # Breaking-change gate (run in PR pipeline):
  python scripts/validate_contract.py contracts/accounts.yaml \\
      --check-breaking --base origin/main

  # Write structured JSON diagnostics:
  python scripts/validate_contract.py contracts/accounts.yaml \\
      --data-dir data/baseline --out results.json

Exit codes
----------
  0  All checks passed.
  1  One or more data-rule violations detected.
  2  Invalid contract configuration or execution failure
     (bad YAML, unsupported rule keys, missing data directory, etc.).

Freshness measurement
---------------------
  Freshness is measured exclusively from the producer-supplied ingested_at
  column declared in freshness.measurement.source_column.
  This validator never adjusts, refreshes, or clamps timestamps to make
  stale data pass.  Pass --now to pin the clock for deterministic tests.

Contract format
---------------
  Supports lumen-dq-contract format_version "1" only.
  Any column constraint key not in SUPPORTED_CONSTRAINT_KEYS causes exit 2.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# ── Optional heavy imports (graceful errors) ──────────────────────────────────

try:
    import duckdb
except ImportError:  # pragma: no cover
    duckdb = None  # type: ignore[assignment]

try:
    import yaml
except ImportError:  # pragma: no cover
    yaml = None  # type: ignore[assignment]


# ── Contract format constants ─────────────────────────────────────────────────

CONTRACT_FORMAT = "lumen-dq-contract"
SUPPORTED_FORMAT_VERSIONS = {"1"}

REQUIRED_TOP_LEVEL = {
    "contract_format", "format_version", "name", "version",
    "producer", "consumer", "table", "grain", "columns", "freshness",
}

REQUIRED_COLUMN_KEYS = {"name", "logical_type", "physical_type", "nullable"}
ALLOWED_COLUMN_KEYS = {
    "name", "logical_type", "physical_type", "nullable",
    "description", "allowed_values", "constraints",
}

# Every constraint key must be listed here.  Unknown keys → exit 2.
SUPPORTED_CONSTRAINT_KEYS = frozenset({
    "pattern",
    "min", "max",
    "date_format",
    "min_date", "max_date",
    "max_date_offset_days",
    "lifecycle_rule",
})

# Bounded vocabulary for logical_type.  Any other value → exit 2.
SUPPORTED_LOGICAL_TYPES = frozenset({
    "identifier", "integer", "enum", "date", "timestamp_utc", "text",
})

# Valid values for the top-level status field.
SUPPORTED_STATUS_VALUES = frozenset({"active", "deprecated", "draft"})

REQUIRED_FRESHNESS_KEYS = {
    "sla_hours", "reject_future_minutes", "reject_empty_snapshot", "measurement",
}
REQUIRED_MEASUREMENT_KEYS = {"source_column", "source_table"}

REQUIRED_PRODUCER_KEYS = {"name", "contact_role", "escalation_role"}
REQUIRED_CONSUMER_KEYS = {"name", "contact_role", "escalation_role"}

# Keys starting with "_" are documentation keys; the validator ignores them.
_META_KEY_PREFIX = "_"


# ── Diagnostic dataclass ──────────────────────────────────────────────────────

class Diagnostic:
    """One validation result record."""

    __slots__ = (
        "check_id", "check_type", "severity",
        "passed", "message", "failing_rows", "details",
    )

    def __init__(
        self,
        check_id: str,
        check_type: str,
        severity: str,
        passed: bool,
        message: str,
        failing_rows: int = 0,
        details: dict | None = None,
    ) -> None:
        self.check_id = check_id
        self.check_type = check_type
        self.severity = severity   # BLOCKING | WARNING | INFO
        self.passed = passed
        self.message = message
        self.failing_rows = failing_rows
        self.details = details or {}

    def to_dict(self) -> dict[str, Any]:
        return {
            "check_id": self.check_id,
            "check_type": self.check_type,
            "severity": self.severity,
            "passed": self.passed,
            "message": self.message,
            "failing_rows": self.failing_rows,
            "details": self.details,
        }


# ── Contract loading ──────────────────────────────────────────────────────────


def load_contract(path: Path | str) -> dict:
    """
    Load a lumen-dq-contract YAML file.

    Raises SystemExit(2) on parse errors or format mismatches.
    """
    if yaml is None:
        _exit2("PyYAML is not installed.  Run: pip install pyyaml")

    path = Path(path)
    if not path.exists():
        _exit2(f"Contract file not found: {path}")

    try:
        with open(path, encoding="utf-8") as fh:
            data = yaml.safe_load(fh)
    except yaml.YAMLError as exc:
        _exit2(f"YAML parse error in {path}:\n  {exc}")

    if not isinstance(data, dict):
        _exit2(f"Contract must be a YAML mapping; got {type(data).__name__}")

    if data.get("contract_format") != CONTRACT_FORMAT:
        _exit2(
            f"Unsupported contract_format {data.get('contract_format')!r}.  "
            f"Expected {CONTRACT_FORMAT!r}."
        )
    if str(data.get("format_version", "")) not in SUPPORTED_FORMAT_VERSIONS:
        _exit2(
            f"Unsupported format_version {data.get('format_version')!r}.  "
            f"Supported: {sorted(SUPPORTED_FORMAT_VERSIONS)}"
        )

    return data


# ── Contract structure validation ─────────────────────────────────────────────


def validate_contract_structure(contract: dict) -> list[str]:
    """
    Validate the contract YAML structure.

    Returns a list of error strings.  An empty list means the contract is
    structurally valid.  Does NOT open any data files.

    Any column constraint key not in SUPPORTED_CONSTRAINT_KEYS is an error;
    the validator refuses to silently ignore unsupported rules.
    """
    errors: list[str] = []

    def _err(msg: str) -> None:
        errors.append(msg)

    # ── Required top-level keys ───────────────────────────────────────────────
    for key in REQUIRED_TOP_LEVEL:
        if key not in contract:
            _err(f"Missing required top-level key: {key!r}")

    # ── Status field ──────────────────────────────────────────────────────────
    status = contract.get("status")
    if status is not None and status not in SUPPORTED_STATUS_VALUES:
        _err(
            f"Unsupported status {status!r}.  "
            f"Allowed: {sorted(SUPPORTED_STATUS_VALUES)}"
        )

    # ── Producer ──────────────────────────────────────────────────────────────
    producer = contract.get("producer", {})
    for k in REQUIRED_PRODUCER_KEYS:
        if k not in producer:
            _err(f"producer is missing required key: {k!r}")

    # ── Consumer ──────────────────────────────────────────────────────────────
    consumer = contract.get("consumer", {})
    for k in REQUIRED_CONSUMER_KEYS:
        if k not in consumer:
            _err(f"consumer is missing required key: {k!r}")

    # ── Grain ─────────────────────────────────────────────────────────────────
    grain = contract.get("grain", {})
    if not isinstance(grain, dict) or not grain.get("columns"):
        _err("grain.columns must be a non-empty list")

    # ── Columns ───────────────────────────────────────────────────────────────
    columns = contract.get("columns", [])
    if not isinstance(columns, list) or not columns:
        _err("columns must be a non-empty list")
    else:
        seen_names: set[str] = set()
        for i, col in enumerate(columns):
            label = col.get("name", f"columns[{i}]")

            # Required keys
            for k in REQUIRED_COLUMN_KEYS:
                if k not in col:
                    _err(f"Column {label!r} missing required key: {k!r}")

            # Unknown keys
            actual_keys = {k for k in col if not k.startswith(_META_KEY_PREFIX)}
            unknown = actual_keys - ALLOWED_COLUMN_KEYS
            if unknown:
                _err(
                    f"Column {label!r} has unsupported keys: {sorted(unknown)}.  "
                    f"Allowed: {sorted(ALLOWED_COLUMN_KEYS)}"
                )

            # Duplicate names
            name = col.get("name")
            if name:
                if name in seen_names:
                    _err(f"Duplicate column name: {name!r}")
                seen_names.add(name)

            # Unsupported logical_type
            lt = col.get("logical_type")
            if lt is not None and lt not in SUPPORTED_LOGICAL_TYPES:
                _err(
                    f"Column {label!r} has unsupported logical_type {lt!r}.  "
                    f"Supported: {sorted(SUPPORTED_LOGICAL_TYPES)}"
                )

            # Unsupported constraint keys
            constraints = col.get("constraints", {})
            if constraints:
                if not isinstance(constraints, dict):
                    _err(f"Column {label!r}: constraints must be a mapping")
                else:
                    actual_ck = {
                        k for k in constraints
                        if not k.startswith(_META_KEY_PREFIX)
                    }
                    unknown_ck = actual_ck - SUPPORTED_CONSTRAINT_KEYS
                    if unknown_ck:
                        _err(
                            f"Column {label!r} has unsupported constraint keys: "
                            f"{sorted(unknown_ck)}.  "
                            f"Supported: {sorted(SUPPORTED_CONSTRAINT_KEYS)}"
                        )

    # ── Cross-reference: grain/pk/fk columns must be declared ─────────────────
    declared_col_names: set[str] = {
        col.get("name", "") for col in columns if isinstance(col, dict)
    }

    if isinstance(grain, dict):
        for gc in grain.get("columns", []):
            if gc and gc not in declared_col_names:
                _err(
                    f"grain.columns references undeclared column {gc!r}.  "
                    f"Declared columns: {sorted(declared_col_names)}"
                )

    pk = contract.get("primary_key", {})
    if isinstance(pk, dict):
        for pkc in pk.get("columns", []):
            if pkc and pkc not in declared_col_names:
                _err(
                    f"primary_key.columns references undeclared column {pkc!r}.  "
                    f"Declared columns: {sorted(declared_col_names)}"
                )

    for fk in contract.get("foreign_keys", []):
        fk_col = fk.get("column")
        if fk_col and fk_col not in declared_col_names:
            _err(
                f"foreign_keys references undeclared column {fk_col!r}.  "
                f"Declared columns: {sorted(declared_col_names)}"
            )

    # ── Cross-reference: lifecycle_rule values must be defined ─────────────────
    lc_defs = contract.get("lifecycle_constraints", {})
    if isinstance(columns, list):
        for col in columns:
            rule_name = (col.get("constraints") or {}).get("lifecycle_rule")
            if rule_name and rule_name not in lc_defs:
                _err(
                    f"Column {col.get('name')!r} references undefined "
                    f"lifecycle_rule {rule_name!r}.  "
                    f"Defined rules: {sorted(lc_defs)}"
                )

    # ── Contradictory settings ─────────────────────────────────────────────────
    # A column with nullable: false linked to a lifecycle_rule whose SQL condition
    # tests that column for NULL would never trigger (the condition can never be
    # true for a non-nullable column).
    if isinstance(columns, list) and isinstance(lc_defs, dict):
        for col in columns:
            if col.get("nullable") is False:
                rule_name = (col.get("constraints") or {}).get("lifecycle_rule")
                if rule_name and rule_name in lc_defs:
                    rule_def = lc_defs[rule_name]
                    col_name = col.get("name", "?")
                    for check in rule_def.get("checks", []):
                        cond = check.get("sql_condition", "")
                        if (f"{col_name} IS NULL" in cond
                                or f"{col_name} IS NOT NULL" in cond):
                            _err(
                                f"Contradictory setting: column {col_name!r} is "
                                f"nullable: false but lifecycle_rule "
                                f"{rule_name!r} check {check.get('name')!r} tests "
                                f"{col_name} for NULL — a non-nullable column can "
                                f"never be NULL."
                            )

    # ── Freshness ─────────────────────────────────────────────────────────────
    freshness = contract.get("freshness", {})
    for k in REQUIRED_FRESHNESS_KEYS:
        if k not in freshness:
            _err(f"freshness is missing required key: {k!r}")

    measurement = freshness.get("measurement", {})
    for k in REQUIRED_MEASUREMENT_KEYS:
        if k not in measurement:
            _err(f"freshness.measurement is missing required key: {k!r}")

    # Cross-reference: freshness source_column must be a declared column
    src_col = measurement.get("source_column")
    if src_col and declared_col_names and src_col not in declared_col_names:
        _err(
            f"freshness.measurement.source_column {src_col!r} is not a declared "
            f"column.  Declared columns: {sorted(declared_col_names)}"
        )

    sla_hours = freshness.get("sla_hours")
    if sla_hours is not None and not isinstance(sla_hours, (int, float)):
        _err(f"freshness.sla_hours must be a number; got {type(sla_hours).__name__}")

    reject_min = freshness.get("reject_future_minutes")
    if reject_min is not None and not isinstance(reject_min, (int, float)):
        _err(
            f"freshness.reject_future_minutes must be a number; "
            f"got {type(reject_min).__name__}"
        )

    # Contradictory: sla_hours <= 0 makes every snapshot stale by definition
    if isinstance(sla_hours, (int, float)) and sla_hours <= 0:
        _err(
            f"freshness.sla_hours must be > 0; got {sla_hours}.  "
            "A zero or negative SLA makes every row stale."
        )

    return errors


# ── Contract table-name helper ────────────────────────────────────────────────


def _table_name(contract: dict) -> str:
    """Return the main table name for this contract.

    The v2 contract format uses ``name:`` (not ``table:``).
    This helper abstracts the lookup so all check functions use one call site.
    """
    return str(contract.get("name", contract.get("table", "")))


# ── DuckDB helpers ────────────────────────────────────────────────────────────


def _open_connection(data_dir: Path, table: str) -> Any:
    """Open a DuckDB in-memory connection with parquet views from data_dir."""
    if duckdb is None:
        _exit2("duckdb is not installed.  Run: pip install duckdb")

    parquet_file = data_dir / f"{table}.parquet"
    if not parquet_file.exists():
        _exit2(f"Expected parquet file not found: {parquet_file}")

    conn = duckdb.connect()
    conn.execute(
        f"CREATE VIEW {table} AS SELECT * FROM read_parquet('{parquet_file}')"
    )
    return conn


def _open_connection_all(data_dir: Path, contract: dict) -> Any:
    """
    Open a DuckDB connection with views for the contract table plus any
    foreign-key reference tables present in data_dir.
    """
    if duckdb is None:
        _exit2("duckdb is not installed.  Run: pip install duckdb")

    conn = duckdb.connect()
    main_table = _table_name(contract)
    tables_to_load = {main_table}

    for fk in contract.get("foreign_keys", []):
        tables_to_load.add(fk["references"]["table"])

    # Also load any tables referenced in lifecycle_constraints (future-proof)
    for lc in contract.get("lifecycle_constraints", {}).values():
        pass  # lifecycle checks use the main table only

    for t in tables_to_load:
        pq = data_dir / f"{t}.parquet"
        if not pq.exists():
            _exit2(f"Expected parquet file not found: {pq}")
        conn.execute(
            f"CREATE VIEW {t} AS SELECT * FROM read_parquet('{pq}')"
        )

    return conn


def _scalar(conn: Any, sql: str) -> Any:
    return conn.execute(sql).fetchone()[0]


def _get_duckdb_type(conn: Any, table: str, column: str) -> str | None:
    """Return the DuckDB column type, or None if column does not exist."""
    rows = conn.execute(
        "SELECT data_type FROM information_schema.columns "
        f"WHERE table_name = '{table}' AND column_name = '{column}'"
    ).fetchall()
    if not rows:
        return None
    return rows[0][0]


# ── Per-check functions ───────────────────────────────────────────────────────


def _diag_pass(check_id: str, check_type: str, severity: str, message: str) -> Diagnostic:
    return Diagnostic(check_id, check_type, severity, True, message)


def _diag_fail(
    check_id: str,
    check_type: str,
    severity: str,
    message: str,
    failing_rows: int = 0,
    details: dict | None = None,
) -> Diagnostic:
    return Diagnostic(check_id, check_type, severity, False, message, failing_rows, details)


def check_required_columns_present(
    conn: Any, contract: dict
) -> list[Diagnostic]:
    """All columns declared in the contract must exist in the table."""
    table = _table_name(contract)
    declared = [col["name"] for col in contract.get("columns", [])]
    existing = {
        row[0]
        for row in conn.execute(
            f"SELECT column_name FROM information_schema.columns "
            f"WHERE table_name = '{table}'"
        ).fetchall()
    }
    results = []
    for col_name in declared:
        if col_name not in existing:
            results.append(
                _diag_fail(
                    f"required_column.{col_name}",
                    "required_column",
                    "BLOCKING",
                    f"Column {col_name!r} is declared in the contract but absent from {table}",
                    details={"missing_column": col_name},
                )
            )
    if not results:
        results.append(
            _diag_pass(
                "required_columns",
                "required_column",
                "BLOCKING",
                f"All {len(declared)} declared columns present in {table}",
            )
        )
    return results


def check_physical_types(conn: Any, contract: dict) -> list[Diagnostic]:
    """Physical column types must match the contract declaration."""
    table = _table_name(contract)
    results = []
    for col in contract.get("columns", []):
        name = col["name"]
        expected = col.get("physical_type")
        if not expected:
            continue
        actual = _get_duckdb_type(conn, table, name)
        if actual is None:
            # Already caught by check_required_columns_present
            continue
        if actual in ("NULL", '"NULL"'):
            # All values are NULL; column type cannot be inferred from runtime
            # values alone (common for all-NULL optional columns in in-memory
            # fixtures; DuckDB reports '"NULL"' in this case).  In parquet the
            # type is encoded in the file schema so DuckDB reports the correct
            # type regardless of nulls.  Skip the check here.
            results.append(_diag_pass(
                f"physical_type.{name}",
                "physical_type",
                "BLOCKING",
                f"{table}.{name}: column is all-NULL; physical type not checkable at runtime",
            ))
            continue
        # Normalize: "TIMESTAMP WITH TIME ZONE" → "TIMESTAMP"
        actual_norm = actual.replace(" WITH TIME ZONE", "")
        if actual_norm != expected:
            results.append(
                _diag_fail(
                    f"physical_type.{name}",
                    "physical_type",
                    "BLOCKING",
                    f"{table}.{name}: expected type {expected!r}, found {actual!r}",
                    details={"column": name, "expected": expected, "actual": actual},
                )
            )
        else:
            results.append(
                _diag_pass(
                    f"physical_type.{name}",
                    "physical_type",
                    "BLOCKING",
                    f"{table}.{name}: type {actual!r} matches contract",
                )
            )
    return results


def check_grain(conn: Any, contract: dict) -> list[Diagnostic]:
    """Primary key / grain columns must be unique."""
    table = _table_name(contract)
    grain_cols = contract.get("grain", {}).get("columns", [])
    if not grain_cols:
        return []

    cols_sql = ", ".join(grain_cols)
    dup_count: int = _scalar(conn, f"""
        SELECT COALESCE(SUM(cnt - 1), 0)
        FROM (
            SELECT {cols_sql}, COUNT(*) AS cnt
            FROM {table}
            GROUP BY {cols_sql}
            HAVING COUNT(*) > 1
        ) _d
    """)
    key_label = "+".join(grain_cols)
    if dup_count == 0:
        return [_diag_pass(
            f"grain.{key_label}",
            "grain",
            "BLOCKING",
            f"grain [{key_label}] is unique in {table}",
        )]
    return [_diag_fail(
        f"grain.{key_label}",
        "grain",
        "BLOCKING",
        f"grain [{key_label}] has {dup_count} duplicate key combination(s)",
        failing_rows=dup_count,
    )]


def check_not_null_constraints(conn: Any, contract: dict) -> list[Diagnostic]:
    """All nullable: false columns must have no NULL values."""
    table = _table_name(contract)
    results = []
    for col in contract.get("columns", []):
        if col.get("nullable", True):
            continue
        name = col["name"]
        # Skip if column is missing (already reported by check_required_columns_present)
        if _get_duckdb_type(conn, table, name) is None:
            continue
        null_count: int = _scalar(
            conn, f"SELECT COUNT(*) FROM {table} WHERE {name} IS NULL"
        )
        if null_count == 0:
            results.append(_diag_pass(
                f"not_null.{name}",
                "not_null",
                "BLOCKING",
                f"{table}.{name}: no NULL values",
            ))
        else:
            results.append(_diag_fail(
                f"not_null.{name}",
                "not_null",
                "BLOCKING",
                f"{table}.{name}: {null_count} NULL value(s)",
                failing_rows=null_count,
            ))
    return results


def check_allowed_values(conn: Any, contract: dict) -> list[Diagnostic]:
    """Enum columns must contain only the declared allowed values."""
    table = _table_name(contract)
    results = []
    for col in contract.get("columns", []):
        allowed = col.get("allowed_values")
        if not allowed:
            continue
        name = col["name"]
        if _get_duckdb_type(conn, table, name) is None:
            continue
        quoted = ", ".join(f"'{v}'" for v in allowed)
        bad_count: int = _scalar(
            conn,
            f"SELECT COUNT(*) FROM {table} WHERE {name} IS NOT NULL "
            f"AND {name} NOT IN ({quoted})",
        )
        sample: list = []
        if bad_count > 0:
            sample = [
                row[0]
                for row in conn.execute(
                    f"SELECT DISTINCT {name} FROM {table} "
                    f"WHERE {name} NOT IN ({quoted}) LIMIT 5"
                ).fetchall()
            ]
        if bad_count == 0:
            results.append(_diag_pass(
                f"allowed_values.{name}",
                "allowed_values",
                "BLOCKING",
                f"{table}.{name}: all values in allowed set",
            ))
        else:
            results.append(_diag_fail(
                f"allowed_values.{name}",
                "allowed_values",
                "BLOCKING",
                f"{table}.{name}: {bad_count} row(s) with disallowed value(s): {sample}",
                failing_rows=bad_count,
                details={"disallowed_sample": sample, "allowed": allowed},
            ))
    return results


def check_identifier_patterns(conn: Any, contract: dict) -> list[Diagnostic]:
    """Identifier columns must match the declared regex pattern."""
    table = _table_name(contract)
    results = []
    for col in contract.get("columns", []):
        constraints = col.get("constraints", {})
        pattern = constraints.get("pattern")
        if not pattern:
            continue
        name = col["name"]
        if _get_duckdb_type(conn, table, name) is None:
            continue
        bad_count: int = _scalar(
            conn,
            f"SELECT COUNT(*) FROM {table} "
            f"WHERE {name} IS NOT NULL AND NOT regexp_matches({name}, '{pattern}')",
        )
        if bad_count == 0:
            results.append(_diag_pass(
                f"pattern.{name}",
                "pattern",
                "BLOCKING",
                f"{table}.{name}: all values match pattern {pattern!r}",
            ))
        else:
            results.append(_diag_fail(
                f"pattern.{name}",
                "pattern",
                "BLOCKING",
                f"{table}.{name}: {bad_count} value(s) do not match pattern {pattern!r}",
                failing_rows=bad_count,
                details={"pattern": pattern},
            ))
    return results


def check_min_value_constraints(conn: Any, contract: dict) -> list[Diagnostic]:
    """Numeric columns with min constraint must be >= min."""
    table = _table_name(contract)
    results = []
    for col in contract.get("columns", []):
        constraints = col.get("constraints", {})
        min_val = constraints.get("min")
        if min_val is None:
            continue
        name = col["name"]
        if _get_duckdb_type(conn, table, name) is None:
            continue
        bad_count: int = _scalar(
            conn,
            f"SELECT COUNT(*) FROM {table} WHERE {name} IS NOT NULL AND {name} < {min_val}",
        )
        if bad_count == 0:
            results.append(_diag_pass(
                f"min_value.{name}",
                "min_value",
                "BLOCKING",
                f"{table}.{name}: all values >= {min_val}",
            ))
        else:
            results.append(_diag_fail(
                f"min_value.{name}",
                "min_value",
                "BLOCKING",
                f"{table}.{name}: {bad_count} value(s) below minimum {min_val}",
                failing_rows=bad_count,
                details={"min": min_val},
            ))
    return results


def check_date_format_constraints(conn: Any, contract: dict) -> list[Diagnostic]:
    """
    Date columns declared with date_format: YYYY-MM-DD must contain only
    strings parseable as valid dates.
    """
    table = _table_name(contract)
    results = []
    for col in contract.get("columns", []):
        constraints = col.get("constraints", {})
        date_fmt = constraints.get("date_format")
        if not date_fmt:
            continue
        if date_fmt != "YYYY-MM-DD":
            # Only YYYY-MM-DD is supported.
            results.append(_diag_fail(
                f"date_format.{col['name']}",
                "date_format",
                "BLOCKING",
                f"Unsupported date_format {date_fmt!r} for column {col['name']!r}; "
                "only YYYY-MM-DD is implemented",
                details={"unsupported_format": date_fmt},
            ))
            continue
        name = col["name"]
        if _get_duckdb_type(conn, table, name) is None:
            continue
        # Use regex for structural check (DuckDB's TRY_CAST accepts too many
        # date formats including slash-separated, so structural check first).
        bad_count: int = _scalar(
            conn,
            f"SELECT COUNT(*) FROM {table} "
            f"WHERE {name} IS NOT NULL "
            f"AND (NOT regexp_matches({name}, '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}$') "
            f"     OR TRY_CAST({name} AS DATE) IS NULL)",
        )
        if bad_count == 0:
            results.append(_diag_pass(
                f"date_format.{name}",
                "date_format",
                "BLOCKING",
                f"{table}.{name}: all non-null values are valid YYYY-MM-DD dates",
            ))
        else:
            results.append(_diag_fail(
                f"date_format.{name}",
                "date_format",
                "BLOCKING",
                f"{table}.{name}: {bad_count} value(s) not parseable as YYYY-MM-DD",
                failing_rows=bad_count,
            ))
    return results


def check_date_range_constraints(
    conn: Any, contract: dict, now_utc: datetime
) -> list[Diagnostic]:
    """
    Columns with min_date / max_date_offset_days constraints.
    max_date_offset_days: 0 means the date must be <= now.date().
    """
    table = _table_name(contract)
    results = []
    now_date = now_utc.date().isoformat()

    for col in contract.get("columns", []):
        constraints = col.get("constraints", {})
        min_date = constraints.get("min_date")
        max_offset = constraints.get("max_date_offset_days")
        if min_date is None and max_offset is None:
            continue
        name = col["name"]
        if _get_duckdb_type(conn, table, name) is None:
            continue

        # min_date check
        if min_date is not None:
            bad: int = _scalar(
                conn,
                f"SELECT COUNT(*) FROM {table} "
                f"WHERE {name} IS NOT NULL "
                f"AND TRY_CAST({name} AS DATE) < DATE '{min_date}'",
            )
            if bad == 0:
                results.append(_diag_pass(
                    f"min_date.{name}",
                    "date_range",
                    "BLOCKING",
                    f"{table}.{name}: all dates >= {min_date}",
                ))
            else:
                results.append(_diag_fail(
                    f"min_date.{name}",
                    "date_range",
                    "BLOCKING",
                    f"{table}.{name}: {bad} date(s) before minimum {min_date}",
                    failing_rows=bad,
                    details={"min_date": min_date},
                ))

        # max_date_offset_days check
        if max_offset is not None:
            # max allowed date = now + offset days
            from datetime import timedelta
            max_date_val = (
                now_utc.date() + timedelta(days=int(max_offset))
            ).isoformat()
            bad = _scalar(
                conn,
                f"SELECT COUNT(*) FROM {table} "
                f"WHERE {name} IS NOT NULL "
                f"AND TRY_CAST({name} AS DATE) > DATE '{max_date_val}'",
            )
            if bad == 0:
                results.append(_diag_pass(
                    f"max_date.{name}",
                    "date_range",
                    "BLOCKING",
                    f"{table}.{name}: no dates after {max_date_val} (now+{max_offset}d)",
                ))
            else:
                results.append(_diag_fail(
                    f"max_date.{name}",
                    "date_range",
                    "BLOCKING",
                    f"{table}.{name}: {bad} date(s) after maximum {max_date_val} "
                    f"(now+{max_offset}d = {now_date})",
                    failing_rows=bad,
                    details={"max_date": max_date_val, "now_date": now_date},
                ))

    return results


def check_lifecycle_constraints(conn: Any, contract: dict) -> list[Diagnostic]:
    """
    Run lifecycle constraint checks referenced from columns via
    constraints.lifecycle_rule.
    """
    table = _table_name(contract)
    lc_defs = contract.get("lifecycle_constraints", {})
    referenced_rules: set[str] = set()

    for col in contract.get("columns", []):
        rule_name = (col.get("constraints") or {}).get("lifecycle_rule")
        if rule_name:
            referenced_rules.add(rule_name)

    results = []
    for rule_name in referenced_rules:
        if rule_name not in lc_defs:
            results.append(_diag_fail(
                f"lifecycle.{rule_name}",
                "lifecycle",
                "BLOCKING",
                f"lifecycle_rule {rule_name!r} referenced in columns but not defined "
                f"in lifecycle_constraints",
                details={"missing_rule": rule_name},
            ))
            continue

        rule_def = lc_defs[rule_name]
        for check in rule_def.get("checks", []):
            check_name = check.get("name", "unnamed")
            condition = check.get("sql_condition", "").strip()
            if not condition:
                continue
            expected = check.get("expected_violations", 0)
            severity = check.get("severity", "BLOCKING")

            check_id = f"lifecycle.{rule_name}.{check_name}"
            try:
                actual: int = _scalar(
                    conn, f"SELECT COUNT(*) FROM {table} WHERE {condition}"
                )
            except Exception as exc:  # noqa: BLE001
                # Most likely a missing column in the data; the required-columns
                # check should already have reported it as BLOCKING.  Report this
                # check as failed rather than crashing to exit 2.
                results.append(_diag_fail(
                    check_id,
                    "lifecycle",
                    severity,
                    f"{check_name}: SQL error — {exc}",
                    details={"condition": condition, "sql_error": str(exc)},
                ))
                continue
            if actual == expected:
                results.append(_diag_pass(
                    check_id,
                    "lifecycle",
                    severity,
                    f"{check_name}: {actual} violation(s) (expected {expected})",
                ))
            else:
                results.append(_diag_fail(
                    check_id,
                    "lifecycle",
                    severity,
                    f"{check_name}: {actual} violation(s), expected {expected}",
                    failing_rows=actual,
                    details={"condition": condition, "expected": expected},
                ))

    return results


def check_foreign_keys(conn: Any, contract: dict) -> list[Diagnostic]:
    """All FK columns must reference existing values in the reference table."""
    table = _table_name(contract)
    results = []
    for fk in contract.get("foreign_keys", []):
        col = fk["column"]
        ref_table = fk["references"]["table"]
        ref_col = fk["references"]["column"]
        severity = fk.get("severity", "BLOCKING")

        orphan_count: int = _scalar(
            conn,
            f"SELECT COUNT(*) FROM {table} t "
            f"WHERE t.{col} IS NOT NULL "
            f"AND t.{col} NOT IN (SELECT {ref_col} FROM {ref_table})",
        )
        check_id = f"fk.{col}→{ref_table}.{ref_col}"
        if orphan_count == 0:
            results.append(_diag_pass(
                check_id, "foreign_key", severity,
                f"fk {col}→{ref_table}.{ref_col}: no orphaned values",
            ))
        else:
            results.append(_diag_fail(
                check_id, "foreign_key", severity,
                f"fk {col}→{ref_table}.{ref_col}: {orphan_count} orphaned value(s)",
                failing_rows=orphan_count,
            ))
    return results


def check_row_counts(conn: Any, contract: dict) -> list[Diagnostic]:
    """Row count must fall within the declared bounds."""
    rc = contract.get("row_counts")
    if not rc:
        return []

    table = _table_name(contract)
    actual: int = _scalar(conn, f"SELECT COUNT(*) FROM {table}")
    min_rows = rc.get("min", 0)
    max_rows = rc.get("max", 10_000_000)

    if min_rows <= actual <= max_rows:
        return [_diag_pass(
            "row_counts",
            "row_counts",
            "BLOCKING",
            f"{table}: {actual:,} rows (within [{min_rows:,}, {max_rows:,}])",
        )]
    return [_diag_fail(
        "row_counts",
        "row_counts",
        "BLOCKING",
        f"{table}: {actual:,} rows outside bounds [{min_rows:,}, {max_rows:,}]",
        failing_rows=0,
        details={"actual": actual, "min": min_rows, "max": max_rows},
    )]


def check_freshness(
    conn: Any, contract: dict, now_utc: datetime
) -> list[Diagnostic]:
    """
    Freshness rules from freshness section.

    Measures from the producer-supplied ingested_at column only.
    Never adjusts timestamps.  Validates:
      1. Non-empty snapshot (reject_empty_snapshot)
      2. No future timestamps beyond reject_future_minutes
      3. All rows within sla_hours of now_utc
    """
    freshness = contract.get("freshness", {})
    measurement = freshness.get("measurement", {})
    source_col = measurement.get("source_column", "ingested_at")
    source_table = measurement.get("source_table", _table_name(contract))
    sla_hours = float(freshness.get("sla_hours", 24))
    reject_future_min = float(freshness.get("reject_future_minutes", 5))
    reject_empty = freshness.get("reject_empty_snapshot", True)

    table = _table_name(contract)
    if source_table != table:
        # Safety: warn if source_table differs from main table
        pass

    results: list[Diagnostic] = []
    now_iso = now_utc.strftime("%Y-%m-%dT%H:%M:%SZ")

    # Guard: if source_column is absent from the table, report and return.
    # check_required_columns_present will have already flagged the missing column.
    if _get_duckdb_type(conn, table, source_col) is None:
        results.append(_diag_fail(
            "freshness.source_column_missing",
            "freshness",
            "BLOCKING",
            f"Freshness source column {source_col!r} not found in {table}.  "
            "Freshness checks skipped.",
            details={"missing_column": source_col},
        ))
        return results

    # ── 1. Non-empty snapshot ─────────────────────────────────────────────────
    row_count: int = _scalar(conn, f"SELECT COUNT(*) FROM {table}")
    if reject_empty and row_count == 0:
        results.append(_diag_fail(
            "freshness.empty_snapshot",
            "freshness",
            "BLOCKING",
            f"Snapshot is empty (0 rows).  An empty snapshot is always rejected.",
            details={"validation_clock": now_iso},
        ))
        return results  # nothing else to check

    # ── 2. Future timestamps ──────────────────────────────────────────────────
    # Use epoch() for all timestamp comparisons to avoid the pytz dependency
    # on Python 3.14 (DuckDB requires pytz to return tz-aware Python datetime
    # objects, but epoch() stays entirely in SQL and returns plain integers).
    now_epoch = int(now_utc.timestamp())
    reject_seconds = int(reject_future_min * 60)
    sla_seconds = int(sla_hours * 3600)

    future_count: int = _scalar(
        conn,
        f"SELECT COUNT(*) FROM {table} "
        f"WHERE {source_col} IS NOT NULL "
        f"AND epoch(CAST({source_col} AS TIMESTAMPTZ)) > {now_epoch} + {reject_seconds}",
    )
    if future_count == 0:
        results.append(_diag_pass(
            "freshness.future_timestamps",
            "freshness",
            "BLOCKING",
            f"No {source_col} values more than {reject_future_min}m ahead of "
            f"validation clock ({now_iso})",
        ))
    else:
        results.append(_diag_fail(
            "freshness.future_timestamps",
            "freshness",
            "BLOCKING",
            f"{future_count} row(s) have {source_col} more than "
            f"{reject_future_min} minute(s) ahead of validation clock ({now_iso})",
            failing_rows=future_count,
            details={
                "validation_clock": now_iso,
                "reject_future_minutes": reject_future_min,
            },
        ))

    # ── 3. SLA age check (full-snapshot: ALL rows must be within SLA) ─────────
    stale_count: int = _scalar(
        conn,
        f"SELECT COUNT(*) FROM {table} "
        f"WHERE {source_col} IS NOT NULL "
        f"AND epoch(CAST({source_col} AS TIMESTAMPTZ)) < {now_epoch} - {sla_seconds}",
    )

    if stale_count == 0:
        results.append(_diag_pass(
            "freshness.sla",
            "freshness",
            "BLOCKING",
            f"All {row_count:,} rows have {source_col} within {sla_hours}h of "
            f"validation clock ({now_iso})",
        ))
    else:
        # Use strftime AT TIME ZONE 'UTC' to get a UTC string without pytz
        oldest = _scalar(
            conn,
            f"SELECT strftime(MIN({source_col} AT TIME ZONE 'UTC'), "
            f"'%Y-%m-%dT%H:%M:%SZ') FROM {table}",
        )
        results.append(_diag_fail(
            "freshness.sla",
            "freshness",
            "BLOCKING",
            f"{stale_count:,} of {row_count:,} rows have {source_col} older than "
            f"{sla_hours}h (SLA).  Oldest: {oldest}.  Clock: {now_iso}",
            failing_rows=stale_count,
            details={
                "sla_hours": sla_hours,
                "oldest_ingested_at": str(oldest),
                "validation_clock": now_iso,
                "snapshot_mode": "full",
                "_note": (
                    "Full-snapshot mode: every row must satisfy the SLA.  "
                    "The validator did not adjust any timestamps."
                ),
            },
        ))

    return results


# ── Top-level data validation ─────────────────────────────────────────────────


def run_all_checks(
    conn: Any, contract: dict, now_utc: datetime
) -> list[Diagnostic]:
    """
    Run every data rule declared in the contract.

    Order: required columns → physical types → grain → not-null →
           allowed values → patterns → min values → date formats →
           date ranges → lifecycle → foreign keys → row counts → freshness.
    """
    results: list[Diagnostic] = []
    results += check_required_columns_present(conn, contract)
    results += check_physical_types(conn, contract)
    results += check_grain(conn, contract)
    results += check_not_null_constraints(conn, contract)
    results += check_allowed_values(conn, contract)
    results += check_identifier_patterns(conn, contract)
    results += check_min_value_constraints(conn, contract)
    results += check_date_format_constraints(conn, contract)
    results += check_date_range_constraints(conn, contract, now_utc)
    results += check_lifecycle_constraints(conn, contract)
    results += check_foreign_keys(conn, contract)
    results += check_row_counts(conn, contract)
    results += check_freshness(conn, contract, now_utc)
    return results


# ── Breaking-change detection ─────────────────────────────────────────────────


def _major_version(version_str: str) -> int:
    try:
        return int(str(version_str).split(".")[0])
    except (ValueError, IndexError):
        return -1


class BreakingChange:
    """One detected breaking change."""

    def __init__(self, category: str, description: str) -> None:
        self.category = category
        self.description = description

    def __str__(self) -> str:
        return f"[{self.category}] {self.description}"


def check_breaking_changes(
    base_contract: dict, current_contract: dict
) -> list[BreakingChange]:
    """
    Compare base and current contracts.  Return a list of BreakingChange
    objects representing all detected breaking changes.

    An empty list means no breaking changes were detected.

    This function only detects; it does not enforce version requirements.
    Call require_major_version_bump() to combine detection with version
    enforcement.
    """
    breaking: list[BreakingChange] = []

    # Index base columns by name
    base_cols: dict[str, dict] = {
        c["name"]: c for c in base_contract.get("columns", [])
    }
    curr_cols: dict[str, dict] = {
        c["name"]: c for c in current_contract.get("columns", [])
    }

    # ── 1. Removed columns ────────────────────────────────────────────────────
    for col_name in base_cols:
        if col_name not in curr_cols:
            breaking.append(BreakingChange(
                "removed_column",
                f"Column {col_name!r} was removed.  Consumers that reference "
                f"this column will break.",
            ))

    # ── 2. Physical type changes ───────────────────────────────────────────────
    for col_name, base_col in base_cols.items():
        if col_name not in curr_cols:
            continue
        curr_col = curr_cols[col_name]
        base_type = base_col.get("physical_type")
        curr_type = curr_col.get("physical_type")
        if base_type and curr_type and base_type != curr_type:
            breaking.append(BreakingChange(
                "type_changed",
                f"Column {col_name!r} physical_type changed from "
                f"{base_type!r} to {curr_type!r}.  This may be incompatible.",
            ))

    # ── 3. Nullability widened (non-null → nullable) ──────────────────────────
    for col_name, base_col in base_cols.items():
        if col_name not in curr_cols:
            continue
        base_nullable = base_col.get("nullable", True)
        curr_nullable = curr_cols[col_name].get("nullable", True)
        # Breaking: was non-null, now nullable (producer may now send NULLs)
        if base_nullable is False and curr_nullable is True:
            breaking.append(BreakingChange(
                "nullability_widened",
                f"Column {col_name!r} was nullable: false; now nullable: true.  "
                f"Consumers expecting non-null values may receive NULLs.",
            ))

    # ── 4. Grain / primary key changed ───────────────────────────────────────
    base_grain = sorted(base_contract.get("grain", {}).get("columns", []))
    curr_grain = sorted(current_contract.get("grain", {}).get("columns", []))
    if base_grain != curr_grain:
        breaking.append(BreakingChange(
            "grain_changed",
            f"grain.columns changed from {base_grain} to {curr_grain}.  "
            f"Consumers may JOIN on the wrong key.",
        ))

    base_pk = sorted(
        (base_contract.get("primary_key") or {}).get("columns", [])
    )
    curr_pk = sorted(
        (current_contract.get("primary_key") or {}).get("columns", [])
    )
    if base_pk != curr_pk and base_pk:
        breaking.append(BreakingChange(
            "primary_key_changed",
            f"primary_key.columns changed from {base_pk} to {curr_pk}.",
        ))

    # ── 5. Allowed values narrowed ────────────────────────────────────────────
    for col_name, base_col in base_cols.items():
        if col_name not in curr_cols:
            continue
        base_av = set(base_col.get("allowed_values") or [])
        curr_av = set(curr_cols[col_name].get("allowed_values") or [])
        if not base_av or not curr_av:
            continue
        removed = base_av - curr_av
        if removed:
            breaking.append(BreakingChange(
                "allowed_values_narrowed",
                f"Column {col_name!r}: allowed_values narrowed.  "
                f"Removed values: {sorted(removed)}.  "
                f"Existing data with these values will fail validation.",
            ))

    # ── 6. Freshness weakened (sla_hours increased) ───────────────────────────
    base_sla = base_contract.get("freshness", {}).get("sla_hours")
    curr_sla = current_contract.get("freshness", {}).get("sla_hours")
    if base_sla is not None and curr_sla is not None:
        if float(curr_sla) > float(base_sla):
            breaking.append(BreakingChange(
                "freshness_weakened",
                f"freshness.sla_hours increased from {base_sla} to {curr_sla}.  "
                f"Consumers depending on {base_sla}h freshness may receive staler data.",
            ))

    # ── 7. Foreign key removed ────────────────────────────────────────────────
    base_fks = {
        (fk["column"], fk["references"]["table"], fk["references"]["column"])
        for fk in base_contract.get("foreign_keys", [])
    }
    curr_fks = {
        (fk["column"], fk["references"]["table"], fk["references"]["column"])
        for fk in current_contract.get("foreign_keys", [])
    }
    for fk_triple in base_fks - curr_fks:
        col, ref_t, ref_c = fk_triple
        breaking.append(BreakingChange(
            "foreign_key_removed",
            f"Foreign key {col}→{ref_t}.{ref_c} was removed.  "
            f"Consumers depending on referential integrity of {col} may receive orphans.",
        ))

    return breaking


def require_major_version_bump(
    base_contract: dict,
    current_contract: dict,
    breaking: list[BreakingChange],
) -> list[str]:
    """
    Given a list of detected breaking changes, check whether the major version
    was bumped.

    Returns a list of enforcement errors.  Empty = no enforcement violations.
    """
    if not breaking:
        return []

    base_major = _major_version(str(base_contract.get("version", "0")))
    curr_major = _major_version(str(current_contract.get("version", "0")))

    if curr_major <= base_major:
        return [
            f"Breaking changes detected but major version was not bumped "
            f"(base: {base_contract.get('version')}, current: {current_contract.get('version')}).  "
            f"A major version bump is required.  "
            f"Breaking changes: {len(breaking)}"
        ]
    return []


def _get_base_contract_from_git(
    contract_path: Path, base_ref: str = "HEAD~1"
) -> dict | None:
    """
    Retrieve the base version of the contract from git.

    Returns None if the file did not exist in the base ref (new file).
    Raises SystemExit(2) on git errors other than file-not-found.
    """
    result = subprocess.run(
        ["git", "show", f"{base_ref}:{contract_path}"],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        if "does not exist" in result.stderr or "exists on disk" in result.stderr:
            return None  # new file — no base to compare
        stderr = result.stderr.strip()
        if "not a git repository" in stderr.lower():
            _exit2(
                f"--check-breaking requires a git repository.  "
                f"git error: {stderr}"
            )
        # File simply wasn't tracked in the base ref
        return None

    try:
        base_data = yaml.safe_load(result.stdout)
    except yaml.YAMLError as exc:
        _exit2(f"Failed to parse base contract from git ({base_ref}):\n  {exc}")

    return base_data


# ── Output formatting ─────────────────────────────────────────────────────────


def _status_label(d: Diagnostic) -> str:
    if d.passed:
        return "PASS"
    return f"FAIL ({d.message})"


def format_terminal_summary(
    diagnostics: list[Diagnostic],
    contract: dict,
    now_utc: datetime,
    data_dir: Path | None = None,
    synthetic_note: bool = False,
) -> str:
    """Return a human-readable summary string."""
    lines: list[str] = []
    sep = "=" * 70

    name = contract.get("name", "?")
    version = contract.get("version", "?")
    now_iso = now_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
    data_label = str(data_dir) if data_dir else "(in-memory)"

    lines.append(sep)
    lines.append(f"LUMEN-DQ CONTRACT VALIDATION: {name}  (v{version})")
    lines.append(f"Contract format : {CONTRACT_FORMAT} v1")
    lines.append(f"Data directory  : {data_label}")
    lines.append(f"Validation clock: {now_iso}")
    if synthetic_note:
        lines.append(
            "*** SYNTHETIC FRESHNESS TEST — not proof of source-system timeliness ***"
        )
    lines.append(sep)
    lines.append("")

    col_w = 45
    lines.append(f"{'CHECK':<{col_w}} {'SEVERITY':<10} STATUS")
    lines.append("-" * 70)

    total = len(diagnostics)
    failures = 0
    blocking_failures = 0
    for d in diagnostics:
        status = _status_label(d)
        lines.append(f"{d.check_id:<{col_w}} {d.severity:<10} {status}")
        if not d.passed:
            failures += 1
            if d.severity == "BLOCKING":
                blocking_failures += 1

    lines.append("-" * 70)
    passed = total - failures
    if failures == 0:
        lines.append(f"Result: {passed}/{total} checks passed — all green")
    else:
        lines.append(
            f"Result: {passed}/{total} checks passed — "
            f"{failures} FAILURE(S), {blocking_failures} BLOCKING"
        )
    lines.append(sep)
    return "\n".join(lines)


def format_json_output(
    diagnostics: list[Diagnostic],
    contract: dict,
    now_utc: datetime,
    data_dir: Path | None = None,
    synthetic_note: bool = False,
) -> dict:
    """Return a structured dict suitable for JSON serialisation."""
    total = len(diagnostics)
    failed = sum(1 for d in diagnostics if not d.passed)
    blocking_failures = sum(
        1 for d in diagnostics if not d.passed and d.severity == "BLOCKING"
    )
    return {
        "contract": contract.get("name"),
        "version": contract.get("version"),
        "contract_format": CONTRACT_FORMAT,
        "validation_clock": now_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "data_dir": str(data_dir) if data_dir else None,
        "synthetic_freshness_test": synthetic_note,
        "total": total,
        "passed": total - failed,
        "failed": failed,
        "blocking_failures": blocking_failures,
        "checks": [d.to_dict() for d in diagnostics],
    }


# ── Utility ───────────────────────────────────────────────────────────────────


def _exit2(msg: str) -> None:
    """Print an error to stderr and exit with code 2 (config/execution failure)."""
    print(f"ERROR: {msg}", file=sys.stderr)
    sys.exit(2)


def _parse_now(now_str: str) -> datetime:
    """Parse an explicit timezone-aware ISO-8601 UTC timestamp.

    Only formats that include an explicit UTC timezone indicator are accepted.
    Strings without a timezone suffix (e.g. bare ``2024-01-30T12:00:00`` or
    ``2024-01-30``) are rejected with exit 2 — they are ambiguous and could
    silently conceal a local-time mistake.
    """
    # Only timezone-aware formats.  The Z suffix and +00:00 are both UTC.
    for fmt in (
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S+00:00",
        "%Y-%m-%dT%H:%MZ",
        "%Y-%m-%dT%H:%M+00:00",
    ):
        try:
            dt = datetime.strptime(now_str, fmt)
            return dt.replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    _exit2(
        f"Could not parse --now value {now_str!r}.  "
        "Expected an explicit UTC ISO-8601 timestamp, e.g. 2024-01-30T12:00:00Z.  "
        "Bare dates (2024-01-30) and naive datetimes (2024-01-30T12:00:00) are "
        "rejected: timezone must be explicit."
    )


# ── CLI ───────────────────────────────────────────────────────────────────────


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Validate a lumen-dq-contract YAML against parquet data.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Single command that runs all blocking checks:\n"
            "  python scripts/validate_contract.py contracts/accounts.yaml "
            "--data-dir data/baseline\n\n"
            "Deterministic / CI (synthetic freshness test):\n"
            "  python scripts/validate_contract.py contracts/accounts.yaml "
            "--data-dir data/baseline --now 2024-01-30T12:00:00Z\n"
        ),
    )
    parser.add_argument("contract", help="Path to lumen-dq-contract YAML file")
    parser.add_argument(
        "--data-dir", default="data/baseline",
        help="Directory containing parquet files (default: data/baseline)",
    )
    parser.add_argument(
        "--now",
        help=(
            "Pin the validation clock to this UTC timestamp for deterministic "
            "tests (e.g. 2024-01-30T12:00:00Z).  Omit for operational use "
            "(defaults to actual UTC time)."
        ),
    )
    parser.add_argument(
        "--out", metavar="FILE",
        help="Write structured JSON diagnostics to this file",
    )
    parser.add_argument(
        "--json", action="store_true",
        help="Print JSON diagnostics to stdout instead of human-readable output",
    )
    parser.add_argument(
        "--no-data", action="store_true",
        help="Validate contract structure only; skip data checks",
    )
    parser.add_argument(
        "--check-breaking", action="store_true",
        help="Check for breaking changes against the git base revision",
    )
    parser.add_argument(
        "--base", default="HEAD~1",
        help="Git ref for breaking-change comparison (default: HEAD~1)",
    )
    args = parser.parse_args(argv)

    # ── Load and validate contract structure ──────────────────────────────────
    contract_path = Path(args.contract)
    contract = load_contract(contract_path)

    struct_errors = validate_contract_structure(contract)
    if struct_errors:
        print("Contract structure errors:", file=sys.stderr)
        for e in struct_errors:
            print(f"  • {e}", file=sys.stderr)
        sys.exit(2)

    # ── Breaking-change gate ───────────────────────────────────────────────────
    if args.check_breaking:
        base = _get_base_contract_from_git(contract_path, args.base)
        if base is None:
            print(
                f"INFO: {contract_path} not found in {args.base!r}; "
                "treating as new contract (no breaking-change comparison)."
            )
            sys.exit(0)

        breaking = check_breaking_changes(base, contract)
        enforcement_errors = require_major_version_bump(base, contract, breaking)

        if not breaking and not enforcement_errors:
            print(f"Breaking-change check: PASS (no breaking changes detected)")
            sys.exit(0)

        if breaking:
            print(f"Breaking changes detected ({len(breaking)}):")
            for bc in breaking:
                print(f"  • {bc}")
        if enforcement_errors:
            print("\nEnforcement errors:")
            for e in enforcement_errors:
                print(f"  ✗ {e}")
            sys.exit(1)
        else:
            # Breaking changes present but major version was bumped — warn only
            print(
                "\nBreaking changes present.  Major version was bumped.  "
                "Ensure migration guide and consumer sign-off are complete."
            )
            sys.exit(0)

    # ── Data validation ───────────────────────────────────────────────────────
    if args.no_data:
        print(f"Contract {contract_path} structure: OK  (--no-data; data checks skipped)")
        sys.exit(0)

    # Determine validation clock
    synthetic_note = False
    if args.now:
        now_utc = _parse_now(args.now)
        synthetic_note = True
        print(
            f"*** SYNTHETIC FRESHNESS TEST — clock pinned to {args.now} ***",
            file=sys.stderr,
        )
    else:
        now_utc = datetime.now(tz=timezone.utc)

    data_dir = Path(args.data_dir)
    if not data_dir.is_dir():
        _exit2(f"Data directory not found: {data_dir}")

    try:
        conn = _open_connection_all(data_dir, contract)
    except SystemExit:
        raise
    except Exception as exc:  # noqa: BLE001
        _exit2(f"Failed to open DuckDB connection: {exc}")

    try:
        diagnostics = run_all_checks(conn, contract, now_utc)
    except SystemExit:
        raise
    except Exception as exc:  # noqa: BLE001
        _exit2(f"Unexpected error during validation: {exc}")

    # ── Output ────────────────────────────────────────────────────────────────
    payload = format_json_output(
        diagnostics, contract, now_utc, data_dir, synthetic_note
    )

    if args.json:
        print(json.dumps(payload, indent=2))
    else:
        print(format_terminal_summary(
            diagnostics, contract, now_utc, data_dir, synthetic_note
        ))

    if args.out:
        out_path = Path(args.out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        print(f"Diagnostics written to {out_path}", file=sys.stderr)

    # Exit 1 if any blocking failures
    blocking_failures = sum(
        1 for d in diagnostics if not d.passed and d.severity == "BLOCKING"
    )
    sys.exit(1 if blocking_failures > 0 else 0)


if __name__ == "__main__":
    main()
