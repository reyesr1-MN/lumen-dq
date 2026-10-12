"""
DuckDB connection helpers for Lumen Community Bank DQ demo.

All four grain tables are registered as in-memory views over Parquet files.
No data is modified; the connection is read-only from the application's perspective.
"""

from __future__ import annotations

from pathlib import Path

import duckdb

_DEFAULT_DATA_DIR = Path(__file__).parent.parent.parent / "data" / "baseline"

_TABLES = ("branch_reference", "customers", "accounts", "daily_balances")


def open_connection(data_dir: Path | str | None = None) -> duckdb.DuckDBPyConnection:
    """
    Open an in-memory DuckDB connection with views over the four baseline tables.

    Parameters
    ----------
    data_dir:
        Directory that contains the four ``.parquet`` files.
        Defaults to ``data/baseline`` relative to the repository root.

    Returns
    -------
    duckdb.DuckDBPyConnection
        Connection with views: branch_reference, customers, accounts, daily_balances.
    """
    data_dir = Path(data_dir) if data_dir is not None else _DEFAULT_DATA_DIR

    conn = duckdb.connect(":memory:")
    for table in _TABLES:
        path = data_dir / f"{table}.parquet"
        conn.execute(
            f"CREATE VIEW {table} AS SELECT * FROM read_parquet('{path}')"
        )
    return conn
