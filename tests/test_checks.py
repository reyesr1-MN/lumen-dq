"""
Unit tests for individual DQ check functions (Layers 1–4).

A small in-memory DuckDB table (the `mem` fixture) is used for isolated
unit tests so they do not depend on the parquet files.
"""

import duckdb
import pytest

from lumen_dq.checks import (
    check_allowed_values,
    check_branch_count,
    check_foreign_key,
    check_non_negative,
    check_not_null,
    check_row_count,
    run_all_checks,
)


# ── Isolated in-memory fixtures ────────────────────────────────────────────────


@pytest.fixture()
def mem():
    """
    Tiny DuckDB tables for unit testing individual check functions.

    Table `t`: has a NULL in `val`, a negative in `amt`, and id=3 is absent from `ref`.
    Table `ref`: reference table with ids 1 and 2 only.
    """
    conn = duckdb.connect(":memory:")
    conn.execute("""
        CREATE TABLE t (id INTEGER, val VARCHAR, amt INTEGER);
        INSERT INTO t VALUES (1, 'A', 100), (2, 'B', -50), (3, NULL, 300);

        CREATE TABLE ref (id INTEGER);
        INSERT INTO ref VALUES (1), (2);
    """)
    return conn


# ── Layer 1: check_not_null ────────────────────────────────────────────────────


def test_not_null_detects_null(mem):
    r = check_not_null(mem, "t", "val")
    assert not r.passed
    assert r.failing_rows == 1


def test_not_null_passes_on_clean_column(mem):
    r = check_not_null(mem, "t", "id")
    assert r.passed
    assert r.failing_rows == 0


# ── Layer 2: check_grain_uniqueness tested in test_grain.py ───────────────────


# ── Layer 3: check_foreign_key ─────────────────────────────────────────────────


def test_foreign_key_detects_orphan(mem):
    """id=3 in `t` does not exist in `ref`."""
    r = check_foreign_key(mem, "t", "id", "ref", "id")
    assert not r.passed
    assert r.failing_rows == 1


def test_foreign_key_passes_when_all_matched():
    conn = duckdb.connect(":memory:")
    conn.execute("""
        CREATE TABLE parent (pid INTEGER);
        INSERT INTO parent VALUES (10), (20);
        CREATE TABLE child (pid INTEGER);
        INSERT INTO child VALUES (10), (20);
    """)
    r = check_foreign_key(conn, "child", "pid", "parent", "pid")
    assert r.passed


def test_foreign_key_ignores_nulls():
    """NULL FK values must not be counted as orphans."""
    conn = duckdb.connect(":memory:")
    conn.execute("""
        CREATE TABLE parent (pid INTEGER);
        INSERT INTO parent VALUES (1);
        CREATE TABLE child (pid INTEGER);
        INSERT INTO child VALUES (1), (NULL);
    """)
    r = check_foreign_key(conn, "child", "pid", "parent", "pid")
    assert r.passed, "NULL FK should not be treated as an orphan"


# ── Layer 4: business rules ────────────────────────────────────────────────────


def test_row_count_passes(mem):
    r = check_row_count(mem, "t", 3)
    assert r.passed


def test_row_count_fails_on_wrong_count(mem):
    r = check_row_count(mem, "t", 5)
    assert not r.passed


def test_row_count_tolerance_passes(mem):
    r = check_row_count(mem, "t", 4, tolerance=1)
    assert r.passed


def test_non_negative_detects_negative(mem):
    r = check_non_negative(mem, "t", "amt")
    assert not r.passed
    assert r.failing_rows == 1


def test_non_negative_passes_on_clean_data():
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE TABLE pos (n INTEGER); INSERT INTO pos VALUES (0),(1),(999)")
    r = check_non_negative(conn, "pos", "n")
    assert r.passed


def test_allowed_values_fails_with_unexpected(mem):
    """val='B' is not in the allowed list ['A']."""
    r = check_allowed_values(mem, "t", "val", ["A"])
    assert not r.passed
    assert r.failing_rows == 1  # only 'B'; NULL is not counted by NOT IN


def test_allowed_values_passes_with_complete_list(mem):
    r = check_allowed_values(mem, "t", "val", ["A", "B"])
    assert r.passed  # NULL is not matched by NOT IN, so not counted as a failure


def test_branch_count_passes_on_valid_table():
    conn = duckdb.connect(":memory:")
    conn.execute("""
        CREATE TABLE br (branch_id INTEGER);
        INSERT INTO br SELECT range + 1 FROM range(42);
    """)
    r = check_branch_count(conn, "br")
    assert r.passed


def test_branch_count_fails_with_duplicates():
    conn = duckdb.connect(":memory:")
    conn.execute("""
        CREATE TABLE br (branch_id INTEGER);
        INSERT INTO br SELECT range + 1 FROM range(42);
        INSERT INTO br VALUES (1), (2), (3), (4), (5);  -- 5 duplicates → 47 rows
    """)
    r = check_branch_count(conn, "br")
    assert not r.passed


def test_branch_count_fails_with_too_few():
    conn = duckdb.connect(":memory:")
    conn.execute("""
        CREATE TABLE br (branch_id INTEGER);
        INSERT INTO br SELECT range + 1 FROM range(40);  -- only 40 branches
    """)
    r = check_branch_count(conn, "br")
    assert not r.passed


# ── Full baseline suite ────────────────────────────────────────────────────────


def test_run_all_checks_passes_on_baseline(baseline_conn):
    """The complete check suite must report zero failures on clean baseline data."""
    results = run_all_checks(baseline_conn)
    failures = [r for r in results if not r.passed]
    assert not failures, (
        "Baseline failed unexpectedly:\n"
        + "\n".join(f"  {r.name}: {r.message}" for r in failures)
    )
