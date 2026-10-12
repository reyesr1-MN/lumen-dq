"""
Layer 4 – Completeness and business rule tests.

Checks:
  account/date coverage   — every account has a balance for every date
  account lifecycle       — closed_date ↔ account_status consistency
  balance movement        — abs(curr − prev) > 3 × abs(prev) (exact integer)
  zero-baseline movement  — 0 → nonzero (undefined %, BLOCKING)

Boundary conditions tested with hand-crafted in-memory fixtures:
  prev=100, curr=400  → exactly 300%  → PASS
  prev=100, curr=401  → 301%           → FAIL
  prev=-100, curr=-401 → 301% abs      → FAIL
  prev=0,   curr=0   → no change       → PASS
  prev=0,   curr≠0   → zero baseline   → ZERO_BASELINE_MOVEMENT FAIL
  first observation  → no comparison   → always PASS
  date gap           → no comparison   → completeness failure, not movement alert
"""

from __future__ import annotations

from datetime import date, datetime, timezone

import duckdb
import pandas as pd
import pytest

from lumen_dq.validators import (
    Severity,
    check_account_date_completeness,
    check_account_lifecycle,
    check_balance_movement,
)
from lumen_dq.generate import generate_scenario
from lumen_dq.db import open_connection


# ── Scenario fixtures ──────────────────────────────────────────────────────────


@pytest.fixture(scope="module")
def balance_spike_conn(tmp_path_factory):
    """daily_balances with a balance spike exceeding 300%."""
    d = generate_scenario("balance_spike", tmp_path_factory.mktemp("s_bs"))
    return open_connection(d)


@pytest.fixture(scope="module")
def missing_date_conn(tmp_path_factory):
    """daily_balances with a missing balance date for an account."""
    d = generate_scenario("missing_balance_date", tmp_path_factory.mktemp("s_md"))
    return open_connection(d)


# ── Hand-crafted in-memory fixtures ───────────────────────────────────────────

_INGESTED_AT = datetime(2024, 1, 30, 12, 0, 0, tzinfo=timezone.utc)


def _base_conn_with_balances(rows: list[tuple]) -> duckdb.DuckDBPyConnection:
    """
    Create a minimal in-memory connection with 1 account and the given
    (account_id, balance_date, closing_balance_cents) rows.

    'rows' must be in ascending date order for the window function to be useful.
    """
    conn = duckdb.connect()
    conn.execute("""
        CREATE TABLE branch_reference (
            branch_id INTEGER, branch_name VARCHAR, city VARCHAR, state_code VARCHAR
        )
    """)
    conn.execute("INSERT INTO branch_reference VALUES (1,'Alpha','Springfield','IL')")

    conn.execute("""
        CREATE TABLE customers (
            customer_id VARCHAR, customer_name VARCHAR, created_date DATE
        )
    """)
    conn.execute("INSERT INTO customers VALUES ('C-001','Alice','2020-01-01')")

    conn.execute("""
        CREATE TABLE accounts (
            account_id VARCHAR, customer_id VARCHAR, branch_id INTEGER,
            product_type VARCHAR, account_status VARCHAR, currency VARCHAR,
            opened_date DATE, closed_date DATE, ingested_at TIMESTAMP
        )
    """)
    conn.execute(
        "INSERT INTO accounts VALUES ('A-001','C-001',1,'CHECKING','ACTIVE','USD',"
        "'2020-01-02',NULL,'2024-01-30 12:00:00')"
    )

    conn.execute("""
        CREATE TABLE daily_balances (
            account_id VARCHAR, balance_date DATE, closing_balance_cents BIGINT
        )
    """)
    for acct_id, bal_date, cents in rows:
        conn.execute(
            "INSERT INTO daily_balances VALUES (?,?,?)",
            [acct_id, bal_date, cents],
        )
    return conn


# ── Positive: baseline passes completeness and lifecycle ───────────────────────


class TestCompletenessBaseline:
    def test_account_date_completeness_passes(self, baseline_conn):
        r = check_account_date_completeness(baseline_conn)
        assert r.passed, r.observed

    def test_layer_is_4(self, baseline_conn):
        r = check_account_date_completeness(baseline_conn)
        assert r.layer == 4


class TestLifecycleBaseline:
    def test_lifecycle_passes(self, baseline_conn):
        r = check_account_lifecycle(baseline_conn)
        assert r.passed, r.observed


# ── Positive: no spikes in baseline ──────────────────────────────────────────


class TestBalanceMovementBaseline:
    def test_no_spike_on_baseline(self, baseline_conn):
        """Baseline balances are a multiplicative random walk with σ=0.08;
        daily changes never exceed ~27%, well below the 300% threshold."""
        spike, zero = check_balance_movement(baseline_conn)
        assert spike.passed, spike.observed

    def test_no_zero_baseline_on_baseline(self, baseline_conn):
        spike, zero = check_balance_movement(baseline_conn)
        assert zero.passed, zero.observed

    def test_returns_two_results(self, baseline_conn):
        results = check_balance_movement(baseline_conn)
        assert len(results) == 2

    def test_check_ids(self, baseline_conn):
        spike, zero = check_balance_movement(baseline_conn)
        assert spike.check_id == "L4.balance_spike"
        assert zero.check_id  == "L4.zero_baseline_movement"


# ── Negative: missing balance date ───────────────────────────────────────────


class TestMissingBalanceDate:
    def test_completeness_fails(self, missing_date_conn):
        r = check_account_date_completeness(missing_date_conn)
        assert not r.passed, "Expected FAIL — at least one date is missing"

    def test_missing_count_positive(self, missing_date_conn):
        r = check_account_date_completeness(missing_date_conn)
        assert r.failing_rows > 0

    def test_severity_is_blocking(self, missing_date_conn):
        r = check_account_date_completeness(missing_date_conn)
        assert r.severity == Severity.BLOCKING

    def test_pipeline_health_mentions_gap(self, missing_date_conn):
        r = check_account_date_completeness(missing_date_conn)
        assert not r.passed
        assert "gap" in r.pipeline_health.lower() or "missing" in r.pipeline_health.lower()


# ── Negative: balance spike ───────────────────────────────────────────────────


class TestBalanceSpike:
    def test_spike_detected(self, balance_spike_conn):
        spike, _ = check_balance_movement(balance_spike_conn)
        assert not spike.passed, "Expected FAIL — a 400%+ spike was injected"

    def test_spike_count_positive(self, balance_spike_conn):
        spike, _ = check_balance_movement(balance_spike_conn)
        assert spike.failing_rows > 0

    def test_affected_keys_populated(self, balance_spike_conn):
        spike, _ = check_balance_movement(balance_spike_conn)
        assert not spike.passed
        assert spike.affected_keys

    def test_affected_key_has_account_and_date(self, balance_spike_conn):
        spike, _ = check_balance_movement(balance_spike_conn)
        assert not spike.passed
        key = spike.affected_keys[0]
        assert "account_id" in key
        assert "balance_date" in key
        assert "prev_cents" in key
        assert "curr_cents" in key

    def test_severity_is_blocking(self, balance_spike_conn):
        spike, _ = check_balance_movement(balance_spike_conn)
        assert spike.severity == Severity.BLOCKING

    def test_pipeline_health_explains_investigation(self, balance_spike_conn):
        spike, _ = check_balance_movement(balance_spike_conn)
        assert not spike.passed
        assert "investigation" in spike.pipeline_health.lower()
        assert "does not" in spike.pipeline_health.lower() or "not by itself" in spike.pipeline_health.lower()


# ── Boundary condition tests (exact integer comparisons) ─────────────────────


class TestBalanceMovementBoundary:
    """
    Rule: ABS(curr − prev) > 3 × ABS(prev)

    These tests use hand-crafted single-account, two-consecutive-date fixtures
    to verify the exact boundary without relying on the generator.
    """

    def _conn(self, prev: int, curr: int) -> duckdb.DuckDBPyConnection:
        return _base_conn_with_balances([
            ("A-001", date(2024, 1, 1), prev),
            ("A-001", date(2024, 1, 2), curr),
        ])

    def test_exactly_300_pct_does_not_fail(self):
        """100 → 400 : ABS(300) = 3×100 → NOT > 3× → PASS."""
        spike, _ = check_balance_movement(self._conn(100, 400))
        assert spike.passed, f"Expected PASS for 100→400 (exactly 300%): {spike.observed}"

    def test_301_pct_fails(self):
        """100 → 401 : ABS(301) > 3×100 → FAIL."""
        spike, _ = check_balance_movement(self._conn(100, 401))
        assert not spike.passed, "Expected FAIL for 100→401 (301%)"

    def test_negative_exactly_300_pct_passes(self):
        """-100 → -400 : ABS(300) = 3×100 → PASS."""
        spike, _ = check_balance_movement(self._conn(-100, -400))
        assert spike.passed, "Expected PASS for -100→-400"

    def test_negative_301_pct_fails(self):
        """-100 → -401 : ABS(301) > 3×100 → FAIL."""
        spike, _ = check_balance_movement(self._conn(-100, -401))
        assert not spike.passed, "Expected FAIL for -100→-401"

    def test_zero_to_zero_passes(self):
        """0 → 0: no movement, prev=0 and curr=0 → neither spike nor zero_baseline."""
        spike, zero = check_balance_movement(self._conn(0, 0))
        assert spike.passed
        assert zero.passed

    def test_zero_to_nonzero_is_zero_baseline(self):
        """0 → 500: undefined %, flagged as ZERO_BASELINE_MOVEMENT."""
        spike, zero = check_balance_movement(self._conn(0, 500))
        assert zero.passed is False
        assert zero.check_id == "L4.zero_baseline_movement"

    def test_zero_to_nonzero_is_not_a_spike(self):
        """The spike check must NOT fire on 0→nonzero; only zero_baseline fires."""
        spike, zero = check_balance_movement(self._conn(0, 500))
        assert spike.passed  # spike check ignores zero-prev rows

    def test_zero_baseline_severity_is_blocking(self):
        _, zero = check_balance_movement(self._conn(0, 500))
        assert zero.severity == Severity.BLOCKING


# ── First observation and date gap ────────────────────────────────────────────


class TestFirstObservationAndGap:
    def test_first_observation_not_compared(self):
        """A single row per account must never trigger a movement alert."""
        conn = _base_conn_with_balances([
            ("A-001", date(2024, 1, 1), 50_000),
        ])
        spike, zero = check_balance_movement(conn)
        assert spike.passed
        assert zero.passed

    def test_gap_is_not_compared_as_consecutive(self):
        """
        2024-01-01 → 2024-01-03 is a two-day gap; the movement check must
        not compare them as consecutive.  Only a completeness failure should
        arise, not a false spike alert.
        """
        # 100 → 401 across a GAP: if we naively compared them it would look like
        # a 301% spike.  With gap filtering it must be silent at the movement layer.
        conn = _base_conn_with_balances([
            ("A-001", date(2024, 1, 1), 100),
            ("A-001", date(2024, 1, 3), 401),  # gap: no 2024-01-02 row
        ])
        spike, zero = check_balance_movement(conn)
        # The movement check does NOT compare across the gap
        assert spike.passed, (
            "Movement check compared across a date gap — it must skip non-consecutive dates"
        )

    def test_gap_produces_completeness_failure(self):
        """Same data as above: the completeness check must detect the gap."""
        conn = _base_conn_with_balances([
            ("A-001", date(2024, 1, 1), 100),
            ("A-001", date(2024, 1, 3), 401),
        ])
        r = check_account_date_completeness(conn)
        assert not r.passed, "Expected completeness FAIL for the date gap"


# ── Alert documentation ───────────────────────────────────────────────────────


class TestAlertDocumentation:
    def test_spike_pipeline_health_says_not_proof_of_corruption(
        self, balance_spike_conn
    ):
        """
        The spec requires the pipeline_health text to explain that a movement
        alert is not proof of corruption.
        """
        spike, _ = check_balance_movement(balance_spike_conn)
        assert not spike.passed
        health = spike.pipeline_health.lower()
        assert (
            "not by itself" in health
            or "does not" in health
            or "not prove" in health
        ), (
            f"pipeline_health must state the alert is not proof of corruption; "
            f"got: {spike.pipeline_health!r}"
        )

    def test_zero_baseline_pipeline_health_explains_blocking_reason(self):
        conn = _base_conn_with_balances([
            ("A-001", date(2024, 1, 1), 0),
            ("A-001", date(2024, 1, 2), 100),
        ])
        _, zero = check_balance_movement(conn)
        assert not zero.passed
        assert "undefined" in zero.pipeline_health.lower()
        assert "blocking" in zero.pipeline_health.lower()
