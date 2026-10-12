#!/usr/bin/env bash
# scripts/record_demo.sh
#
# Records a complete demonstration of the Lumen Community Bank DQ incident.
# Output is written to evidence/demo_<timestamp>.txt
#
# Usage:
#   bash scripts/record_demo.sh          # from repo root
#   make demo

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
EVIDENCE_DIR="$REPO_ROOT/evidence"
TIMESTAMP=$(date -u +%Y%m%dT%H%M%SZ)
LOG="$EVIDENCE_DIR/demo_${TIMESTAMP}.txt"

mkdir -p "$EVIDENCE_DIR"

# Resolve python — prefer the venv, fall back to python3
VENV_PY="$REPO_ROOT/.venv/bin/python"
VENV_PT="$REPO_ROOT/.venv/bin/pytest"
if [ -f "$VENV_PY" ]; then
  PY="$VENV_PY"
  PT="$VENV_PT"
elif command -v python3 &>/dev/null; then
  PY="python3"
  PT="python3 -m pytest"
else
  PY="python"
  PT="python -m pytest"
fi

run_step() {
    local title="$1"
    shift
    echo ""
    echo "── $title ──────────────────────────────────────────────────────────"
    "$@"
}

{
  echo "════════════════════════════════════════════════════════════════════"
  echo "  Lumen Community Bank — Data Quality Demonstration"
  echo "  Timestamp : $TIMESTAMP"
  echo "  Python    : $("$PY" --version 2>&1)"
  echo "  DuckDB    : $("$PY" -c 'import duckdb; print(duckdb.__version__)')"
  echo "════════════════════════════════════════════════════════════════════"

  cd "$REPO_ROOT"

  run_step "Step 1: Generate deterministic baseline data (seed=42)"
  "$PY" -m lumen_dq.generate --output-dir data/baseline

  run_step "Step 2: Run layered DQ checks — expect all PASS"
  "$PY" -m lumen_dq.run_checks --data-dir data/baseline

  run_step "Step 3: Validate accounts YAML data contract — expect PASS"
  "$PY" -m lumen_dq.validate_contract contracts/accounts.yml --data-dir data/baseline

  run_step "Step 4: Run full pytest suite"
  "$PT" tests/ -v --tb=short

  run_step "Step 5: Replay the incident (buggy branch_reference)"
  "$PY" -m lumen_dq.replay_incident

  echo ""
  echo "════════════════════════════════════════════════════════════════════"
  echo "  Demonstration complete."
  echo "  Evidence written to: $LOG"
  echo "════════════════════════════════════════════════════════════════════"
  echo ""
} 2>&1 | tee "$LOG"

echo "Log saved to $LOG"
