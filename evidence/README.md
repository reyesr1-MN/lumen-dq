# evidence/

This directory holds generated evidence from DQ check runs.

| File pattern | Source | Contents |
|---|---|---|
| `demo_<timestamp>.txt` | `scripts/record_demo.sh` | Full terminal recording of the demo run |
| `evidence_<timestamp>.json` | `scripts/record_evidence.py` | Structured JSON: SHA, timestamps, per-check results, row counts |
| `junit.xml` | pytest `--junitxml` | JUnit-format test results (consumed by GitHub Actions) |

Generated files are excluded from git (see `.gitignore`).
The `.gitkeep` file ensures the directory exists in a fresh clone.

## Interpreting evidence files

A clean run produces a `demo_*.txt` with all checks showing `PASS` and
all pytest tests green.

The incident replay section shows:
- `grain_uniqueness:branch_reference[branch_id]` → **FAIL** (5 duplicate rows)
- `branch_count:branch_reference`                → **FAIL** (47 rows, not 42)
- Inflated bank-wide avg > correct bank-wide avg (positive overstatement %)
