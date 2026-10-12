# Rollback Instructions

## Incident Summary

**What happened:** The `branch_reference` table in the production DQ pipeline
contained duplicate `branch_id` values (47 rows, 5 duplicated IDs: 1, 5, 12, 23, 37).
A join in the CFO weekly packet query fanned out `daily_balances` rows for the
affected branches by 2×, silently inflating average customer balance figures.

**Duration:** 11 days — 2024-01-11 through 2024-01-21.

**Detection:** Manual reconciliation by a data analyst. Automated gate was not in place.

---

## Step 1 — Diagnose the duplicate branch_ids

Run the grain check against the current `branch_reference`:

```bash
python -m lumen_dq.run_checks --data-dir data/baseline
```

Look for failures in the `grain_uniqueness:branch_reference[branch_id]` and
`branch_count:branch_reference` rows.

Or run the diagnostic SQL directly in DuckDB:

```sql
-- Identify duplicate branch_ids
SELECT branch_id, COUNT(*) AS cnt
FROM branch_reference
GROUP BY branch_id
HAVING COUNT(*) > 1
ORDER BY branch_id;
```

---

## Step 2 — Restore branch_reference to a unique-grain state

In DuckDB (or your production database), remove duplicate rows while keeping
the canonical (first-inserted) row for each `branch_id`:

```sql
-- Preview what would be deleted
SELECT *
FROM branch_reference
WHERE branch_id IN (
    SELECT branch_id FROM branch_reference GROUP BY branch_id HAVING COUNT(*) > 1
)
ORDER BY branch_id;

-- Delete duplicates — keep one row per branch_id
-- DuckDB: use rowid
DELETE FROM branch_reference
WHERE rowid NOT IN (
    SELECT MIN(rowid)
    FROM branch_reference
    GROUP BY branch_id
);

-- Verify: must return 42 and 42
SELECT COUNT(*) AS total_rows, COUNT(DISTINCT branch_id) AS unique_branches
FROM branch_reference;
```

---

## Step 3 — Re-run the quality gate

```bash
# Regenerate baseline data (uses the deterministic generator; always produces clean data)
python -m lumen_dq.generate --output-dir data/baseline

# All checks must pass
python -m lumen_dq.run_checks --data-dir data/baseline --strict

# YAML contract must pass
python -m lumen_dq.validate_contract contracts/accounts.yml --data-dir data/baseline

# Full test suite must pass
pytest tests/ -v --tb=short
```

All steps must exit 0 before proceeding.

---

## Step 4 — Recompute the CFO report for the affected window

Re-run `models/reports/cfo_weekly_packet.sql` for balance dates
2024-01-11 through 2024-01-21 using the corrected `branch_reference`.

The corrected `avg_customer_balance_cents` figures will be lower than the
inflated figures that were reported. Save the corrected output for audit purposes.

---

## Step 5 — Notify stakeholders

Document the following in the audit log:
- Dates of incorrect figures: 2024-01-11 → 2024-01-21
- Root cause: non-unique `branch_reference` (5 duplicate `branch_id` rows)
- Approximate overstatement: see `evidence/` for exact figures from the replay test
- Corrected figures: attach the re-run report output
- Fix applied: removed duplicate rows; added grain check as a blocking gate

---

## Step 6 — Confirm the gate is now in place

The quality gate blocks future occurrences. Verify:

1. The GitHub Actions `quality_gate.yml` workflow runs on every push.
2. The `check_grain_uniqueness(branch_reference, [branch_id])` check is in the
   `_BASELINE_CHECKS` list in `src/lumen_dq/checks.py`.
3. The `check_branch_count` check is also present.
4. Branch protection on `main` requires the `DQ checks + tests` job to pass.

---

## Restore from a known-good snapshot

The synthetic-data generator is deterministic (seed=42). Running it always
produces a clean 42-row `branch_reference`. To restore a known-good baseline:

```bash
python -m lumen_dq.generate --output-dir data/baseline
python -m lumen_dq.run_checks --strict
```

This does not restore production data — it restores the synthetic demo baseline.
For a production rollback, use the last clean snapshot from your data warehouse
audit log or the last green staging artifact in GitHub Actions.
