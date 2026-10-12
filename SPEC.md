# Lumen Community Bank — Data Specification & Implementation Checklist

> **Incident recap.**
> A join to a non-unique `branch_reference` table silently multiplied
> `daily_balances` rows, overstating average customer balances in the CFO's
> weekly packet for eleven consecutive days.
> This repository contains deterministic synthetic data, layered DQ checks,
> an executable YAML contract, and a full incident replay test.

---

## 1. Grain Definitions

| Table | Grain (primary key) | Constraint |
|---|---|---|
| `customers` | `customer_id` | One row per customer. |
| `accounts` | `account_id` | One row per account; exactly one `customer_id` and one `branch_id` per account. |
| `daily_balances` | `(account_id, balance_date)` | One row per account per calendar date. |
| `branch_reference` | `branch_id` | One **current** row per branch; exactly 42 rows total. No slowly-changing dimension. |

---

## 2. Baseline Sizes

| Table | Rows |
|---|---|
| `customers` | 1,000 |
| `accounts` | 1,500 |
| `daily_balances` | 45,000 (1,500 accounts × 30 dates) |
| `branch_reference` | 42 |

- Balance dates: 2024-01-01 through 2024-01-30 (30 consecutive days, UTC).
- Money representation: **integer cents** (`balance_cents BIGINT`). Example: `500000` = $5,000.00.
- All timestamps treated as UTC.
- All identifiers and names are **fictional** (no real persons, banks, or locations).

---

## 3. Report Metric — Precise Definition

### Branch-level metric

For each `(branch_id, balance_date)`:

1. **Customer-branch total**: `SUM(balance_cents)` over all accounts owned by a
   given `customer_id` at a given `branch_id` on a given `balance_date`.
   A customer with multiple accounts at the same branch is counted **once**
   within that branch.

2. **Branch average**: `AVG` of those customer totals across all customers in
   the branch on that date.

```sql
WITH customer_branch_totals AS (
    SELECT  a.branch_id,
            a.customer_id,
            db.balance_date,
            SUM(db.balance_cents) AS customer_branch_total_cents
    FROM    daily_balances db
    JOIN    accounts a ON db.account_id = a.account_id
    GROUP BY a.branch_id, a.customer_id, db.balance_date
)
SELECT  branch_id,
        balance_date,
        COUNT(DISTINCT customer_id)         AS customer_count,
        AVG(customer_branch_total_cents)    AS avg_customer_balance_cents
FROM    customer_branch_totals
GROUP BY branch_id, balance_date
```

### Bank-wide metric

Computed at **customer-date grain**, NOT by averaging branch averages
(which would introduce bias when branch customer counts differ).

```sql
WITH customer_date_totals AS (
    SELECT  a.customer_id,
            db.balance_date,
            SUM(db.balance_cents) AS customer_total_cents
    FROM    daily_balances db
    JOIN    accounts a ON db.account_id = a.account_id
    GROUP BY a.customer_id, db.balance_date
)
SELECT  balance_date,
        COUNT(DISTINCT customer_id)     AS customer_count,
        AVG(customer_total_cents)       AS avg_customer_balance_cents
FROM    customer_date_totals
GROUP BY balance_date
```

---

## 4. Incident — Root Cause

The production CFO query joined `daily_balances → accounts → branch_reference`
to obtain branch metadata. At the time of the incident, `branch_reference`
contained **duplicate `branch_id` rows** (5 branch IDs each appeared twice).

A `JOIN … ON a.branch_id = br.branch_id` against a non-unique `branch_reference`
fans out every `daily_balances` row for the affected branches by a factor equal
to the number of duplicate rows. The `SUM` and subsequent `AVG` operate on
multiplied rows, producing inflated figures with **no runtime error**.

**Detection gate (this repo):** `check_grain_uniqueness(branch_reference, [branch_id])`
and `check_branch_count(branch_reference)` must PASS before the report executes.

---

## 5. Invalid Fixture Catalog

| Fixture | Generator function | Violation |
|---|---|---|
| Buggy `branch_reference` | `generate_buggy_branch_reference` | 5 duplicate `branch_id` values (47 rows, 42 unique) |
| Duplicate `daily_balances` | `generate_invalid_daily_balances` | 10 duplicate `(account_id, balance_date)` pairs |
| Orphan-FK `accounts` | `generate_invalid_accounts_orphan_fk` | 5 accounts with `customer_id = 'CUST-XXXX'` (non-existent) |

---

## 6. DQ Check Layers

| Layer | What it checks |
|---|---|
| 1 — Schema | Non-null constraints on every PK and FK column |
| 2 — Grain | PK uniqueness on all four tables |
| 3 — Referential | FK relationships: `accounts→customers`, `accounts→branch_reference`, `daily_balances→accounts` |
| 4 — Business | Exactly 42 branch rows, correct row counts, `balance_cents ≥ 0`, allowed `account_type` values |
| 5 — Metric | No NULLs in computed report columns |

---

## 7. Implementation Checklist

- [x] Deterministic synthetic-data generator (`src/lumen_dq/generate.py`, seed=42)
- [x] Valid baseline parquet files (`data/baseline/`, generated at runtime)
- [x] Invalid fixture generators (three fixture types, in `generate.py`)
- [x] SQL deposit staging models (`models/deposits/`)
- [x] SQL report models with precise metric definition (`models/reports/`)
- [x] Layered DQ check functions (`src/lumen_dq/checks.py`)
- [x] CLI check runner (`src/lumen_dq/run_checks.py`)
- [x] YAML data contract for `accounts` (`contracts/accounts.yml`)
- [x] Executable contract validator (`src/lumen_dq/validate_contract.py`)
- [x] Incident replay module (`src/lumen_dq/replay_incident.py`)
- [x] Grain tests (`tests/test_grain.py`)
- [x] Check-function unit tests (`tests/test_checks.py`)
- [x] Contract validator tests (`tests/test_contract.py`)
- [x] Metric correctness tests (`tests/test_metric.py`)
- [x] Incident replay integration test (`tests/test_incident_replay.py`)
- [x] GitHub Actions quality gate (`quality_gate.yml`)
- [x] GitHub Actions staging promotion (`staging_promotion.yml`)
- [x] Rollback instructions (`scripts/rollback.md`)
- [x] Demo recording script (`scripts/record_demo.sh`)
- [x] Evidence directory (`evidence/`)
