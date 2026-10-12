-- Report model: average customer balance by branch and date
--
-- Metric definition (precise)
-- ─────────────────────────────────────────────────────────────────────────────
-- Step 1 – Customer-branch total:
--   For each (branch_id, customer_id, balance_date), SUM all account balances.
--   A customer with multiple accounts at the same branch is counted ONCE
--   within that branch on a given date.
--
-- Step 2 – Branch average:
--   For each (branch_id, balance_date), AVG those customer totals across all
--   customers who held at least one account at the branch on that date.
--
-- Output grain: (branch_id, balance_date)
-- Expected rows: 42 branches × 30 dates = 1,260
--
-- IMPORTANT: The `accounts` table carries branch_id as a column (FK).
-- Do NOT re-join to branch_reference inside this query.
-- A duplicate in branch_reference would fan out rows here, silently inflating
-- every SUM and the subsequent AVG — that was the incident.
-- Run check_grain_uniqueness(branch_reference, [branch_id]) before executing.
-- ─────────────────────────────────────────────────────────────────────────────

WITH customer_branch_totals AS (
    SELECT
        a.branch_id,
        a.customer_id,
        db.balance_date,
        SUM(db.closing_balance_cents)   AS customer_branch_total_cents
    FROM  daily_balances db
    JOIN  accounts a ON db.account_id = a.account_id
    GROUP BY
        a.branch_id,
        a.customer_id,
        db.balance_date
)
SELECT
    branch_id,
    balance_date,
    COUNT(DISTINCT customer_id)         AS customer_count,
    AVG(customer_branch_total_cents)    AS avg_customer_balance_cents,
    SUM(customer_branch_total_cents)    AS total_branch_balance_cents
FROM  customer_branch_totals
GROUP BY
    branch_id,
    balance_date
ORDER BY
    branch_id,
    balance_date
