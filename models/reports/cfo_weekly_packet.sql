-- Report model: CFO Weekly Packet
--
-- Combines the branch-level and bank-wide average customer balance metrics
-- for each (branch_id, balance_date) in a single output.
--
-- Pre-condition (MUST pass before this query executes)
-- ─────────────────────────────────────────────────────────────────────────────
--   check_grain_uniqueness(branch_reference, [branch_id]) → 0 duplicates
--   check_branch_count(branch_reference)                  → exactly 42 rows
--
-- Without these gates the buggy join inside `branch_level` would silently
-- multiply daily_balances rows for every branch that had duplicates, overstating
-- customer balances.  The incident ran for 11 days (2024-01-11 → 2024-01-21).
-- ─────────────────────────────────────────────────────────────────────────────
--
-- Output grain: (branch_id, balance_date)
-- Expected rows: 42 × 30 = 1,260
-- ─────────────────────────────────────────────────────────────────────────────

WITH customer_branch_totals AS (
    -- Step 1a: one row per (branch_id, customer_id, balance_date)
    SELECT
        a.branch_id,
        a.customer_id,
        db.balance_date,
        SUM(db.closing_balance_cents)       AS customer_branch_total_cents
    FROM  daily_balances db
    JOIN  accounts a ON db.account_id = a.account_id
    GROUP BY a.branch_id, a.customer_id, db.balance_date
),

branch_level AS (
    -- Step 1b: branch average of customer totals
    SELECT
        branch_id,
        balance_date,
        COUNT(DISTINCT customer_id)         AS branch_customer_count,
        AVG(customer_branch_total_cents)    AS branch_avg_customer_balance_cents
    FROM  customer_branch_totals
    GROUP BY branch_id, balance_date
),

customer_date_totals AS (
    -- Step 2a: one row per (customer_id, balance_date) — bank-wide grain
    SELECT
        a.customer_id,
        db.balance_date,
        SUM(db.closing_balance_cents)       AS customer_total_cents
    FROM  daily_balances db
    JOIN  accounts a ON db.account_id = a.account_id
    GROUP BY a.customer_id, db.balance_date
),

bank_level AS (
    -- Step 2b: bank-wide average of customer totals (NOT avg of branch avgs)
    SELECT
        balance_date,
        COUNT(DISTINCT customer_id)     AS bank_customer_count,
        AVG(customer_total_cents)       AS bank_avg_customer_balance_cents
    FROM  customer_date_totals
    GROUP BY balance_date
)

SELECT
    bl.balance_date,
    bl.branch_id,
    bl.branch_customer_count,
    bl.branch_avg_customer_balance_cents,
    bk.bank_customer_count,
    bk.bank_avg_customer_balance_cents
FROM  branch_level bl
JOIN  bank_level   bk USING (balance_date)
ORDER BY
    bl.balance_date,
    bl.branch_id
