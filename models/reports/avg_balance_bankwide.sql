-- Report model: bank-wide average customer balance by date
--
-- Metric definition (precise)
-- ─────────────────────────────────────────────────────────────────────────────
-- Step 1 – Customer total:
--   For each (customer_id, balance_date), SUM all account balances across ALL
--   branches.  A customer with accounts in multiple branches is counted ONCE
--   at the bank-wide grain.
--
-- Step 2 – Bank-wide average:
--   For each balance_date, AVG those customer totals.
--
-- Output grain: (balance_date)
-- Expected rows: 30 (one per date)
--
-- IMPORTANT: The bank-wide metric is computed directly at customer-date grain.
-- It is NOT derived by averaging the branch-level averages, which would
-- produce a biased estimate whenever branch customer counts differ.
-- ─────────────────────────────────────────────────────────────────────────────

WITH customer_date_totals AS (
    SELECT
        a.customer_id,
        db.balance_date,
        SUM(db.closing_balance_cents)   AS customer_total_cents
    FROM  daily_balances db
    JOIN  accounts a ON db.account_id = a.account_id
    GROUP BY
        a.customer_id,
        db.balance_date
)
SELECT
    balance_date,
    COUNT(DISTINCT customer_id)     AS customer_count,
    AVG(customer_total_cents)       AS avg_customer_balance_cents,
    SUM(customer_total_cents)       AS total_bank_balance_cents
FROM  customer_date_totals
GROUP BY
    balance_date
ORDER BY
    balance_date
