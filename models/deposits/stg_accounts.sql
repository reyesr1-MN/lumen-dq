-- Staging model: accounts
--
-- Grain : one row per account_id
-- Rows  : 1,500
-- Each account belongs to exactly one customer and exactly one branch.

SELECT
    account_id,
    customer_id,
    branch_id,
    product_type,
    account_status,
    currency,
    opened_date,
    closed_date,
    ingested_at
FROM accounts
ORDER BY account_id
