-- Staging model: daily_balances
--
-- Grain : one row per (account_id, balance_date)
-- Rows  : 45,000  (1,500 accounts × 30 dates: 2024-01-01 → 2024-01-30)
-- Money : balance_cents is an integer representing cents.
--         Example: 500000 = $5,000.00

SELECT
    account_id,
    CAST(balance_date AS DATE) AS balance_date,
    closing_balance_cents
FROM daily_balances
ORDER BY account_id, balance_date
