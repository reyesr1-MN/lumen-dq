-- Staging model: branch_reference
--
-- Grain : one row per branch_id
-- Rows  : exactly 42
-- Notes : This is a current-state reference table.
--         No slowly-changing dimension.
--         DQ pre-condition: check_grain_uniqueness + check_branch_count must PASS.
--
-- Pre-condition gate (must pass before any downstream join to this table):
--   check_grain_uniqueness(branch_reference, [branch_id])  → 0 duplicates
--   check_branch_count(branch_reference)                   → exactly 42 rows

SELECT
    branch_id,
    branch_name,
    city,
    state_code
FROM branch_reference
ORDER BY branch_id
