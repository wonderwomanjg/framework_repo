
-- CTE/setup statements if needed
-- e.g., CREATE TEMP VIEW, WITH clauses, etc.

-- Final SELECT (becomes the DataFrame written to ORC)
SELECT
  c.*,reverse(entity) AS derived_business_col
FROM ${VIEW_NAME} c
WHERE c.biz_dt = '${BIZ_DT}';
