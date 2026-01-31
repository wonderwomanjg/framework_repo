SELECT
    row1.customer_id AS customer_id,
    row1.contract_id AS contract_id,
    row1.status AS status,
    row1.amount AS amount,
    "2025-05-31" AS biz_dt,
    "2025-05-01" AS etl_start_dt,
    "2025-05-31" AS etl_end_dt,
    "active" AS biz_status,
    "Y" AS change_indicator,
    UPPER(row1.status) AS entity_new,
    row2.premium_amt AS premium_amt
FROM row1
INNER JOIN row2 ON row2.contract_id = row1.contract_id;