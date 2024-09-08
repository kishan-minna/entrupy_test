-- tests/unique_combination_stg_items.sql

SELECT
    brand_name,
    item_name,
    COUNT(*)
FROM {{ ref('stg_items') }}
GROUP BY brand_name, item_name
HAVING COUNT(*) > 1
