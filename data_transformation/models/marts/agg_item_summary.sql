-- models/marts/agg_item_summary.sql

/*
    This model aggregates data at the item level.
    - Aggregates authentication results by item.
    - Uses the master_table as the source for item-level aggregations.
    - Summarizes results into fake, authentic, and bad input counts.
    - Provides additional insights such as total MSRP, breakdown by result, distinct company and region counts.
*/

WITH item_aggregates AS (
    SELECT
        mt.item_name,
        mt.brand_name,
        COUNT(*) AS total_auth_events,  -- Total number of authentication events
        SUM(CASE WHEN mt.result = 'authentic' THEN 1 ELSE 0 END) AS total_authentic,
        SUM(CASE WHEN mt.result = 'fake' THEN 1 ELSE 0 END) AS total_fake,
        SUM(CASE WHEN mt.result = 'bad_input' THEN 1 ELSE 0 END) AS total_bad_input,
        
        -- Total MSRP by authentication result
        SUM(CASE WHEN mt.result = 'authentic' THEN mt.msrp_in_usd ELSE 0 END) AS total_msrp_authentic,
        SUM(CASE WHEN mt.result = 'fake' THEN mt.msrp_in_usd ELSE 0 END) AS total_msrp_fake,
        SUM(CASE WHEN mt.result = 'bad_input' THEN mt.msrp_in_usd ELSE 0 END) AS total_msrp_bad_input,
        
        -- Total MSRP across all events for the item
        SUM(mt.msrp_in_usd) AS total_msrp,

        -- Distinct count of companies authenticating this item
        COUNT(DISTINCT mt.company_id) AS distinct_company_count,

        -- Distinct count of regions where the item was authenticated
        COUNT(DISTINCT mt.region) AS distinct_region_count
    FROM {{ ref('master_table') }} mt  -- Use master_table as the source
    GROUP BY mt.item_name, mt.brand_name
)

SELECT * 
FROM item_aggregates
ORDER BY total_auth_events DESC  -- Order by items with the most authentication events
