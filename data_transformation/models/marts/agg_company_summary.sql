-- models/marts/agg_company_summary.sql

/*
    This model aggregates data at the company level.
    - Aggregates authentication results by company.
    - Uses the master_table as the source for company-level aggregations.
    - Summarizes results into fake, authentic, and bad input counts.
    - Provides additional insights such as total MSRP, breakdown by result, distinct item and brand counts, and distinct regions.
*/

WITH company_aggregates AS (
    SELECT
        mt.company_id,
        mt.company_name,
        mt.region,
        mt.num_staff,
        COUNT(*) AS total_auth_events,  -- Total number of authentication events
        SUM(CASE WHEN mt.result = 'authentic' THEN 1 ELSE 0 END) AS total_authentic,
        SUM(CASE WHEN mt.result = 'fake' THEN 1 ELSE 0 END) AS total_fake,
        SUM(CASE WHEN mt.result = 'bad_input' THEN 1 ELSE 0 END) AS total_bad_input,
        
        -- Total MSRP by authentication result
        SUM(CASE WHEN mt.result = 'authentic' THEN mt.msrp_in_usd ELSE 0 END) AS total_msrp_authentic,
        SUM(CASE WHEN mt.result = 'fake' THEN mt.msrp_in_usd ELSE 0 END) AS total_msrp_fake,
        SUM(CASE WHEN mt.result = 'bad_input' THEN mt.msrp_in_usd ELSE 0 END) AS total_msrp_bad_input,
        
        -- Total MSRP across all items authenticated by the company
        SUM(mt.msrp_in_usd) AS total_msrp,

        -- Distinct count of items and brands
        COUNT(DISTINCT mt.item_name) AS distinct_item_count,
        COUNT(DISTINCT mt.brand_name) AS distinct_brand_count,
        
        -- Distinct count of regions where the company operates
        COUNT(DISTINCT mt.region) AS distinct_region_count
    FROM {{ ref('master_auth_events') }} mt  -- Use master_table as the source
    GROUP BY mt.company_id, mt.company_name, mt.region, mt.num_staff
)

SELECT * 
FROM company_aggregates
ORDER BY total_auth_events DESC  -- Order by companies with the most authentication events
