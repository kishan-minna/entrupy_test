-- models/marts/master_table.sql

/*
    This model combines data from the staging tables (companies, authentication events, and items) into a master table.
    - Joins stg_companies with stg_auth_events using company_id.
    - Joins stg_auth_events with stg_items using item_name to bring in item details.
    - Provides key company, item, and authentication event information.
    - This table is designed for downstream analysis and reporting.
*/
SELECT
    cd.company_id,
    cd.company_name,
    cd.num_staff,
    cd.region,
    i.brand_name,
    i.item_name,
    i.msrp_in_usd,
    ae.auth_date,
    ae.result
FROM {{ ref('stg_companies') }} cd
INNER JOIN {{ ref('stg_auth_events') }} ae ON cd.company_id = ae.company_id  
INNER JOIN {{ ref('stg_items') }} i ON ae.item_name = i.item_name
