-- models/staging/stg_items.sql

/*
    This model cleans and standardizes the raw items data.
    - Standardizes the brand and item name (lowercase, trims spaces, and removes accents).
    - Renames the MSRP column to msrp_inusd.
    - Reads from the raw items source defined in the schema.yml file.
*/

WITH cleaned_items AS (
    SELECT
        LOWER(TRIM(brand)) AS brand_name,
        LOWER(TRIM(unaccent(item_name))) AS item_name,  -- Removes accents from item_name
        msrp AS msrp_in_usd                             -- Renames msrp to msrp_inusd
    FROM {{ source('raw_data', 'raw_items') }}          -- Reference the raw items table
)

SELECT * FROM cleaned_items
