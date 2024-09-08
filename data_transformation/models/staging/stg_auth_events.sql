-- models/staging/stg_auth_events.sql

/*
    This model cleans and standardizes the raw authentication events data.
    - Standardizes the item name (lowercase, trims spaces, and removes accents).
    - Selects relevant fields: company_id, item_name, result, and auth_date.
    - Converts auth_date to a proper date format.
    - Reads from the raw authentication events source defined in the schema.yml file.
*/

WITH cleaned_auth_events AS (
    SELECT
        company_id,
        LOWER(TRIM(unaccent(
            CASE 
                WHEN lower(trim(item_name)) = 'dionysus 66 supreme' THEN 'dionysus gg supreme'  -- Correcting the item name
                ELSE item_name
            END
        ))) AS item_name, 
        result,
        CAST(auth_date AS DATE) AS auth_date  -- Converts auth_date to date format
    FROM {{ source('raw_data', 'raw_auth_events') }}  -- Reference the raw authentication events table
)

SELECT * FROM cleaned_auth_events
