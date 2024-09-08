-- models/staging/stg_companies.sql

/*
    This model cleans and standardizes the raw companies data.
    - Standardizes the company name (lowercase and trims spaces).
    - Selects relevant fields: company_id, company_name, region, and num_staff.
    - Reads from the raw companies source defined in the schema.yml file.
*/

WITH cleaned_companies AS (
    SELECT
        id AS company_id,
        LOWER(TRIM(company_name)) AS company_name,
        region,
        num_staff
    FROM {{ source('raw_data', 'raw_companies') }}  -- Reference the raw companies table
)

SELECT * FROM cleaned_companies
