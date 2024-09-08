-- snapshots/company_snapshot.sql

{% snapshot company_snapshot %}
    {{
        config(
            target_schema='snapshots',  -- Ensure the 'snapshots' schema exists in your database
            unique_key='company_id',    -- Unique key for tracking changes
            strategy='check',           -- Use 'check' strategy to monitor specific columns for changes
            check_cols=['company_name', 'num_staff', 'region']  -- Columns to track for changes
        )
    }}

    SELECT 
        company_id,
        company_name,
        num_staff,
        region
    FROM {{ ref('stg_companies') }}

{% endsnapshot %}
