-- snapshots/item_snapshot.sql

{% snapshot item_snapshot %}
    {{
        config(
            target_schema='snapshots',  -- Schema where snapshot will be stored
            unique_key='item_name',     -- Unique key for tracking changes
            strategy='check',           -- Use 'check' strategy to monitor specific columns
            check_cols=['brand_name', 'msrp_in_usd']  -- Columns to track for changes
        )
    }}

    SELECT 
        item_name,
        brand_name,
        msrp_in_usd
    FROM {{ ref('stg_items') }}

{% endsnapshot %}
