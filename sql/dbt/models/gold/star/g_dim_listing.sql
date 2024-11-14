{{ 
    config(
        unique_key='listing_id',
        alias='dim_listing'
    ) 
}}

WITH latest_snapshots AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY listing_id ORDER BY dbt_updated_at DESC) AS rn
    FROM 
        {{ ref('listing_snapshot') }}  
)

SELECT
    s.*
FROM 
    {{ ref('s_listing') }} s  
LEFT JOIN latest_snapshots ls ON s.listing_id = ls.listing_id AND ls.rn = 1 
