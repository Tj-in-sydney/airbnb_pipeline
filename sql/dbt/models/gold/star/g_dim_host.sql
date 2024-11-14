{{ 
    config(
        unique_key='host_id',
        alias='dim_host'
    ) 
}}

WITH latest_snapshots AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY host_id ORDER BY dbt_updated_at DESC) AS rn
    FROM 
        {{ ref('listing_snapshot') }}  
)

SELECT
    h.*
FROM 
    {{ ref('s_host') }} h  
LEFT JOIN latest_snapshots ls ON h.host_id = ls.host_id AND ls.rn = 1 
