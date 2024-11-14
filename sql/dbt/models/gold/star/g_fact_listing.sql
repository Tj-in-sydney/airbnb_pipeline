{{ 
    config(
        materialized='table', 
        unique_key='listing_id',
        alias='fact_listing'
    ) 
}}

WITH base_data AS (
    SELECT distinct
        l.listing_id,
        l.date,
        h.host_id, 
        l.accommodates,                    
        l.price,
        l.lga_code,                                                            
        p.property_id,                
        r.room_id                    
    FROM 
        {{ ref('g_dim_listing') }} l 
    LEFT JOIN 
        {{ ref('g_dim_host') }} h ON l.host_id = h.host_id 
    LEFT JOIN 
        {{ ref('g_dim_property_type') }} p ON l.property_type = p.property_type 
    LEFT JOIN 
        {{ ref('g_dim_room_type') }} r ON l.room_type = r.room_type
)

SELECT * from base_data
ORDER BY listing_id
