{{
    config(
        alias='dm_property_type'
    )
}}

WITH base_data AS (
    SELECT
        l.property_type,
        l.room_type,
        l.accommodates,
        l.date as month_year,
        l.listing_id,
        l.host_id,
        l.price,
        l.review_scores_rating,
        l.availability_30,
        CASE WHEN l.has_availability = 't' THEN 1 ELSE 0 END AS is_active,
        CASE WHEN h.host_is_superhost = 't' THEN 1 ELSE 0 END AS is_superhost
    FROM {{ ref('g_dim_listing') }} l
    LEFT JOIN {{ ref('g_dim_host') }} h on l.host_id = h.host_id
),

property_metrics AS (
    SELECT
        property_type,
        room_type,
        accommodates,
        month_year,
        
        SUM(is_active) AS active_listings,
        COUNT(listing_id) AS total_listings,
        (SUM(is_active)::float / NULLIF(COUNT(listing_id), 0) * 100) AS active_listing_rate,
        
        MIN(CASE WHEN is_active = 1 THEN price END) AS min_price,
        MAX(CASE WHEN is_active = 1 THEN price END) AS max_price,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY CASE WHEN is_active = 1 THEN price END) AS median_price,
        ROUND(AVG(CASE WHEN is_active = 1 THEN price END), 2) AS avg_price,
        COUNT(DISTINCT host_id) AS distinct_hosts,
        ROUND((SUM(is_superhost)::float / NULLIF(COUNT(DISTINCT host_id), 0))::NUMERIC, 2) as superhost_rate,
        -- Review Score Average for Active Listings
        ROUND(AVG(CASE WHEN is_active = 1 THEN review_scores_rating END), 2) AS avg_review_score,

        -- Stays and Revenue
        SUM(CASE WHEN is_active = 1 THEN 30 - availability_30 ELSE 0 END) AS total_stays,
        SUM(CASE WHEN is_active = 1 THEN (30 - availability_30) * price ELSE 0 END) AS total_estimated_revenue,
        ROUND((SUM(CASE WHEN is_active = 1 THEN (30 - availability_30) * price ELSE 0 END) / NULLIF(SUM(is_active), 0))::NUMERIC, 2) AS avg_estimated_revenue_per_active_listing
    FROM base_data
    GROUP BY property_type, room_type, accommodates, month_year
),

-- Calculate Month-over-Month Percentage Change for Active and Inactive Listings
monthly_changes AS (
    SELECT
        pm.property_type,
        pm.room_type,
        pm.accommodates,
        pm.month_year,
        pm.active_listings,
        pm.total_listings - pm.active_listings AS inactive_listings,      
        LAG(pm.active_listings) OVER (PARTITION BY pm.property_type, pm.room_type, pm.accommodates 
        ORDER BY pm.month_year) AS prev_active_listings,
        (pm.active_listings - LAG(pm.active_listings) OVER (PARTITION BY pm.property_type, pm.room_type, pm.accommodates ORDER BY pm.month_year)) 
            / NULLIF(LAG(pm.active_listings) OVER (PARTITION BY pm.property_type, pm.room_type, pm.accommodates ORDER BY pm.month_year), 0) * 100 AS pct_change_active_listings,

        -- Inactive Listings Percentage Change
        LAG(pm.total_listings - pm.active_listings) OVER (PARTITION BY pm.property_type, pm.room_type, pm.accommodates ORDER BY pm.month_year) AS prev_inactive_listings,
        ((pm.total_listings - pm.active_listings) - LAG(pm.total_listings - pm.active_listings) OVER (PARTITION BY pm.property_type, pm.room_type, pm.accommodates ORDER BY pm.month_year))
            / NULLIF(LAG(pm.total_listings - pm.active_listings) OVER (PARTITION BY pm.property_type, pm.room_type, pm.accommodates ORDER BY pm.month_year), 0) * 100 AS pct_change_inactive_listings
    FROM property_metrics pm
)

SELECT 
    pm.property_type,
    pm.room_type,
    pm.accommodates,
    pm.month_year,
    pm.active_listing_rate,
    pm.min_price,
    pm.max_price,
    pm.median_price,
    pm.avg_price,
    pm.distinct_hosts,
    pm.superhost_rate,
    pm.avg_review_score,
    mc.prev_active_listings,
    mc.prev_inactive_listings,
    pm.total_stays,
    pm.total_estimated_revenue,
    pm.avg_estimated_revenue_per_active_listing
FROM property_metrics pm
LEFT JOIN monthly_changes mc ON pm.property_type = mc.property_type 
AND pm.room_type = mc.room_type 
AND pm.accommodates = mc.accommodates 
AND pm.month_year = mc.month_year
ORDER BY pm.property_type, pm.room_type, pm.accommodates, pm.month_year