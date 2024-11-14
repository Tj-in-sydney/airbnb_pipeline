
{{ config(
    alias='dm_listing_neighbourhood'
) }}

WITH base_data AS (
    SELECT
        l.listing_neighbourhood,
        l.date AS month_year,
        l.listing_id,
        l.host_id,
        l.lga_code,
        l.price,
        l.review_scores_rating,
        l.availability_30,
        CASE WHEN l.has_availability = 't' THEN 1 ELSE 0 END AS is_active,
        CASE WHEN h.host_is_superhost = 't' THEN 1 ELSE 0 END AS is_superhost
    FROM {{ ref('g_dim_listing') }} l
    LEFT JOIN {{ ref('g_dim_host') }} h ON l.host_id = h.host_id
),

listing_metrics AS (
    SELECT
        listing_neighbourhood,
        lga_code,
        month_year,
        
        SUM(is_active) AS active_listings,
        COUNT(listing_id) AS total_listings,
        (SUM(is_active)::float / NULLIF(COUNT(listing_id), 0) * 100) AS active_listing_rate,
        
        MIN(CASE WHEN is_active = 1 THEN price END) AS min_price,
        MAX(CASE WHEN is_active = 1 THEN price END) AS max_price,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY CASE WHEN is_active = 1 THEN price END) AS median_price,
        ROUND(AVG(CASE WHEN is_active = 1 THEN price END), 2) AS avg_price,
        COUNT(DISTINCT host_id) AS distinct_hosts,
        ROUND((SUM(is_superhost)::float / NULLIF(COUNT(DISTINCT host_id), 0))::NUMERIC, 2) AS superhost_rate,
        ROUND(AVG(CASE WHEN is_active = 1 THEN review_scores_rating END)::NUMERIC, 2) AS avg_review_score,
        SUM(CASE WHEN is_active = 1 THEN 30 - availability_30 ELSE 0 END) AS total_stays,
        SUM(CASE WHEN is_active = 1 THEN (30 - availability_30) * price ELSE 0 END) AS total_estimated_revenue,
        ROUND((SUM(CASE WHEN is_active = 1 THEN (30 - availability_30) * price ELSE 0 END) / NULLIF(SUM(is_active), 0))::NUMERIC, 2) AS avg_estimated_revenue_per_active_listing
    FROM 
        base_data
    GROUP BY 
        listing_neighbourhood, month_year, lga_code
),

-- Calculate Month-over-Month Percentage Change for Active and Inactive Listings
monthly_changes AS (
    SELECT
        lm.listing_neighbourhood,
        lm.month_year,
        lm.active_listings,
        lm.total_listings - lm.active_listings AS inactive_listings,
        LAG(lm.active_listings) OVER (PARTITION BY lm.listing_neighbourhood ORDER BY lm.month_year) AS prev_active_listings,

    -- Active Listings Percentage Change
        CASE 
            WHEN LAG(lm.active_listings) OVER (PARTITION BY lm.listing_neighbourhood ORDER BY lm.month_year) IS NULL OR LAG(lm.active_listings) OVER (PARTITION BY lm.listing_neighbourhood ORDER BY lm.month_year) = 0
            THEN 0
            ELSE (lm.active_listings - LAG(lm.active_listings) OVER (PARTITION BY lm.listing_neighbourhood ORDER BY lm.month_year)) 
             / LAG(lm.active_listings) OVER (PARTITION BY lm.listing_neighbourhood ORDER BY lm.month_year) * 100
        END AS pct_change_active_listings,

    -- Previous inactive listings for percentage change calculation
    LAG(lm.total_listings - lm.active_listings) OVER (PARTITION BY lm.listing_neighbourhood ORDER BY lm.month_year) AS prev_inactive_listings,

    -- Inactive Listings Percentage Change
        CASE 
            WHEN LAG(lm.total_listings - lm.active_listings) OVER (PARTITION BY lm.listing_neighbourhood ORDER BY lm.month_year) IS NULL OR LAG(lm.total_listings - lm.active_listings) OVER (PARTITION BY lm.month_year) = 0
            THEN 0
            ELSE ((lm.total_listings - lm.active_listings) - LAG(lm.total_listings - lm.active_listings) OVER (PARTITION BY lm.listing_neighbourhood ORDER BY lm.month_year))
             / LAG(lm.total_listings - lm.active_listings) OVER (PARTITION BY lm.listing_neighbourhood ORDER BY lm.month_year) * 100
        END AS pct_change_inactive_listings
FROM listing_metrics lm
)

SELECT 
    lm.listing_neighbourhood,
    lm.lga_code,
    lm.month_year,
    mc.prev_active_listings,
    lm.active_listings,
    lm.active_listing_rate,
    lm.min_price,
    lm.max_price,
    lm.median_price,
    lm.avg_price,
    lm.distinct_hosts,
    lm.superhost_rate,
    lm.avg_review_score,
    mc.pct_change_active_listings,
    mc.pct_change_inactive_listings,
    lm.total_stays,
    lm.total_estimated_revenue,
    lm.avg_estimated_revenue_per_active_listing
FROM listing_metrics lm
LEFT JOIN monthly_changes mc ON lm.listing_neighbourhood = mc.listing_neighbourhood AND lm.month_year = mc.month_year
ORDER BY lm.listing_neighbourhood, lm.month_year
