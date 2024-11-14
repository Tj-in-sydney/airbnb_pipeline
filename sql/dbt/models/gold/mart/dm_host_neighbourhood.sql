{{
    config(
        alias='dm_host_neighbourhood'
    )
}}

WITH host_revenue AS (
    SELECT
        h.host_id,
        h.host_neighbourhood,
        h.lga_code,
        l.date as month_year,  -- Truncate date to month/year
        SUM(l.price) as estimated_revenue  -- Calculate estimated revenue
    FROM 
        {{ ref('g_dim_host') }} h  -- Reference to host dimension
    JOIN 
        {{ ref('g_fact_listing') }} l ON h.host_id = l.host_id 
    GROUP BY h.host_id, h.host_neighbourhood, h.lga_code, month_year
)

SELECT
    host_neighbourhood,
    month_year,
    COUNT(DISTINCT host_id) AS num_distinct_hosts,  -- Count distinct hosts
    ROUND(SUM(estimated_revenue), 2) AS total_estimated_revenue,  -- Total estimated revenue
    ROUND(SUM(estimated_revenue) / NULLIF(COUNT(DISTINCT host_id), 0),2) AS estimated_revenue_per_host  -- Revenue per distinct host
FROM host_revenue
GROUP BY host_neighbourhood, month_year
ORDER BY host_neighbourhood, month_year

