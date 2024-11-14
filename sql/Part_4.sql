--- Question 1 ---

WITH revenue AS (
    SELECT 
        listing_neighbourhood,
        SUM(total_estimated_revenue) AS total_revenue
    FROM gold.dm_listing_neighbourhood
    WHERE month_year >= '04/2020'  
    GROUP BY listing_neighbourhood
),
ranked_lgas AS (
    SELECT 
        listing_neighbourhood,
        total_revenue,
        RANK() OVER (ORDER BY total_revenue DESC) AS revenue_rank_desc,
        RANK() OVER (ORDER BY total_revenue ASC) AS revenue_rank_asc
    FROM revenue
),
top_and_bottom_lgas AS (
    SELECT 
        listing_neighbourhood,
        total_revenue
    FROM ranked_lgas
    WHERE revenue_rank_desc <= 3 OR revenue_rank_asc <= 3
)
SELECT 
    gl.*,
    total_revenue
FROM gold.dim_lga gl
LEFT JOIN top_and_bottom_lgas tbl ON tbl.listing_neighbourhood = gl.lga_name
WHERE total_revenue IS NOT NULL
ORDER BY tbl.total_revenue DESC;

--- Question 2a ---

SELECT 
  CORR(gbl.total_estimated_revenue, dlga.median_age_persons) OVER () AS age_revenue_correlation
FROM gold.dm_listing_neighbourhood gbl
LEFT JOIN gold.dim_lga dlga ON gbl.listing_neighbourhood = dlga.lga_name
LIMIT 1 ;

--- Question 2b -----
SELECT 
  CORR(gbl.total_estimated_revenue, dlga.median_age_persons) OVER () AS age_revenue_correlation,
  CORR(gbl.total_estimated_revenue, dlga.median_mortgage_repay_monthly) OVER () AS mortgage_revenue_correlation,
  CORR(gbl.total_estimated_revenue, dlga.median_tot_hhd_inc_weekly) OVER () AS hhd_inc_revenue_correlation,
  CORR(gbl.total_estimated_revenue, dlga.median_rent_weekly) OVER () AS rent_revenue_correlation
FROM gold.dm_listing_neighbourhood gbl
LEFT JOIN gold.dim_lga dlga ON gbl.listing_neighbourhood = dlga.lga_name
LIMIT 1 ;

--- Question 3 ---

WITH neighborhood_revenue AS (
    SELECT 
        listing_neighbourhood,
        SUM(total_estimated_revenue) AS total_revenue,
        SUM(active_listings) AS total_active_listings,
        SUM(total_estimated_revenue) / NULLIF(SUM(active_listings), 0) AS revenue_per_active_listing
    FROM gold.dm_listing_neighbourhood
    GROUP BY listing_neighbourhood
),
top_neighborhoods AS (
    SELECT 
        listing_neighbourhood
    FROM neighborhood_revenue
    ORDER BY revenue_per_active_listing DESC
    LIMIT 5
),
listing_analysis AS (
    SELECT 
        dl.listing_neighbourhood,
        dl.property_type,
        dl.room_type,
        dl.accommodates,
        SUM(CASE WHEN dl.has_availability = 't' THEN 30 - dl.availability_30 ELSE 0 END) AS total_stays
    FROM gold.dim_listing dl
    JOIN top_neighborhoods tn ON dl.listing_neighbourhood = tn.listing_neighbourhood
    GROUP BY dl.listing_neighbourhood, dl.property_type, dl.room_type, dl.accommodates
)
SELECT 
    listing_neighbourhood,
    property_type,
    room_type,
    accommodates,
    total_stays
FROM 
    listing_analysis
ORDER BY 
    listing_neighbourhood, total_stays DESC;

   
---- Question 4 ----
   
WITH host_listings AS (
    SELECT 
        host_id,
        lga_code,
        COUNT(DISTINCT listing_id) AS listings_count
    FROM gold.dim_listing
    GROUP BY host_id, lga_code
),
multi_lga_hosts AS (
    SELECT 
        host_id,
        COUNT(DISTINCT lga_code) AS distinct_lga_count,
        SUM(listings_count) AS total_listings
    FROM host_listings
    GROUP BY host_id
    HAVING SUM(listings_count) > 1
),
lga_distribution AS (
    SELECT 
        host_id,
        total_listings,
        distinct_lga_count,
        CASE 
            WHEN distinct_lga_count = 1 THEN 'Concentrated in one LGA'
            ELSE 'Distributed across multiple LGAs'
        END AS lga_concentration
    FROM multi_lga_hosts
)
SELECT 
    lga_concentration,
    COUNT(host_id) AS host_count,
    ROUND(AVG(total_listings), 2) AS avg_listings_per_host
FROM lga_distribution
GROUP BY lga_concentration;

SELECT COUNT(DISTINCT host_id)
FROM gold.dim_listing;


--- Question 5 ---- 

WITH single_listing_hosts AS (
    SELECT 
        host_id,
        listing_id,
        listing_neighbourhood,
        lga_code,
        ROUND(SUM((30 - availability_30) * price)::NUMERIC, 2) AS annual_revenue,
        ROUND(SUM((30 - availability_30) * price)::NUMERIC / NULLIF(COUNT(host_id), 0), 2) AS avg_estimated_revenue_per_host
    FROM gold.dim_listing dl 
    WHERE date >= '04/2020'
    GROUP BY host_id, listing_id, listing_neighbourhood, lga_code
    HAVING COUNT(listing_id) = 1
),
mortgage_comparison AS (
    SELECT 
        slh.host_id,
        slh.listing_id,
        slh.listing_neighbourhood,
        slh.lga_code,
        slh.annual_revenue,
        dl.median_mortgage_repay_monthly * 12 AS annual_mortgage,  -- Annualize mortgage repayment
        CASE 
            WHEN slh.annual_revenue >= dl.median_mortgage_repay_monthly * 12 THEN 1 
            ELSE 0 
        END AS mortgage_covered
    FROM single_listing_hosts slh
    LEFT JOIN gold.dim_lga dl ON slh.lga_code = dl.lga_code
),
lga_coverage_summary AS (
    SELECT 
    	listing_neighbourhood,
        lga_code,
        COUNT(host_id) AS total_single_listing_hosts,
        SUM(mortgage_covered) AS hosts_covering_mortgage,
        ROUND(SUM(mortgage_covered)::NUMERIC / NULLIF(COUNT(host_id), 0) * 100, 2) AS pct_hosts_covering_mortgage
    FROM mortgage_comparison
    GROUP BY listing_neighbourhood, lga_code
)
SELECT 
	listing_neighbourhood,
    lga_code,
    total_single_listing_hosts,
    hosts_covering_mortgage,
    pct_hosts_covering_mortgage
FROM lga_coverage_summary
ORDER BY pct_hosts_covering_mortgage DESC
LIMIT 1;

