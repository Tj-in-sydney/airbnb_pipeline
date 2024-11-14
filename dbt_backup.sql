-- Bronze layer --

-- b_lga_code --
{{
    config(
        unique_key='lga_code',
        alias='lga_code'
    )
}}

select * from {{ source('raw', 'raw_lga_code') }}

-- b_lga_g01 --
{{
    config(
        unique_key='lga_code_2016',
        alias='lga_g01'
    )
}}

select * from {{ source('raw', 'raw_lga_g01') }}

-- b_lga_g02 --
{{
    config(
        unique_key='lga_code_2016',
        alias='lga_g02'
    )
}}

select * from {{ source('raw', 'raw_lga_g02') }}

-- b_lga_suburb --
{{
    config(
        unique_key='suburb_id',
        alias='lga_suburb'
    )
}}

WITH ranked_suburbs AS (
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY suburb_name) AS row_num 
    FROM {{ source('raw', 'raw_lga_suburb') }}
)

SELECT
    LPAD(row_num::TEXT, 5, '1') AS suburb_id,
    suburb_name,
    lga_name
FROM ranked_suburbs


-- b_listing --

{{
    config(
        unique_key='transaction_id',
        alias='listing'
    )
}}

with latest_data as (
    select 
        *,
        TO_DATE(scraped_date, 'YYYY-MM-DD')  as updated_at  
    from {{ source('raw', 'raw_airbnb') }}
)

select
{{ dbt_utils.generate_surrogate_key(['listing_id', 'scraped_date', 'host_id']) }} as transaction_id, 
*
from latest_data
order by updated_at asc

-- Silver layer --

-- s_host --
{{
    config(
        unique_key='host_id',
        alias='host'
    )
}}

with source  as (

    select * from {{ ref('b_listing') }}

),

standardized_neighbourhood as (
    select s.*,
        case
            when UPPER(s.host_neighbourhood) = UPPER(lgs.suburb_name)
                 then lgs.lga_name
            else s.host_neighbourhood
        end as host_neighbourhood_standardized
    from source s
    LEFT JOIN {{ ref('b_lga_suburb') }} lgs ON UPPER(s.host_neighbourhood) = UPPER(lgs.suburb_name)
),

lga_join as (
    select sn.*,
    lga.lga_code as lga_code
    from standardized_neighbourhood sn
    left join {{ ref('b_lga_code') }} lga on UPPER(sn.host_neighbourhood_standardized) = UPPER(lga.lga_name)
),

cleaned as (
    select 
        host_id,
        CASE 
            WHEN host_name = 'NaN' THEN 'unknown' 
            ELSE host_name 
        END AS host_name,
        CASE 
            WHEN host_since = 'NaN' THEN '01/01/2000' 
            ELSE host_since 
        END AS host_since,
        CASE 
            WHEN host_is_superhost = 'NaN' THEN 'f' 
            ELSE host_is_superhost 
        END AS host_is_superhost,
        lga_code,
        CASE 
            WHEN host_neighbourhood_standardized = 'NaN' THEN 'unknown' 
            ELSE LOWER(host_neighbourhood_standardized)
        END AS host_neighbourhood

    from lga_join
),

selected as (
    select
        host_id,
        host_name,
        TO_CHAR(TO_DATE(host_since, 'DD/MM/YYYY'), 'DD/MM/YYYY') as host_since,
        host_is_superhost,
        lga_code,
        host_neighbourhood 
    from cleaned
)

select * from selected

-- listing --

{{
    config(
        unique_key='transaction_id',
        alias='listing'
    )
}}

with source  as (

    select * from {{ ref('b_listing') }}

),

lga_join as (
    select s.*,
    lga.lga_code as lga_code
    from source s
    left join {{ ref('b_lga_code') }} lga on UPPER(s.listing_neighbourhood) = UPPER(lga.lga_name)
),

select_columns as (
    select
        transaction_id,
        listing_id,
        TO_CHAR(TO_DATE(scraped_date, 'YYYY-MM-DD'), 'MM/YYYY') as date,
        host_id,
        lga_code,
        Lower(listing_neighbourhood) as listing_neighbourhood,
        lower(property_type) as property_type,
        lower(room_type) as room_type,
        CAST(ACCOMMODATES AS INT) AS ACCOMMODATES,
        CAST(PRICE AS DECIMAL(10, 2)) AS PRICE,
        has_availability, 
        CAST(AVAILABILITY_30 AS INT) AS AVAILABILITY_30,
        CAST(NUMBER_OF_REVIEWS AS INT) AS NUMBER_OF_REVIEWS,
        CASE 
            WHEN REVIEW_SCORES_RATING = 'NaN' THEN CAST(0 AS DECIMAL(10,2))
            ELSE CAST(REVIEW_SCORES_RATING AS DECIMAL(10,2))
        END AS REVIEW_SCORES_RATING,
        CASE 
            WHEN REVIEW_SCORES_ACCURACY = 'NaN' THEN CAST(0 AS DECIMAL(10,2))
            ELSE CAST(REVIEW_SCORES_ACCURACY AS DECIMAL(10,2))
        END AS REVIEW_SCORES_ACCURACY,
        CASE 
            WHEN REVIEW_SCORES_CLEANLINESS = 'NaN' THEN CAST(0 AS DECIMAL(10,2))
            ELSE CAST(REVIEW_SCORES_CLEANLINESS AS DECIMAL(10,2))
        END AS REVIEW_SCORES_CLEANLINESS,
        CASE 
            WHEN REVIEW_SCORES_CHECKIN = 'NaN' THEN CAST(0 AS DECIMAL(10,2))
            ELSE CAST(REVIEW_SCORES_CHECKIN AS DECIMAL(10,2))
        END AS REVIEW_SCORES_CHECKIN,
        CASE 
            WHEN REVIEW_SCORES_COMMUNICATION = 'NaN' THEN CAST(0 AS DECIMAL(10,2))
            ELSE CAST(REVIEW_SCORES_COMMUNICATION AS DECIMAL(10,2))
        END AS REVIEW_SCORES_COMMUNICATION,
        CASE 
            WHEN REVIEW_SCORES_VALUE = 'NaN' THEN CAST(0 AS DECIMAL(10,2))
            ELSE CAST(REVIEW_SCORES_COMMUNICATION AS DECIMAL(10,2))
        END AS REVIEW_SCORES_VALUE   
    from lga_join
)

select * from select_columns

-- snapshot -- 

{% snapshot listing_snapshot %}

{{
        config(
        strategy = 'timestamp',
        unique_key = 'listing_id',
        updated_at = 'updated_at',
        alias = 'listing_snapshot'
        )
    }}

select * from {{ ref('b_listing') }}

{% endsnapshot %}

-- Gold layer --
-- star --
-- g_dim_host --
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

-- g_dim_lga --

{{
    config(
        unique_key='lga_code',
        alias='dim_lga'
    )
}}

with source  as (

    select * from {{ ref('b_lga_g01') }}

),

join_g02 as (
    select s.*,
    g02.median_age_persons as median_age_persons,
    g02.median_mortgage_repay_monthly as median_mortgage_repay_monthly,
    g02.median_tot_prsnl_inc_weekly as median_tot_prsnl_inc_weekly,
    g02.median_rent_weekly as median_rent_weekly,
    g02.median_tot_fam_inc_weekly as median_tot_fam_inc_weekly,
    g02.average_num_psns_per_bedroom as average_num_psns_per_bedroom,
    g02.median_tot_hhd_inc_weekly as median_tot_hhd_inc_weekly,
    g02.average_household_size as average_household_size
    from source s 
    Left join {{ ref('b_lga_g02') }} g02 on s.lga_code_2016 = g02.lga_code_2016
),

join_lga_code as (
    select 
        RIGHT(LGA_CODE_2016, 5) as lga_code,
        lower(lga.lga_name) as lga_name,
        Tot_P_M,
        Tot_P_F,
        Tot_P_P,
        Age_0_4_yr_M,
        Age_0_4_yr_F,
        Age_0_4_yr_P,
        Age_5_14_yr_M,
        Age_5_14_yr_F,
        Age_5_14_yr_P,
        Age_15_19_yr_M,
        Age_15_19_yr_F,
        Age_15_19_yr_P,
        Age_20_24_yr_M,
        Age_20_24_yr_F,
        Age_20_24_yr_P,
        Age_25_34_yr_M,
        Age_25_34_yr_F,
        Age_25_34_yr_P,
        Age_35_44_yr_M,
        Age_35_44_yr_F,
        Age_35_44_yr_P,
        Age_45_54_yr_M,
        Age_45_54_yr_F,
        Age_45_54_yr_P,
        Age_55_64_yr_M,
        Age_55_64_yr_F,
        Age_55_64_yr_P,
        Age_65_74_yr_M,
        Age_65_74_yr_F,
        Age_65_74_yr_P,
        Age_75_84_yr_M,
        Age_75_84_yr_F,
        Age_75_84_yr_P,
        Age_85ov_M,
        Age_85ov_F,
        Age_85ov_P,
        Counted_Census_Night_home_M,
        Counted_Census_Night_home_F,
        Counted_Census_Night_home_P,
        Count_Census_Nt_Ewhere_Aust_M,
        Count_Census_Nt_Ewhere_Aust_F,
        Count_Census_Nt_Ewhere_Aust_P,
        Indigenous_psns_Aboriginal_M,
        Indigenous_psns_Aboriginal_F,
        Indigenous_psns_Aboriginal_P,
        Indig_psns_Torres_Strait_Is_M,
        Indig_psns_Torres_Strait_Is_F,
        Indig_psns_Torres_Strait_Is_P,
        Indig_Bth_Abor_Torres_St_Is_M,
        Indig_Bth_Abor_Torres_St_Is_F,
        Indig_Bth_Abor_Torres_St_Is_P,
        Indigenous_P_Tot_M,
        Indigenous_P_Tot_F,
        Indigenous_P_Tot_P,
        Birthplace_Australia_M,
        Birthplace_Australia_F,
        Birthplace_Australia_P,
        Birthplace_Elsewhere_M,
        Birthplace_Elsewhere_F,
        Birthplace_Elsewhere_P,
        Lang_spoken_home_Eng_only_M,
        Lang_spoken_home_Eng_only_F,
        Lang_spoken_home_Eng_only_P,
        Lang_spoken_home_Oth_Lang_M,
        Lang_spoken_home_Oth_Lang_F,
        Lang_spoken_home_Oth_Lang_P,
        Australian_citizen_M,
        Australian_citizen_F,
        Australian_citizen_P,
        Age_psns_att_educ_inst_0_4_M,
        Age_psns_att_educ_inst_0_4_F,
        Age_psns_att_educ_inst_0_4_P,
        Age_psns_att_educ_inst_5_14_M,
        Age_psns_att_educ_inst_5_14_F,
        Age_psns_att_educ_inst_5_14_P,
        Age_psns_att_edu_inst_15_19_M,
        Age_psns_att_edu_inst_15_19_F,
        Age_psns_att_edu_inst_15_19_P,
        Age_psns_att_edu_inst_20_24_M,
        Age_psns_att_edu_inst_20_24_F,
        Age_psns_att_edu_inst_20_24_P,
        Age_psns_att_edu_inst_25_ov_M,
        Age_psns_att_edu_inst_25_ov_F,
        Age_psns_att_edu_inst_25_ov_P,
        High_yr_schl_comp_Yr_12_eq_M,
        High_yr_schl_comp_Yr_12_eq_F,
        High_yr_schl_comp_Yr_12_eq_P,
        High_yr_schl_comp_Yr_11_eq_M,
        High_yr_schl_comp_Yr_11_eq_F,
        High_yr_schl_comp_Yr_11_eq_P,
        High_yr_schl_comp_Yr_10_eq_M,
        High_yr_schl_comp_Yr_10_eq_F,
        High_yr_schl_comp_Yr_10_eq_P,
        High_yr_schl_comp_Yr_9_eq_M,
        High_yr_schl_comp_Yr_9_eq_F,
        High_yr_schl_comp_Yr_9_eq_P,
        High_yr_schl_comp_Yr_8_belw_M,
        High_yr_schl_comp_Yr_8_belw_F,
        High_yr_schl_comp_Yr_8_belw_P,
        High_yr_schl_comp_D_n_g_sch_M,
        High_yr_schl_comp_D_n_g_sch_F,
        High_yr_schl_comp_D_n_g_sch_P,
        Count_psns_occ_priv_dwgs_M,
        Count_psns_occ_priv_dwgs_F,
        Count_psns_occ_priv_dwgs_P,
        Count_Persons_other_dwgs_M,
        Count_Persons_other_dwgs_F,
        Count_Persons_other_dwgs_P,
        median_age_persons,
        median_mortgage_repay_monthly,
        median_tot_prsnl_inc_weekly,
        median_rent_weekly,
        median_tot_fam_inc_weekly,
        average_num_psns_per_bedroom,
        median_tot_hhd_inc_weekly,
        average_household_size 
    from join_g02 jg
    left join {{ ref('b_lga_code') }} lga on RIGHT(jg.LGA_CODE_2016, 5) = CAST(lga.lga_code AS TEXT)
)

select * from join_lga_code

-- g_dim_listing --
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

-- g_dim_property_type --
{{
    config(
        unique_key='property_id',
        alias='dim_property_type'
    )
}}

with source  as (

    select * from {{ ref('listing_snapshot') }}

),

distinct_property_type as (
    select distinct property_type
    from source
),

ranked_property as (

    select
        dense_rank() OVER (order by property_type) AS property_id,
        lower(property_type) as property_type
    from distinct_property_type
)

select * from ranked_property


-- g_dim_room_type -- 
{{
    config(
        unique_key='room_id',
        alias='dim_room_type'
    )
}}

with source  as (

    select * from {{ ref('b_listing') }}

),

distinct_room_type as (
    select distinct room_type
    from source
),

ranked_room as (

    select
        dense_rank() OVER (order by room_type) AS room_id,
        lower(room_type) as room_type
    from distinct_room_type
)

select * from ranked_room


-- g_dim_subrub --
{{
    config(
        unique_key='suburb_id',
        alias='dim_suburb'
    )
}}

with source  as (

    select * from {{ ref('b_lga_suburb') }}

),

lga_join as (
    select 
    s.suburb_id,
    lga.lga_code as lga_code,
    lower(s.lga_name) as lga_name,
    lower(s.suburb_name) as suburb_name
    from source s
    left join {{ ref('b_lga_code') }} lga on UPPER(s.lga_name) = UPPER(lga.lga_name)
)

select * from lga_join

-- g_fact_listing --
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


-- dm_host_neighbourhood.sql ---

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



-- dm_listing_neighbourhood.sql --


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
ORDER BY lm.listing_neighbourhood, lm.month_year;


-- dm_property_type.sql -- 

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