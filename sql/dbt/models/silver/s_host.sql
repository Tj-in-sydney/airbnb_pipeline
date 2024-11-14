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
