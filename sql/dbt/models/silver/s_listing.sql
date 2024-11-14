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
