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
