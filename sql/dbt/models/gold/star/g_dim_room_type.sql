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
