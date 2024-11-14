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