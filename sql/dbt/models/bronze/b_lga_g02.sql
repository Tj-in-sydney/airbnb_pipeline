{{
    config(
        unique_key='lga_code_2016',
        alias='lga_g02'
    )
}}

select * from {{ source('raw', 'raw_lga_g02') }}