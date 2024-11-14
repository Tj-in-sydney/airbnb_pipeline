{{
    config(
        unique_key='lga_code',
        alias='lga_code'
    )
}}

select * from {{ source('raw', 'raw_lga_code') }}