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