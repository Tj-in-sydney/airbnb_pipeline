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