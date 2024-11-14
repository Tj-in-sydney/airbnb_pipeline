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
