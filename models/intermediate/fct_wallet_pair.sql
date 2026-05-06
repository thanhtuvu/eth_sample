{{
    config(
        materialized='view'
    )
}}

with result as (
    SELECT 
    from_address as wallet_a
    ,to_address as wallet_b 
    FROM {{ ref('fct_wallet_edges') }}

    union distinct

    SELECT 
    to_address as wallet_a
    ,from_address as wallet_b 
    FROM  {{ ref('fct_wallet_edges') }}
)


select distinct *
from result 
where wallet_a != wallet_b