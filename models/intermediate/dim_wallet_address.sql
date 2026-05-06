-- dim_wallet_address

select distinct wallet_address 
from (
    select distinct from_address as wallet_address
    from {{ ref('fct_wallet_edges') }}

    union all 

    select distinct to_address as wallet_address
    from {{ ref('fct_wallet_edges') }}
)