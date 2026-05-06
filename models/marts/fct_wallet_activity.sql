
with dim_wallet as (
    select * from {{ ref('dim_wallet_address') }}   
)

,raw_tnx as (
    select * from {{ ref('fct_wallet_edges') }}
)

,_sent as (
    select
        d.wallet_address as wallet_address,
        sum(f1.tnx_count) as tnx_sent,
        sum(f1.total_eth_value) as eth_sent
    from dim_wallet d
    join raw_tnx f1 
        on d.wallet_address = f1.from_address
    group by 1
)

,_received as (
    select
        d.wallet_address as wallet_address,
        sum(f2.tnx_count) as tnx_received,
        sum(f2.total_eth_value) as eth_received
    from dim_wallet d
    join raw_tnx f2 
        on d.wallet_address = f2.to_address
    group by 1
)

,combine as (
    select d.wallet_address
        ,coalesce(s.tnx_sent,0) as tnx_sent
        ,cast(coalesce(s.eth_sent,0) as BIGNUMERIC) as eth_sent
        ,coalesce(r.tnx_received,0) as tnx_received
        ,cast(coalesce(r.eth_received,0) as BIGNUMERIC) as eth_received
    from dim_wallet d 
    left join _sent s
        on d.wallet_address = s.wallet_address
    left join _received r
        on d.wallet_address = r.wallet_address
)

,result as (
    select *
        ,case 
            when tnx_sent >0 and tnx_received >0 then 'send and received'
            when tnx_sent >0 then 'send only'
            when tnx_received >0 then 'received only'
            else 'not active yet'
            end as send_receive_activity
    from combine
)

select * 
from result