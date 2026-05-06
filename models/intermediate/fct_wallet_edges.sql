with t1 as (
    select
        from_address
        ,to_address
        ,count(tnx_hash) as tnx_count
        ,sum(eth_value) as total_eth_value
        ,min(block_timestamp) as first_tnx_time
        ,max(block_timestamp) as last_tnx_time
        
    from {{ ref('raw_tnx') }}
    where to_address is not null
    group by 1, 2   
)

,t2 as (
    select from_address
        ,to_address
        ,coalesce(tnx_count,0) as tnx_count
        ,{{cast_to_bignumeric('total_eth_value')}} as total_eth_value
        ,first_tnx_time
        ,last_tnx_time
    from t1 
    where total_eth_value > 0.001
)

,result as (
    select *
        ,safe_divide(total_eth_value,tnx_count) as avg_eth_value
        ,timestamp_diff(current_timestamp(),last_tnx_time,day) as days_since_last_tnx
    from t2 
)

select *
from result