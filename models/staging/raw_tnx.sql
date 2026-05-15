{{
    config(
        materialized = 'incremental'        
        ,unique_key = 'tnx_hash'
        ,incremental_strategy = 'merge'
        ,partition_by={
            "field": "block_date",
            "data_type": "date"
        }
        ,cluster_by=["from_address"]
    )
}}

SELECT
  from_address,
  to_address,
  value,
  CAST(value AS BIGNUMERIC) / 1e18  as eth_value, 
  block_number,
  `hash` as tnx_hash,
  date(block_timestamp) as block_date,
  block_timestamp
FROM {{ source('crypto_ethereum', 'transactions') }}
WHERE from_address is not null 
and to_address is not null 
and receipt_status = 1
{% if var("backfill_start", false) and var("backfill_end", false) %}
    {# 
    backfill when user wanna pass explicit date range
    dbt build --select raw_tnx \
        --vars '{"backfill_start": "2026-01-01", "backfill_end": "2026-01-30"}'
    #}
    and DATE(block_timestamp) BETWEEN
        CAST('{{ var("backfill_start") }}' AS DATE)
    and CAST('{{ var("backfill_end") }}' AS DATE)
{% elif is_incremental() %}
    {#
    If 2 backfill variables are not given -> incremental: load next 1 day from MAX(block_date) in existing table
    #}
    and DATE(block_timestamp) = (
    SELECT DATE_ADD(
        COALESCE(MAX(block_date), DATE('2026-03-01'))
        ,INTERVAL 1 DAY
    ) 
    FROM {{ this }}
)

{% endif %}


