{{
    config(
        materialized = 'view',
        partition_by={
            "field": "block_date",
            "data_type": "date"
        },
        cluster_by=["from_address"]
    )
}}
 
SELECT
  from_address,
  to_address,
  value,
  
  block_number,
  `hash` as tnx_hash,
  date(block_timestamp) as block_date,
  block_timestamp
FROM {{ source('crypto_ethereum', 'transactions') }}
WHERE DATE(block_timestamp) between  '2026-02-28' and '2026-03-01'
and from_address is not null 
and to_address is not null 
and receipt_status = 1