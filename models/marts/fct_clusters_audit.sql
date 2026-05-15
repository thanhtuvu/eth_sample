{{ config(materialized='view') }} 

with audit as (
    {{ audit_cluster_health() }}
)

select *
    ,current_date() as created_at
from audit