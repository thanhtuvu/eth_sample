{% macro audit_cluster_health(
    cluster_model='fct_wallet_clusters'
    ,activity_model='fct_wallet_activity'
    ,min_cluster_size=2
    ,high_risk_min_members=10
    ,high_risk_max_days=7
    ,high_risk_min_eth=100
) %}

with result AS (
    SELECT
        t1.cluster_id
        ,MAX(t1.cluster_size) as cluster_size
        ,SUM(t2.eth_sent) AS total_eth_value
        ,CASE
            WHEN MAX(t1.cluster_size) != COUNT(distinct t1.wallet_address)
            THEN 'size-mismatch'
            WHEN MAX(t1.cluster_size) < {{ min_cluster_size }}
            THEN 'single'
            ELSE 'ok'
        END AS size_status

        ,CASE
            WHEN MAX(t1.cluster_size) >= {{ high_risk_min_members }} AND SUM(t2.eth_sent) >= {{ high_risk_min_eth }}
                THEN 'high_risk'
            WHEN MAX(t1.cluster_size) >= {{ (high_risk_min_members / 2) | int }} AND SUM(t2.eth_sent) >= {{ (high_risk_min_eth / 10) | int }}
                THEN 'medium_risk'
            ELSE 'low_risk'
        END AS risk_flag

    FROM {{ ref(cluster_model) }} t1
    LEFT JOIN {{ ref(activity_model) }} t2
        using(wallet_address)
    group by all
)

SELECT * 
,CASE
    WHEN size_status = 'ok' THEN 1
    ELSE 0
    END AS is_valid_cluster

,CASE
    WHEN risk_flag = 'high_risk' THEN 1
    ELSE 0
    END AS is_high_risk
FROM result
ORDER BY total_eth_value DESC

{% endmacro %}