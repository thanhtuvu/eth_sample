-- fails if any wallet belongs to more than 1 cluster
SELECT
    wallet_address,
    COUNT(DISTINCT cluster_id) AS cluster_count
FROM {{ ref('fct_wallet_clusters') }}
GROUP BY all
HAVING cluster_count > 1