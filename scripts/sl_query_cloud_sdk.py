# pip install dbt-sl-sdk
import asyncio
from dbt_sl_sdk.asyncio import SemanticLayerClient
from dbt_sl_sdk.query import QueryParameters

# ── 1. Connect ───────────────────────────────────────────────
client = SemanticLayerClient(
    environment_id=704718235*****, # dbt Cloud → Account Settings → Environment ID
    auth_token="dbtsl_xxxx",       # dbt Cloud → Profile → Service Tokens
    host="semantic-layer.cloud.getdbt.com"
)

# ── 2. Query metrics grouped by risk_flag ────────────────────
async def query_cluster_metrics():

    async with client.session():

        # ── Query 1: all three metrics by risk_flag ──────────
        results = await client.query(
            metrics=[
                "clusters_count",
                "avg_eth_by_risk_flag",
                "pct_high_risk"
            ],
            group_by=[
                "cluster__risk_flag"
            ],
            order_by=[
                "cluster__risk_flag"
            ]
        )

        print("=== Cluster Metrics by Risk Flag ===")
        print(results.to_pandas())

        # ── Query 2: filter to high_risk only ────────────────
        high_risk_only = await client.query(
            metrics=[
                "clusters_count",
                "avg_eth_by_risk_flag"
            ],
            group_by=[
                "cluster__risk_flag",
                "cluster__size_status"
            ],
            where=[
                "{{ Dimension('cluster__risk_flag') }} = 'high_risk'"
            ]
        )

        print("\n=== High Risk Clusters by Size Status ===")
        print(high_risk_only.to_pandas())

        # ── Query 3: trend over time ──────────────────────────
        trend = await client.query(
            metrics=[
                "clusters_count",
                "pct_high_risk"
            ],
            group_by=[
                "cluster__risk_flag",
                "metric_time__week"       #weekly trend
            ],
            order_by=[
                "metric_time__week"
            ]
        )

        print("\n=== Cluster Risk - Weekly Trend ===")
        print(trend.to_pandas())


asyncio.run(query_cluster_metrics())