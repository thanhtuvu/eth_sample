
# ============================================
# Semantic Layer MetricFlow Queries
# ETH_2026_sample dbt project
# ============================================

echo "=== Multiple Metrics by Risk Flag ==="
mf query --metrics total_eth,clusters_count,avg_eth_by_risk_flag --group-by cluster__risk_flag

echo "=== High Risk Clusters Count ==="
mf query --metrics high_risk_clusters_count

echo "=== % High Risk Over Time ==="
mf query --metrics pct_high_risk

echo "=== Total ETH by Size Status ==="
mf query --metrics total_eth --group-by cluster__size_status

