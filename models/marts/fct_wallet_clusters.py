#This file can only be run on paid plan with BigQuery to use dataproc. For free plan, airflow will use the clusters_util.py
def model(dbt, session):

    dbt.config(
        submission_method="dataproc",
        dataproc_cluster_name="eth_cluster",
        packages=["networkx"]        
    )

    import networkx as nx
    import pandas as pd
    
    # ── Read upstream dbt model as Spark DataFrame ───────
    edges_df = dbt.ref("fct_wallet_pair").toPandas()

    # ── Computing script ─────────
    G = nx.from_pandas_edgelist(
        edges_df,
        source="wallet_a",
        target="wallet_b"
    )

    components = list(nx.connected_components(G))  

    rows = []
        
    for component in components:
        cluster_min  = min(component)          # computed ONCE per component
        cluster_size = len(component)          # computed ONCE per component
        for wallet in component:
            rows.append({
                "wallet_address": wallet,
                "cluster_id"    : cluster_min,
                "cluster_size"  : cluster_size
            })

    # ── Return as Spark DataFrame ─────────────────────────

    result_df = pd.DataFrame(rows)
    result_df["computed_at"] = pd.Timestamp.utcnow()
    return session.createDataFrame(result_df)   # session = SparkSession