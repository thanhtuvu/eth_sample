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

    components = nx.connected_components(G)  
    
    rows = [
        {
            "wallet_id": wallet,
            "cluster_id": str(cluster_id),
            "cluster_size": len(component)
        }
        for cluster_id, component in enumerate(components)
        for wallet in component
    ]

    # ── Return as Spark DataFrame ─────────────────────────

    result_df = pd.DataFrame(rows)
    result_df["computed_at"] = pd.Timestamp.utcnow()
    return session.createDataFrame(result_df)   # session = SparkSession