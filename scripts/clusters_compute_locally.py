import networkx as nx
import pandas as pd
from google.cloud import bigquery

# connect to GCP
project="crypto-493910"
DATASET = "intermediate"
GCP_KEY = 'C:/Users/TuVu/eth_dbt_connection_gcp_key.json' 
client   = bigquery.Client.from_service_account_json(GCP_KEY, project=project)

# 0── create edges_df ────────────────────────────────
edges_df = client.query(f"""
    SELECT wallet_a, wallet_b
    FROM `{DATASET}.fct_wallet_pair`
""").to_dataframe()

# 1── Build undirected graph ────────────────────────────────
G = nx.from_pandas_edgelist(
    edges_df,
    source="wallet_a",
    target="wallet_b"
)
print(f"Graph: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges")

# 2── Compute connected components ─────────────────────────
components = nx.connected_components(G)  

rows = [
    {
        "wallet_address": wallet,
        "cluster_id": cluster_id,          
        "cluster_size": len(component)     
    }
    for cluster_id, component in enumerate(components)

# 3── Write back to BigQuery ────────────────────────────────
destination_dataset = 'marts'
destination_table = 'fct_wallet_clusters'
destination = f"{destination_dataset}.{destination_table}"

job_config = bigquery.LoadJobConfig(
    write_disposition="WRITE_TRUNCATE",   
    schema=[
        bigquery.SchemaField("wallet_address", "STRING"),
        bigquery.SchemaField("cluster_id",   "INTEGER"),
        bigquery.SchemaField("cluster_size", "INTEGER"),
        bigquery.SchemaField("computed_at",  "TIMESTAMP"),
    ]
)

job = client.load_table_from_dataframe(
    result_df,
    destination,
    job_config=job_config
)
job.result() 

print(f"Written to {destination}")
    for wallet in component
]
result_df = pd.DataFrame(rows)
result_df["computed_at"] = pd.Timestamp.utcnow()