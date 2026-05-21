# dags/utils/cluster_utils.py

import os
import logging
import networkx as nx
import pandas as pd
from google.cloud import bigquery

log = logging.getLogger(__name__)

PROJECT        = os.getenv("GCP_PROJECT")
SOURCE_DATASET = os.getenv("GCP_DATASET_INTERMEDIATE")
DEST_DATASET   = os.getenv("GCP_DATASET_MARTS")
GCP_KEY        = os.getenv("GCP_KEY")

required_env_vars = {
    "GCP_PROJECT": PROJECT,
    "GCP_DATASET_INTERMEDIATE": SOURCE_DATASET,
    "GCP_DATASET_MARTS": DEST_DATASET,
    "GCP_KEY": GCP_KEY
}

missing_vars = [
    key for key, value in required_env_vars.items()
    if not value
]

if missing_vars:
    raise EnvironmentError(
        f"Missing required environment variables: "
        f"{', '.join(missing_vars)}"
    )

def compute_wallet_clusters():

    log.info("[START] clusters computing...")
    start_time = pd.Timestamp.utcnow()

    try:                    
        # ── Connect ──────────────────────────────────────────────
        log.info("Connecting to BigQuery...")
        client = bigquery.Client.from_service_account_json(
            GCP_KEY,
            project=PROJECT
        )

        # ── Pull edges ────────────────────────────────────────────
        log.info(f"Pulling edges from {PROJECT}.{SOURCE_DATASET}.fct_wallet_pair")
        edges_df = client.query(f"""
            SELECT wallet_a, wallet_b
            FROM `{PROJECT}.{SOURCE_DATASET}.fct_wallet_pair`
        """).to_dataframe()
        log.info(f"Loaded {len(edges_df)} edges")

        # ── Build graph ───────────────────────────────────────────
        log.info("Building NetworkX graph...")
        G = nx.from_pandas_edgelist(
            edges_df,
            source="wallet_a",
            target="wallet_b"
        )
        log.info(f"Graph: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges")

        # ── Compute clusters ──────────────────────────────────────
        log.info("Computing connected components...")

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

        result_df = pd.DataFrame(rows)
        result_df["computed_at"] = pd.Timestamp.utcnow()

        log.info(f"Found {result_df['cluster_id'].nunique()} clusters")

        # ── Write to BigQuery ─────────────────────────────────────
        destination = f"{PROJECT}.{DEST_DATASET}.fct_wallet_clusters"
        log.info(f"Writing to {destination}...")

        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            schema=[
                bigquery.SchemaField("wallet_address", "STRING"),
                bigquery.SchemaField("cluster_id",     "STRING"),
                bigquery.SchemaField("cluster_size",   "INTEGER"),
                bigquery.SchemaField("computed_at",    "TIMESTAMP"),
            ]
        )

        client.load_table_from_dataframe(
            result_df,
            destination,
            job_config=job_config
        ).result()
        
        elapsed = pd.Timestamp.utcnow() - start_time

        log.info(
            f"[END] clusters computing — completed writing to {destination} "
            f"in {elapsed.total_seconds():.2f} seconds")

    except Exception as e:
        log.error(f"[FAILED] clusters computing — {str(e)}")
        raise