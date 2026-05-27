
import os
import logging
import pandas as pd
from google.cloud import bigquery

log = logging.getLogger(__name__)

PROJECT        = os.getenv("GCP_PROJECT")
DEST_DATASET   = os.getenv("GCP_DATASET_STAGING")
GCP_KEY        = os.getenv("GCP_KEY")

required_env_vars = {
    "GCP_PROJECT": PROJECT,
    "GCP_DATASET_STAGING": DEST_DATASET,
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

def insert_yesterday_raw_tnx(target_date: str):
    destination = f"{PROJECT}.{DEST_DATASET}.raw_tnx"       
    log.info("Connecting to BigQuery...")

    start_time = pd.Timestamp.utcnow()
    # ── Connect ──────────────────────────────────────────────

    client = bigquery.Client.from_service_account_json(
        GCP_KEY,
        project=PROJECT
    )

    try:                    
        # ── Delete ────────────────────────────────────────────── 
        delete_query = f"""
            DELETE
            FROM `{destination}`
            WHERE block_date = DATE('{target_date}')"""
        
        log.info(
            f"[START] Deleting partition for {target_date}"
        )

        delete_job  = client.query (delete_query)
        delete_job .result()

        log.info(
            f"[END] Delete operation completed. "
            f"Affected rows: {delete_job.num_dml_affected_rows}"
        )

    except Exception:
        log.exception(
            f"[FAILED] Delete operation failed for partition_date: {target_date}"
        )
        raise

    try:
        # ── Insert ────────────────────────────────────────────── 
        insert_query = f"""
            INSERT INTO `{destination}`
            SELECT
                from_address,
                to_address,
                value,
                CAST(value AS BIGNUMERIC) / 1e18  as eth_value, 
                block_number,
                `hash` as tnx_hash,
                DATE(block_timestamp) as block_date,
                block_timestamp
            FROM `bigquery-public-data.crypto_ethereum.transactions`
            WHERE
                block_date = DATE('{target_date}')
                AND from_address IS NOT NULL
                AND to_address IS NOT NULL
                AND receipt_status = 1
        """

        log.info("[START] Loading yesterday transactions...")

        insert_job = client.query(insert_query)
        insert_job.result()

        count_query = f"""
            SELECT COUNT(*)
            FROM `{destination}`
            WHERE block_date = DATE('{target_date}')
        """

        count = list(client.query(count_query).result())[0][0]     

        log.info(
            f"[END] Insert operation completed. "
            f"[VERIFY] {count:,} rows affected for partition_date: {target_date}"
        )

    except Exception:
        log.exception(
            f"[FAILED] Insert operation failed for table {destination}"
        )
        raise

    elapsed = pd.Timestamp.utcnow() - start_time
    log.info(
        f"[SUCCESS] Transactions loaded in {elapsed.total_seconds():.2f} seconds on {target_date}"
    )