import os
import logging
import pandas as pd
from google.cloud import bigquery

log = logging.getLogger(__name__)

PROJECT      = os.getenv("GCP_PROJECT")
DEST_DATASET = os.getenv("GCP_DATASET_STAGING")
GCP_KEY      = os.getenv("GCP_KEY")

required_env_vars = {
    "GCP_PROJECT"        : PROJECT,
    "GCP_DATASET_STAGING": DEST_DATASET,
    "GCP_KEY"            : GCP_KEY
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

def get_target_date(client: bigquery.Client) -> str:
    """
    Returns the next date after the latest block_date in raw_tnx.
    Falls back to yesterday if table doesn't exist or is empty.
    """
    # ── Check if table exists first ───────────────────────────
    try:
        client.get_table(f"{PROJECT}.{DEST_DATASET}.raw_tnx")
    except Exception:
        target = (pd.Timestamp.utcnow() - pd.Timedelta(days=1)).strftime("%Y-%m-%d")
        log.info(f"[INFO] Table not found, falling back to yesterday: {target}")
        return target

    # ── Table exists — query latest date ─────────────────────
    query = f"""
        SELECT MAX(block_date) AS latest_date
        FROM `{PROJECT}.{DEST_DATASET}.raw_tnx`
    """

    result = list(client.query(query).result())
    latest_date = result[0]["latest_date"]

    if latest_date is None:
        target = (pd.Timestamp.utcnow() - pd.Timedelta(days=1)).strftime("%Y-%m-%d")
        log.info(f"[INFO] Table is empty, falling back to yesterday: {target}")
    else:
        target = (pd.Timestamp(latest_date) + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
        log.info(f"[INFO] Latest date in raw_tnx: {latest_date} → loading next date: {target}")

    return target

SAMPLE_ROWS = 50000  # adjust as needed

def insert_yesterday_raw_tnx(target_date: str = None):
    destination = f"{PROJECT}.{DEST_DATASET}.raw_tnx"

    log.info("Connecting to BigQuery...")
    start_time = pd.Timestamp.utcnow()

    client = bigquery.Client.from_service_account_json(
        GCP_KEY, project=PROJECT
    )

    # ── Auto-determine target date if not passed ──────────────
    if not target_date:
        target_date = get_target_date(client)

    log.info(f"[INFO] target_date resolved to: {target_date}")

    try:
        # ── Step 1: SELECT sample into DataFrame 
        log.info(f"[START] Fetching {SAMPLE_ROWS:,} sampled transactions for {target_date}")

        fetch_query = f"""
            SELECT
                from_address
                ,to_address
                ,value
                ,CAST(value AS BIGNUMERIC) / 1e18  AS eth_value
                ,block_number
                ,`hash` AS tnx_hash
                ,DATE(block_timestamp) AS block_date
                ,block_timestamp
            FROM `bigquery-public-data.crypto_ethereum.transactions`
            WHERE
                DATE(block_timestamp) = DATE('{target_date}')
                AND from_address   IS NOT NULL
                AND to_address     IS NOT NULL
                AND receipt_status = 1
            LIMIT {SAMPLE_ROWS}
        """

        df = client.query(fetch_query).to_dataframe()
        log.info(f"[END] Fetched {len(df):,} rows for {target_date}")

        # ── Skip if no data available yet ────────────────────
        if df.empty:
            log.info(
                f"[SKIP] No transactions found for {target_date}. "
                f"Public dataset may not have this date yet. Skipping load."
            )
            return
        
    except Exception:
        log.exception(f"[FAILED] Fetch failed for {target_date}")
        raise

    try:
        # ── Step 2: Load DataFrame into BigQuery
        log.info(f"[START] Loading {len(df):,} rows into {destination}")

        job_config = bigquery.LoadJobConfig(
            write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE,
            time_partitioning = bigquery.TimePartitioning(
                type_ = bigquery.TimePartitioningType.DAY,
                field = "block_date"
            )
            ,schema = [
                bigquery.SchemaField("from_address", "STRING")
                ,bigquery.SchemaField("to_address", "STRING")
                ,bigquery.SchemaField("value", "NUMERIC")
                ,bigquery.SchemaField("eth_value", "BIGNUMERIC")
                ,bigquery.SchemaField("block_number", "INTEGER")
                ,bigquery.SchemaField("tnx_hash", "STRING")
                ,bigquery.SchemaField("block_date", "DATE")
                ,bigquery.SchemaField("block_timestamp", "TIMESTAMP")
            ]
        )

        # ── Use partition decorator only if table exists
        try:
            client.get_table(destination)
            table_ref = f"{destination}${target_date.replace('-', '')}"
            log.info(f"[INFO] Table exists — writing to partition {target_date}")
        except Exception:
            table_ref = destination
            log.info(f"[INFO] Table not found — creating partitioned table")

        load_job = client.load_table_from_dataframe(
            df,
            table_ref,
            job_config=job_config
        )
        load_job.result()

        log.info(
            f"[END] Load job completed. "
            f"{load_job.output_rows:,} rows loaded for {target_date}"
        )

    except Exception:
        log.exception(f"[FAILED] Load job failed for {destination}")
        raise

    elapsed = pd.Timestamp.utcnow() - start_time
    log.info(
        f"[SUCCESS] Transactions loaded in "
        f"{elapsed.total_seconds():.2f} seconds on {target_date}"
    )