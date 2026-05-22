from airflow import DAG
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models.baseoperator import chain

from datetime import datetime, timedelta
from utils.clusters_utils import compute_wallet_clusters
import os

log = LoggingMixin().log

# ── dbt env paths ────────────────────────────────────────────────────────
DBT_PROJECT_DIR  = os.getenv("DBT_PROJECT_DIR")
DBT_PROFILES_DIR = os.getenv("DBT_PROFILES_DIR")

required_env_vars = {
    "DBT_PROJECT_DIR": DBT_PROJECT_DIR,
    "DBT_PROFILES_DIR": DBT_PROFILES_DIR
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

DBT_DIRS = (
    f"--project-dir \"{DBT_PROJECT_DIR}\" "
    f"--profiles-dir \"{DBT_PROFILES_DIR}\""
)
# ── CALLBACK wrapper function ────────────────────────────────────────────
def retry_callback(context):
    ti = context['task_instance']
    log.warning(
        f"[RETRY] {ti.task_id} "
        f"— attempt {ti.try_number} "
        f"of {ti.max_tries + 1}"
    )

def failure_callback(context):
    ti = context['task_instance']
    log.error(
        f"[FAILED] task = {ti.task_id}"
        f"— run_id = {context['run_id']}"
    )

# ── DAG config ────────────────────────────────────────────────────────
default_args = {
    "owner"      : "airflow",
    "retries"    : 1,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": False,
    "on_retry_callback": retry_callback,
    "on_failure_callback": failure_callback
}

# ── logs wrapper function ─────────────────────────────────────────────
def dbt_bash(task_name: str, dbt_command: str) -> str:
    """
    Wraps a dbt bash command with echo log messages.
    """

    return f"""
    set -e
    echo '[START] {task_name}'
    {dbt_command} {DBT_DIRS}
    echo '[END] {task_name} completed successfully'
    """

# ── TASKS creation ───────────────────────────────────────────────────
def create_dbt_task(task_name: str) -> BashOperator:
    return BashOperator(
        task_id = task_name,
        bash_command = dbt_bash(
            task_name,
            f"dbt run -s {task_name}"
        )
    )

# ── main DAG ─────────────────────────────────────────────────────────
with DAG(
    dag_id="eth_wallet_pipeline",
    schedule="0 6 * * *",
    start_date=datetime(2026, 3, 1),
    catchup=False,
    default_args=default_args,
    tags=["clustering"]
) as dag:

    # Source freshness
    source_freshness = BashOperator(
        task_id="source_freshness",
        retries = 2,
        retry_delay = timedelta(minutes=5),
        bash_command = f"""
            set -e
            echo '[START] source freshness check'
            dbt source freshness {DBT_DIRS} 
            echo '[END] source freshness check completed'
        """
    )

    # Upstream Tasks Chain
    upstream_models  = [
        'raw_tnx',
        'fct_wallet_edges',
        'dim_wallet_address',
    ]

    upstream_tasks = [
        create_dbt_task(model)
        for model in upstream_models
    ]

    chain(*upstream_tasks)

    # Cluster Task - NetworkX
    wallet_clusters = PythonOperator(
        task_id="fct_wallet_clusters",
        retries = 0,
        python_callable=compute_wallet_clusters   
    )

    # Downstream Tasks Chain
    downstream_models = [
        'fct_wallet_activity',
        'fct_clusters_audit'
    ]

    downstream_tasks = [
        create_dbt_task(model)
        for model in downstream_models
    ]

    chain(*downstream_tasks)

    # dbt tests
    dbt_tests = BashOperator(
        task_id="dbt_tests",
        bash_command=f"""
            set -e
            echo '[START] dbt tests'
            dbt tests {DBT_DIRS}
            echo '[END] dbt tests completed'
        """
    )

    # Pipeline sequence
    source_freshness >> upstream_tasks[0]
    upstream_tasks[-1] >> wallet_clusters >> downstream_tasks[0]
    downstream_tasks[-1] >> dbt_tests