# eth_sample — Ethereum Wallet Clustering Pipeline

An end-to-end data pipeline that clusters Ethereum wallets based on on-chain transaction relationships — wallets that repeatedly transact with each other form clusters ***(sample period from 2026-02-28 to 2026-03-01)***. This exercise helps reveal potential fraudster activities. 

---

## What It Does


This project:
1. Ingests raw Ethereum transaction data into **Google BigQuery**
2. Models wallet-to-wallet edges and dimensional wallet attributes and risk category via **dbt**
3. Runs a **NetworkX graph algorithm** to compute wallet clusters from the edge network
4. Audits and validates cluster outputs downstream
5. Orchestrates the full pipeline daily via **Apache Airflow**
---


## Project Structure

```
eth_sample/                         ← repo root
│
├── airflow/
│    └── dags/
│       ├── utils/
│       │    └── clusters_utils.py  # NetworkX clustering │logic
│       └── eth_pipeline.py         # main DAG  
│
├── models/
│   ├── staging/                    # raw source cleaning — materialised as views
│   ├── intermediate/               # wallet edge construction — materialised as tables
│   ├── marts/                      # wallet activity + cluster facts + audit + semantic models — materialised as tables
│   └── exposures.yml               # models usage in dashboards
│
├── analyses/                       # ad-hoc dbt analyses
├── macros/                         # reusable SQL macros
│    ├── audit_cluster_health.sql
│    ├── cast_to_bignumeric.sql
│    ├── classify_edge_risk.sql
│    ├── find_macro_usage.sql
│    ├── override_default_schema_name.sql
│    ├── show_env.sql
├── scripts/                        
│   └── cluster_metric_query.yml    # semantic layers metrics query
├── seeds/                          # static reference data
├── snapshots/                      # slowly changing dimensions
├── tests/                          # custom dbt data quality tests
│    └── one_wallet_one_cluster.sql # check if any wallet belongs to more than 1 cluster 
├── dbt_project.yml                 # project config + layer materialisation
└── packages.yml                    # dbt package dependencies                
```
---

## Pipeline DAG

```
source_freshness
      │
   raw_tnx
      │
fct_wallet_edges
      │
dim_wallet_address
      │
fct_wallet_clusters   ← NetworkX graph algorithm (Python)
      │
fct_wallet_activity
      │
fct_clusters_audit
      │
   dbt_tests
```

Runs daily at **06:00 UTC** via cron `0 6 * * *`.

---

## dbt Features Demonstrated

- **Multi-layer schema separation** — `staging`, `intermediate`, `marts` each write to their own BigQuery dataset, defined via `+schema` in `dbt_project.yml`
- **Layer-based materialisation** — staging as `view`, intermediate and marts as `table`
- **`+persist_docs`** — table and column descriptions from `.yml` files are pushed directly to BigQuery at both relation and column level
- **Source freshness checks** — validates upstream data arrives on time before any model runs
- **dbt tests** — schema + custom data quality tests run as the final pipeline step


---

## Observability

### Airflow-level
- **Retry logic** — configurable retries with delay on all tasks via `default_args`; `source_freshness` gets additional retries given its dependency on external upstream data
- **Retry callback** — logs task ID and attempt number on every retry event
- **Failure callback** — logs task ID and run ID on terminal failure, ready to extend with Slack or PagerDuty alerting
- **Environment validation at import time** — DAG raises `EnvironmentError` immediately if `DBT_PROJECT_DIR` or `DBT_PROFILES_DIR` are missing, surfacing misconfiguration before any task runs
- **Shell-level logging** — every `BashOperator` wraps dbt commands with `[START]` and `[END]` echo markers for clean log tracing in Airflow UI

### dbt-level
- **Source freshness** as a dedicated DAG task — failures block the entire downstream pipeline
- **`dbt test`** as terminal task — pipeline is only considered successful if all data quality checks pass
- **Cluster audit model** (`fct_clusters_audit`) — dedicated model that validates cluster output integrity post-computation
- **`audit_helper`** package — enables model output comparison across runs to catch silent regressions

