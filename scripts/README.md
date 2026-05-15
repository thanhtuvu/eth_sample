## Scripts
This folder contains operational Python scripts that run outside the dbt DAG.

---

### Files

---

### `clusters_compute_locally.py`

Computes wallet clusters locally using NetworkX.

**Use case:** Free plan alternative to dbt Python models + Dataproc. Reads
`intermediate.fct_wallet_pair` from BigQuery, runs graph computation locally, and writes
`marts.fct_wallet_clusters` back to BigQuery.

---

### `cluster_metrics_query.py`

Queries dbt Semantic Layer metrics via Python SDK.

**Use case:** Paid dbt Cloud plan only. Queries pre-defined metrics from the
semantic layer without writing SQL. Reads metrics defined in
`models/marts/semantic_models/sem_wallet_clusters.yml`.