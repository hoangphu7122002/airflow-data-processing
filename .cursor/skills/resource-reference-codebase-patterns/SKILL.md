---
name: resource-reference-codebase-patterns
description: >-
  Codebase patterns for DAGs, utils, configs, and tests. Use when implementing
  VnExpress crawler or similar ETL pipelines. Bronze/silver/gold flows,
  Airflow task structure, S3/parquet handling. Self-contained; no external paths.
---

# Codebase Patterns

When implementing new DAGs, utils, or transforms, follow these patterns.

## DAG Structure

**Thin DAGs**: Logic lives in utils or dedicated modules; DAGs define tasks and dependencies only.

```python
import os
from airflow.decorators import dag, task
import pendulum
from airflow.models import Variable
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from utils.helper import validate_running_env, load_yml_configs
from utils.s3_utils import write_parquet_to_s3

CONFIG_PATH = os.path.join(os.path.dirname(__file__), "../configs")

@dag(
    dag_id="flow_dag",
    description="Complete flow: bronze, silver, gold layers",
    schedule="*/30 * * * *",
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Kolkata"),
    catchup=False,
)
def pipeline():
    bronze_config = load_yml_configs(f"{CONFIG_PATH}/bronze/flow_bronze.yml")
    silver_config = load_yml_configs(f"{CONFIG_PATH}/silver/flow_silver.yml")
    gold_config = load_yml_configs(f"{CONFIG_PATH}/gold/flow_gold.yml")
    bucket_name = Variable.get("s3_bucket_forecasting_data")

    @task
    def bronze_ingest() -> Dict: ...

    @task
    def silver_process(bronze_info: Dict) -> Dict: ...

    @task
    def gold_process(silver_info: Dict) -> Dict: ...

    slack_failure_alert = SlackWebhookOperator(
        task_id="slack_failure_alert",
        slack_webhook_conn_id="slack_default",
        message="Flow failed. Check Airflow logs.",
        trigger_rule="one_failed",
    )

    bronze_task = bronze_ingest()
    silver_task = silver_process(bronze_task)
    gold_task = gold_process(silver_task)
    gold_task >> slack_failure_alert

pipeline()
```

## Config Loading

Use `load_yml_configs` and resolve config path relative to DAG file:

```python
from utils.helper import load_yml_configs

config = load_yml_configs(f"{CONFIG_PATH}/bronze/flow_bronze.yml")
# Access: config["data_config"]["s3_output_prefix"]
# Access: config["data_config"]["validation_rules"]
```

## YAML Config Structure

**Bronze** – data source, S3 prefix, validation rules:

```yaml
data_config:
  auth_url: "https://..."
  api_url: "https://..."
  s3_output_prefix: "bronze/flow_name/"
  validation_rules:
    columns:
      - feeder_id
      - timestamp
      - demand_mw
      - ingestion_ts
    constraints:
      timestamp_not_null: true
      demand_mw_positive: true
      demand_mw_range: [0, 100]
```

**Silver** – bronze/silver prefixes, validation:

```yaml
data_config:
  bronze_prefix: "bronze/flow_name/"
  silver_prefix: "silver/flow_name/"
  validation_rules:
    columns: [...]
    constraints: {...}
```

## S3 and Parquet

**Write Parquet to S3** – use `write_parquet_to_s3`:

```python
from utils.s3_utils import write_parquet_to_s3

write_parquet_to_s3(
    s3_hook=s3_hook,
    df=df,
    bucket=bucket_name,
    prefix=f"{base_prefix}year={year}/month={month:02d}/day={day:02d}/data_{job_id}_",
)
```

**Partition layout**: `{prefix}year={Y}/month={M}/day={D}/data_{job_id}_{timestamp}.parquet`

**Read bronze/silver**: List keys, download to temp dir, concat parquets:

```python
all_keys = s3_hook.list_keys(bucket_name, prefix=partition_prefix)
keys = [k for k in all_keys if k.endswith(".parquet")]
with tempfile.TemporaryDirectory() as temp_dir:
    for key in keys:
        s3_hook.download_file(key=key, bucket_name=bucket_name, local_path=temp_dir, ...)
        df_list.append(pd.read_parquet(os.path.join(temp_dir, os.path.basename(key))))
df = pd.concat(df_list, ignore_index=True)
```

## Validation Pattern

Validation uses config-driven rules (columns + constraints):

```python
def validate_data(df: pd.DataFrame, rules: Dict, valid_ids: List[str]) -> bool:
    for col in rules["columns"]:
        if col not in df.columns:
            logging.error(f"Missing required column: {col}")
            return False
    if rules["constraints"].get("timestamp_not_null") and df["timestamp"].isnull().any():
        logging.error("Timestamp column contains null values")
        return False
    if rules["constraints"].get("demand_mw_range"):
        lo, hi = rules["constraints"]["demand_mw_range"]
        if (df["demand_mw"] < lo).any() or (df["demand_mw"] > hi).any():
            return False
    return True
```

## Helper Utils

**Environment validation**:

```python
from utils.helper import validate_running_env

env_variable = validate_running_env(Variable.get("environment", default_var="dev"))
```

**Date/time** – use `pendulum` for timezone-aware timestamps:

```python
import pendulum

current_utc = pendulum.now("UTC")
ist_now = pendulum.now("Asia/Kolkata")
run_date = ist_now.strftime("%d-%m-%Y")
```

## Code Format

- **Formatter**: `ruff format` (run via `make format` or `PYTHONPATH=. ruff format`)
- **Tests**: `PYTHONPATH=.:src/dags pytest tests/`
- **Imports**: Standard library first, then third-party, then local (`from utils.helper import ...`)

## Testing Pattern

Use `moto` for S3 mocking; `pytest` fixtures for sample data:

```python
from moto import mock_aws

@mock_aws
def test_write_parquet_to_s3():
    s3_hook = S3Hook(aws_conn_id="aws_default", region_name="us-east-1")
    client = s3_hook.get_conn()
    client.create_bucket(Bucket=bucket_name)
    write_parquet_to_s3(s3_hook, df, bucket_name, "test/")
    response = client.list_objects_v2(Bucket=bucket_name, Prefix="test/")
    assert "Contents" in response
```

## Deduplication

Silver/gold: sort by key columns + `ingestion_ts`, drop duplicates keeping last:

```python
df = df.sort_values(["feeder_id", "timestamp", "ingestion_ts"])
df = df.drop_duplicates(subset=["feeder_id", "timestamp"], keep="last")
```

## Error Handling and Logging

- Use `logging.info`, `logging.warning`, `logging.error` with context (e.g. `f"Error fetching {id}: HTTP {status}"`)
- Raise `ValueError` for validation failures
- Re-raise with context: `raise Exception(f"Failed: {str(e)}") from e`
