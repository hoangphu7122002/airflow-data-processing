---
name: dag-airflow-patterns
description: >-
  Airflow DAG patterns: thin DAGs, @dag/@task decorators, config loading,
  Slack alerts, task dependencies. Use when creating or modifying Airflow DAGs.
---

# DAG Airflow Patterns

## Thin DAGs

Logic lives in utils or dedicated modules; DAGs define tasks and dependencies only.

```python
import os
from airflow.decorators import dag, task
import pendulum
from airflow.models import Variable
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from utils.helper import load_yml_configs
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
    bucket_name = Variable.get("s3_bucket_forecasting_data")

    @task
    def bronze_ingest() -> dict: ...

    @task
    def silver_process(bronze_info: dict) -> dict: ...

    slack_failure_alert = SlackWebhookOperator(
        task_id="slack_failure_alert",
        slack_webhook_conn_id="slack_default",
        message="Flow failed. Check Airflow logs.",
        trigger_rule="one_failed",
    )

    bronze_task = bronze_ingest()
    silver_task = silver_process(bronze_task)
    silver_task >> slack_failure_alert

pipeline()
```

## Config Loading

```python
config = load_yml_configs(f"{CONFIG_PATH}/bronze/flow_bronze.yml")
s3_prefix = config["data_config"]["s3_output_prefix"]
rules = config["data_config"]["validation_rules"]
```

## Task Dependencies

```python
discover_task = discover_links()
fetch_task = fetch_html_batch(discover_task)
silver_task = run_silver(fetch_task)
silver_task >> slack_failure_alert
```

## Variables and Connections

```python
bucket_name = Variable.get("s3_bucket_forecasting_data")
queue_url = Variable.get("sqs_queue_url")
api_key = Variable.get("gemini_api_key")
# Use S3Hook(aws_conn_id="aws_dag_executor"), SqsHook(aws_conn_id="aws_dag_executor")
```
