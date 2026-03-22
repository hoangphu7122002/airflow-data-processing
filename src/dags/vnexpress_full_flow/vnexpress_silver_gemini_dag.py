"""
VnExpress Silver DAG: read bronze HTML, call Gemini for extraction, write Parquet to S3 silver.
Multi-worker: split bronze keys into chunks; each chunk runs on a Celery worker; reduce to single Parquet.
"""
import logging
import os
from datetime import datetime, timezone

import pandas as pd
import pendulum
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.python import get_current_context
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from utils.gemini_extract import extract_article_from_html
from utils.helper import load_yml_configs
from utils.s3_utils import write_parquet_to_s3
from utils.url_utils import extract_article_id_from_key
from utils.validation import validate_silver_schema

CONFIG_PATH = os.path.join(os.path.dirname(__file__), "../configs")
BRONZE_PREFIX = "vnexpress/bronze/"


@dag(
    dag_id="vnexpress_silver_gemini_dag",
    description="Read bronze HTML, extract via Gemini, write silver Parquet",
    schedule="0 4 * * *",  # 1–2h after fetch (fetch at 10 2)
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["vnexpress", "silver", "gemini"],
    default_args={"retries": 2, "retry_delay": 60},
)
def vnexpress_silver_gemini():
    @task
    def list_bronze_keys() -> list[list[str]]:
        """List bronze S3 keys for ingestion_date and split into N chunks for workers."""
        context = get_current_context()
        ds = context["ds"]

        silver_config = load_yml_configs(f"{CONFIG_PATH}/silver/vnexpress_silver.yml")
        num_workers = silver_config["data_config"].get("num_silver_workers", 4)
        bucket = Variable.get("vnexpress_s3_bucket")
        s3_hook = S3Hook(aws_conn_id="aws_dag_executor")

        bronze_prefix_full = f"{BRONZE_PREFIX}ingestion_date={ds}/"
        all_keys = s3_hook.list_keys(bucket_name=bucket, prefix=bronze_prefix_full)
        keys = [k for k in all_keys if k.endswith(".html")]

        if not keys:
            logging.info("No bronze HTML for ingestion_date=%s", ds)
            return [[]]

        n = min(num_workers, len(keys)) or 1
        chunk_size = max(1, (len(keys) + n - 1) // n)
        chunks = [keys[i : i + chunk_size] for i in range(0, len(keys), chunk_size)]
        logging.info("Split %d keys into %d chunks for ingestion_date=%s", len(keys), len(chunks), ds)
        return chunks

    @task
    def silver_extract_chunk(keys: list[str]) -> list[dict]:
        """Extract structured data from bronze HTML keys via Gemini; return list of records."""
        context = get_current_context()
        ds = context["ds"]
        now = datetime.now(timezone.utc)

        silver_config = load_yml_configs(f"{CONFIG_PATH}/silver/vnexpress_silver.yml")
        data_config = silver_config["data_config"]
        gemini_model = Variable.get(
            "gemini_model",
            default_var=data_config["gemini_model"],
        )
        api_key = Variable.get("gemini_api_key")
        max_body_chars = data_config["max_body_chars"]
        bucket = Variable.get("vnexpress_s3_bucket")
        s3_hook = S3Hook(aws_conn_id="aws_dag_executor")

        records = []
        for key in keys:
            try:
                html_bytes = s3_hook.read_key(key=key, bucket_name=bucket)
                html = (
                    html_bytes.decode("utf-8")
                    if isinstance(html_bytes, bytes)
                    else html_bytes
                )
                data = extract_article_from_html(
                    html, api_key, gemini_model, max_body_chars
                )
                if data:
                    article_id = extract_article_id_from_key(key)
                    records.append(
                        {
                            **data,
                            "article_id": article_id,
                            "ingestion_date": ds,
                            "first_seen_at": now,
                            "last_seen_at": now,
                        }
                    )
            except Exception as e:
                logging.warning("Failed to extract from %s: %s", key, e)

        logging.info("Chunk extracted %d records from %d keys", len(records), len(keys))
        return records

    @task
    def silver_reduce(records_from_chunks: list) -> dict:
        """Concat chunk results, dedupe, validate, write Parquet to S3."""
        context = get_current_context()
        ds = context["ds"]

        silver_config = load_yml_configs(f"{CONFIG_PATH}/silver/vnexpress_silver.yml")
        silver_prefix = silver_config["data_config"]["silver_prefix"]
        bucket = Variable.get("vnexpress_s3_bucket")
        s3_hook = S3Hook(aws_conn_id="aws_dag_executor")

        all_records = [r for chunk in records_from_chunks for r in chunk]
        if not all_records:
            logging.info("No records extracted for ingestion_date=%s", ds)
            return {"extracted": 0, "ingestion_date": ds}

        df = pd.DataFrame(all_records)
        df = df.drop_duplicates(subset=["article_id"], keep="last")
        if not validate_silver_schema(df):
            raise ValueError(
                "Silver validation failed: required columns (article_id, url, title) must be non-null and non-empty"
            )
        write_parquet_to_s3(
            s3_hook, df, bucket, f"{silver_prefix}ingestion_date={ds}/"
        )
        logging.info("Wrote %d articles to silver for ingestion_date=%s", len(df), ds)
        return {"extracted": len(df), "ingestion_date": ds}

    chunks = list_bronze_keys()
    extract_results = silver_extract_chunk.expand(keys=chunks)
    silver_reduce(extract_results)


vnexpress_silver_gemini()
