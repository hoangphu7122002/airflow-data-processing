"""
VnExpress Fetch HTML DAG: drain SQS, fetch HTML per URL, write to S3 bronze.
Multi-worker: N parallel workers compete for SQS messages.
"""
import json
import logging
import os
from datetime import datetime, timezone

import pendulum
import requests
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.hooks.sqs import SqsHook

from utils.helper import load_yml_configs
from utils.url_utils import derive_article_id

CONFIG_PATH = os.path.join(os.path.dirname(__file__), "../configs")

USER_AGENT = "Mozilla/5.0 (compatible; VnExpressCrawler/1.0)"
MAX_BATCH_ITERATIONS = 100  # Avoid runaway when draining queue


@dag(
    dag_id="vnexpress_fetch_html_dag",
    description="Drain SQS, fetch HTML per URL, write to S3 bronze",
    schedule="10 2 * * *",  # 10 min after discover (0 2 * * *)
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["vnexpress", "fetch", "bronze"],
)
def vnexpress_fetch_html():
    @task
    def get_fetch_work() -> list[int]:
        """Return worker IDs for dynamic task mapping."""
        config = load_yml_configs(f"{CONFIG_PATH}/bronze/vnexpress_bronze.yml")
        num_workers = config["data_config"].get("num_fetch_workers", 4)
        return list(range(num_workers))

    @task
    def fetch_html_worker(worker_id: int) -> dict:
        """Each worker drains SQS until queue empty or max iterations."""
        config = load_yml_configs(f"{CONFIG_PATH}/bronze/vnexpress_bronze.yml")
        batch_size = config["data_config"]["batch_size"]
        prefix = config["data_config"]["s3_output_prefix"]
        queue_url = Variable.get("vnexpress_sqs_queue_url")
        bucket = Variable.get("vnexpress_s3_bucket")

        sqs_hook = SqsHook(aws_conn_id="aws_dag_executor")
        s3_hook = S3Hook(aws_conn_id="aws_dag_executor")
        sqs_client = sqs_hook.get_conn()

        total_fetched = 0
        total_deleted = 0

        for _ in range(MAX_BATCH_ITERATIONS):
            response = sqs_client.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=batch_size,
            )
            messages = response.get("Messages") or []
            if not messages:
                break

            for msg in messages:
                try:
                    body = json.loads(msg["Body"])
                    url = body["url"]
                    source = body.get("source", "unknown")
                    ingestion_date = body.get("ingestion_date", datetime.now(timezone.utc).strftime("%Y-%m-%d"))

                    resp = requests.get(url, timeout=15, headers={"User-Agent": USER_AGENT})
                    resp.raise_for_status()
                    html = resp.text
                    resp.encoding = resp.encoding or "utf-8"

                    article_id = derive_article_id(url)
                    key = f"{prefix}ingestion_date={ingestion_date}/source={source}/article_id={article_id}.html"
                    s3_hook.load_bytes(
                        html.encode("utf-8"),
                        key=key,
                        bucket_name=bucket,
                        replace=True,
                    )
                    total_fetched += 1

                    sqs_client.delete_message(
                        QueueUrl=queue_url,
                        ReceiptHandle=msg["ReceiptHandle"],
                    )
                    total_deleted += 1
                except Exception as e:
                    logging.warning("Failed to fetch/save %s: %s", body.get("url", "?"), e)

        logging.info("Worker %d: fetched %d HTML files, deleted %d SQS messages", worker_id, total_fetched, total_deleted)
        return {"worker_id": worker_id, "fetched": total_fetched, "deleted": total_deleted}

    worker_ids = get_fetch_work()
    fetch_html_worker.expand(worker_id=worker_ids)


vnexpress_fetch_html()
