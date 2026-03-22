"""
VnExpress Discover Links DAG: fetch seed URLs, extract article links, push to SQS.
"""
import json
import logging
import os
from datetime import datetime, timezone

import pendulum
import requests
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.sqs import SqsHook

from utils.helper import load_yml_configs
from utils.url_utils import extract_article_links, normalize_url

CONFIG_PATH = os.path.join(os.path.dirname(__file__), "../configs")

USER_AGENT = "Mozilla/5.0 (compatible; VnExpressCrawler/1.0)"


@dag(
    dag_id="vnexpress_discover_links_dag",
    description="Discover article URLs from VnExpress homepage and sections, push to SQS",
    schedule="0 2 * * *",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["vnexpress", "discovery"],
)
def vnexpress_discover_links():
    @task
    def discover_links() -> dict:
        config = load_yml_configs(f"{CONFIG_PATH}/bronze/vnexpress_bronze.yml")
        seed_urls = config["data_config"]["seed_urls"]
        patterns = config["data_config"]["allowed_url_patterns"]
        pattern = patterns[0] if isinstance(patterns, list) else patterns
        queue_url = Variable.get("vnexpress_sqs_queue_url")
        ingestion_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        discovered_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        sqs_hook = SqsHook(aws_conn_id="aws_dag_executor")
        all_links = set()

        for seed_url in seed_urls:
            source = "homepage" if seed_url.rstrip("/") == "https://vnexpress.net" else seed_url.split("/")[-1] or "homepage"
            try:
                resp = requests.get(seed_url, timeout=15, headers={"User-Agent": USER_AGENT})
                resp.raise_for_status()
                links = extract_article_links(resp.text, pattern)
                for url in links:
                    normalized = normalize_url(url)
                    if normalized:
                        all_links.add((normalized, source))
            except Exception as e:
                logging.warning("Failed to fetch %s: %s", seed_url, e)

        pushed = 0
        for url, source in all_links:
            msg = {
                "url": url,
                "source": source,
                "discovered_at": discovered_at,
                "ingestion_date": ingestion_date,
            }
            sqs_hook.send_message(queue_url=queue_url, message_body=json.dumps(msg))
            pushed += 1

        logging.info("Pushed %d URLs to SQS", pushed)
        return {"pushed": pushed, "unique_urls": len(all_links)}

    discover_links()


vnexpress_discover_links()
