"""
VnExpress Gold DAG: read silver Parquet, dedupe, write gold to S3, optionally load to ClickHouse.
"""
import io
import logging
import os

import pandas as pd
import pendulum
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.python import get_current_context
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from utils.helper import load_yml_configs
from utils.s3_utils import write_parquet_to_s3

CONFIG_PATH = os.path.join(os.path.dirname(__file__), "../configs")
SILVER_PREFIX = "vnexpress/silver/"
GOLD_PREFIX = "vnexpress/gold/"


@dag(
    dag_id="vnexpress_gold_load_dag",
    description="Read silver Parquet, dedupe, write gold to S3 and optionally ClickHouse",
    schedule="0 5 * * *",  # 1h after silver (0 4)
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["vnexpress", "gold", "analytics"],
    default_args={"retries": 2, "retry_delay": 60},
)
def vnexpress_gold_load():
    @task
    def run_gold_load() -> dict:
        context = get_current_context()
        ds = context["ds"]

        gold_config = load_yml_configs(f"{CONFIG_PATH}/gold/vnexpress_gold.yml")
        data_config = gold_config["data_config"]
        gold_prefix = data_config["gold_prefix"]
        silver_prefix = data_config.get("silver_prefix", SILVER_PREFIX)
        bucket = Variable.get("vnexpress_s3_bucket")

        s3_hook = S3Hook(aws_conn_id="aws_dag_executor")

        silver_prefix_full = f"{silver_prefix}ingestion_date={ds}/"
        all_keys = s3_hook.list_keys(bucket_name=bucket, prefix=silver_prefix_full)
        keys = [k for k in all_keys if k.endswith(".parquet")]

        if not keys:
            logging.info("No silver Parquet for ingestion_date=%s", ds)
            return {"articles": 0, "ingestion_date": ds}

        dfs = []
        for k in keys:
            raw = s3_hook.read_key(key=k, bucket_name=bucket)
            buf = raw if isinstance(raw, bytes) else raw.encode("utf-8")
            dfs.append(pd.read_parquet(io.BytesIO(buf)))

        df = pd.concat(dfs, ignore_index=True)
        df = df.drop_duplicates(subset=["article_id"], keep="last")

        gold_prefix_full = f"{gold_prefix}ingestion_date={ds}/"
        write_parquet_to_s3(s3_hook, df, bucket, gold_prefix_full)

        logging.info(
            "Wrote %d articles to gold for ingestion_date=%s",
            len(df),
            ds,
        )
        result = {"articles": len(df), "ingestion_date": ds}

        clickhouse_enabled = Variable.get(
            "clickhouse_enabled",
            default_var=data_config.get("clickhouse_enabled", "false"),
        )
        if str(clickhouse_enabled).lower() == "true":
            try:
                _insert_to_clickhouse(df, data_config)
            except Exception as e:
                logging.exception("ClickHouse insert failed: %s", e)
                raise

        return result

    def _insert_to_clickhouse(df: pd.DataFrame, data_config: dict) -> None:
        """Bulk insert df into ClickHouse when enabled."""
        import clickhouse_connect
        from utils.clickhouse_utils import create_vnexpress_articles_table, insert_articles_df

        host = data_config.get("clickhouse_host", "clickhouse")
        port = int(data_config.get("clickhouse_port", 8123))
        table = data_config.get("clickhouse_table", "vnexpress_articles")

        client = clickhouse_connect.get_client(host=host, port=port)
        create_vnexpress_articles_table(client)
        insert_articles_df(client, df, table=table)

    run_gold_load()


vnexpress_gold_load()
