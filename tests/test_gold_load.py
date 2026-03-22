"""Tests for gold load logic and ClickHouse insert."""
import io
import pytest
import pandas as pd
from unittest.mock import MagicMock

from utils.clickhouse_utils import insert_articles_df
from utils.s3_utils import write_parquet_to_s3
from moto import mock_aws
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


def _load_silver_and_dedupe(s3_hook, bucket: str, silver_prefix: str, ds: str) -> pd.DataFrame:
    """Core gold logic: list silver keys, read parquet, concat, dedupe. Extracted for testing."""
    prefix = f"{silver_prefix}ingestion_date={ds}/"
    all_keys = s3_hook.list_keys(bucket_name=bucket, prefix=prefix)
    keys = [k for k in all_keys if k.endswith(".parquet")]
    if not keys:
        return pd.DataFrame()
    dfs = []
    for k in keys:
        raw = s3_hook.read_key(key=k, bucket_name=bucket)
        buf = raw if isinstance(raw, bytes) else raw.encode("utf-8")
        dfs.append(pd.read_parquet(io.BytesIO(buf)))
    df = pd.concat(dfs, ignore_index=True)
    return df.drop_duplicates(subset=["article_id"], keep="last")


@mock_aws
def test_gold_load_dedupe():
    """Test gold load: read silver parquet, dedupe by article_id, assert columns."""
    s3_hook = S3Hook(aws_conn_id="aws_default", region_name="us-east-1")
    bucket = "vnexpress-data"
    client = s3_hook.get_conn()
    client.create_bucket(Bucket=bucket)

    # Two rows with same article_id - dedupe should keep last
    df_silver = pd.DataFrame({
        "article_id": ["id1", "id1"],
        "url": ["https://a.com/1", "https://a.com/1"],
        "title": ["First", "Last"],
        "section": ["Thời sự", "Thời sự"],
        "published_at": [None, None],
        "summary": ["x", "y"],
        "body_text": ["a", "b"],
        "main_entities": [["E1"], ["E2"]],
        "tags": [["t1"], ["t2"]],
        "first_seen_at": [pd.Timestamp("2025-01-01"), pd.Timestamp("2025-01-02")],
        "last_seen_at": [pd.Timestamp("2025-01-01"), pd.Timestamp("2025-01-02")],
        "ingestion_date": ["2025-01-01", "2025-01-01"],
    })
    buf = io.BytesIO()
    df_silver.to_parquet(buf, index=False)
    buf.seek(0)
    key = "vnexpress/silver/ingestion_date=2025-01-01/silver.parquet"
    client.put_object(Bucket=bucket, Key=key, Body=buf.getvalue())

    result = _load_silver_and_dedupe(s3_hook, bucket, "vnexpress/silver/", "2025-01-01")
    assert len(result) == 1
    assert result["article_id"].iloc[0] == "id1"
    assert result["title"].iloc[0] == "Last"
    assert "section" in result.columns
    assert "article_id" in result.columns


@mock_aws
def test_gold_load_write():
    """Test gold write: deduped df written to S3."""
    s3_hook = S3Hook(aws_conn_id="aws_default", region_name="us-east-1")
    bucket = "vnexpress-data"
    client = s3_hook.get_conn()
    client.create_bucket(Bucket=bucket)

    df = pd.DataFrame({
        "article_id": ["id1"],
        "url": ["https://a.com/1"],
        "title": ["Title"],
        "section": ["Thế giới"],
        "published_at": [None],
        "summary": ["x"],
        "body_text": ["y"],
        "main_entities": [["E1"]],
        "tags": [["t1"]],
        "first_seen_at": [pd.Timestamp("2025-01-01")],
        "last_seen_at": [pd.Timestamp("2025-01-01")],
        "ingestion_date": ["2025-01-01"],
    })
    write_parquet_to_s3(s3_hook, df, bucket, "vnexpress/gold/ingestion_date=2025-01-01/")
    response = client.list_objects_v2(Bucket=bucket, Prefix="vnexpress/gold/")
    assert "Contents" in response
    assert any("ingestion_date=2025-01-01" in obj["Key"] for obj in response["Contents"])


def test_insert_articles_df():
    """Test ClickHouse insert with mocked client."""
    mock_client = MagicMock()
    mock_client.insert_df = MagicMock()

    df = pd.DataFrame({
        "article_id": ["id1"],
        "url": ["https://a.com/1"],
        "title": ["Test"],
        "section": ["Thời sự"],
        "published_at": [pd.NaT],
        "summary": [""],
        "body_text": [""],
        "main_entities": [["E1"]],
        "tags": [["t1"]],
        "first_seen_at": [pd.Timestamp("2025-01-01")],
        "last_seen_at": [pd.Timestamp("2025-01-01")],
        "ingestion_date": ["2025-01-01"],
    })
    n = insert_articles_df(mock_client, df, table="vnexpress_articles")
    assert n == 1
    assert mock_client.insert_df.called
