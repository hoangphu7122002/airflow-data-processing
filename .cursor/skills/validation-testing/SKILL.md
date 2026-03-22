---
name: validation-testing
description: >-
  Config-driven validation, pytest fixtures, moto for S3 mocking. Use when
  adding validation logic or writing tests.
---

# Validation and Testing Patterns

## Config-Driven Validation

```python
def validate_data(df: pd.DataFrame, rules: dict, valid_ids: list[str]) -> bool:
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
    if not df["feeder_id"].isin(valid_ids).all():
        return False
    return True
```

## Validation Config (YAML)

```yaml
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

## Pytest with Moto (S3)

```python
from moto import mock_aws
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

@mock_aws
def test_write_parquet_to_s3():
    s3_hook = S3Hook(aws_conn_id="aws_default", region_name="us-east-1")
    client = s3_hook.get_conn()
    client.create_bucket(Bucket="test-bucket")
    write_parquet_to_s3(s3_hook, df, "test-bucket", "test/")
    response = client.list_objects_v2(Bucket="test-bucket", Prefix="test/")
    assert "Contents" in response
```

## Pytest Fixtures

```python
@pytest.fixture
def sample_df():
    return pd.DataFrame({
        "timestamp": pd.to_datetime(["2025-06-23 00:00:00"]),
        "demand_mw": [30.0],
        "ingestion_ts": [pd.Timestamp.now()],
    })
```

## Mock Gemini

```python
from unittest.mock import patch

@patch("utils.gemini_extract.call_gemini")
def test_silver_parse(mock_gemini):
    mock_gemini.return_value = {"title": "Test", "url": "https://..."}
    result = parse_article(html_fixture)
    assert result["title"] == "Test"
```
