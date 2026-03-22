import os
import pytest
import pandas as pd
from utils.helper import load_yml_configs, validate_running_env
from utils.s3_utils import write_parquet_to_s3
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from moto import mock_aws


def test_load_yml_configs():
    # Try dags/configs (Docker: /opt/airflow/dags) then src/dags/configs (local)
    base = os.path.dirname(os.path.dirname(__file__))
    for rel in ("dags/configs/bronze/vnexpress_bronze.yml", "src/dags/configs/bronze/vnexpress_bronze.yml"):
        config_path = os.path.join(base, rel)
        if os.path.isfile(config_path):
            break
    else:
        pytest.fail(f"Config not found; checked dags/configs and src/dags/configs from {base}")
    config = load_yml_configs(config_path)
    assert isinstance(config, dict)
    assert "data_config" in config
    assert "seed_urls" in config["data_config"]
    assert isinstance(config["data_config"]["seed_urls"], list)


def test_validate_running_env():
    assert validate_running_env("dev") == "dev"
    assert validate_running_env("prod") == "prod"
    assert validate_running_env("DEV") == "dev"
    assert validate_running_env(None) == "dev"
    with pytest.raises(ValueError, match="Invalid environment"):
        validate_running_env("staging")


@mock_aws
def test_write_parquet_to_s3():
    s3_hook = S3Hook(aws_conn_id="aws_default", region_name="us-east-1")
    bucket_name = "vnexpress-data"
    client = s3_hook.get_conn()
    client.create_bucket(Bucket=bucket_name)

    df = pd.DataFrame({"timestamp": [pd.Timestamp.now()], "value": [100]})
    write_parquet_to_s3(s3_hook, df, bucket_name, "test/")
    response = client.list_objects_v2(Bucket=bucket_name, Prefix="test/")
    assert "Contents" in response
    assert response["Contents"][0]["Key"].startswith("test/")
