import pandas as pd
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


def write_parquet_to_s3(
    s3_hook: S3Hook, df: pd.DataFrame, bucket: str, prefix: str
) -> None:
    """Convert DataFrame to Parquet and upload to S3."""
    import pyarrow.parquet as pq
    import pyarrow as pa

    table = pa.Table.from_pandas(df)
    buffer = pa.BufferOutputStream()
    pq.write_table(table, buffer)
    s3_hook.load_bytes(
        buffer.getvalue().to_pybytes(),
        key=f"{prefix}{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.parquet",
        bucket_name=bucket,
        replace=True,
    )
