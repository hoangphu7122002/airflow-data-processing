"""Data-quality validation for VnExpress silver layer."""
import pandas as pd


def validate_silver_schema(df: pd.DataFrame) -> bool:
    """
    Validate silver DataFrame has required columns with non-null, non-empty values.

    Required: article_id, url, title.
    Returns False if validation fails, True if valid.
    """
    required = ["article_id", "url", "title"]
    if not all(c in df.columns for c in required):
        return False
    if df.empty:
        return True
    if not df[required].notna().all().all():
        return False
    for col in required:
        if (df[col].astype(str).str.strip().str.len() == 0).any():
            return False
    return True
