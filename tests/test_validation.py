"""Tests for silver schema validation."""
import pytest
import pandas as pd

from utils.validation import validate_silver_schema


def test_validate_silver_schema_valid():
    """Valid DataFrame passes."""
    df = pd.DataFrame({
        "article_id": ["id1", "id2"],
        "url": ["https://a.com/1", "https://a.com/2"],
        "title": ["Title One", "Title Two"],
    })
    assert validate_silver_schema(df) is True


def test_validate_silver_schema_empty():
    """Empty DataFrame passes (no invalid rows)."""
    df = pd.DataFrame(columns=["article_id", "url", "title"])
    assert validate_silver_schema(df) is True


def test_validate_silver_schema_missing_column():
    """Missing required column fails."""
    df = pd.DataFrame({
        "article_id": ["id1"],
        "url": ["https://a.com/1"],
    })
    assert validate_silver_schema(df) is False


def test_validate_silver_schema_null_fails():
    """Null in required column fails."""
    df = pd.DataFrame({
        "article_id": ["id1", None],
        "url": ["https://a.com/1", "https://a.com/2"],
        "title": ["Title", "Title 2"],
    })
    assert validate_silver_schema(df) is False


def test_validate_silver_schema_empty_string_fails():
    """Empty string in required column fails."""
    df = pd.DataFrame({
        "article_id": ["id1"],
        "url": ["https://a.com/1"],
        "title": [""],
    })
    assert validate_silver_schema(df) is False


def test_validate_silver_schema_whitespace_only_fails():
    """Whitespace-only title fails."""
    df = pd.DataFrame({
        "article_id": ["id1"],
        "url": ["https://a.com/1"],
        "title": ["   "],
    })
    assert validate_silver_schema(df) is False
