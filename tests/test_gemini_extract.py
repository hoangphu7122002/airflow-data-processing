"""Tests for gemini_extract and extract_article_id_from_key."""
import pytest
from unittest.mock import MagicMock, patch

from utils.gemini_extract import extract_article_from_html
from utils.url_utils import extract_article_id_from_key


@patch("google.generativeai.GenerativeModel")
def test_extract_article_from_html_success(mock_model_class):
    """Test extraction with mocked Gemini API returns parsed dict."""
    mock_model = MagicMock()
    mock_model.generate_content.return_value.text = (
        '{"title": "Test Title", "url": "https://vnexpress.net/article-123.html", '
        '"section": "Thời sự", "published_at": "2025-01-15T10:00:00Z", '
        '"summary": "A summary", "main_entities": ["Entity 1"], "tags": ["tag1"], '
        '"body_text": "Article body text"}'
    )
    mock_model_class.return_value = mock_model

    result = extract_article_from_html(
        "<html><body><h1>Test</h1></body></html>",
        api_key="test-key",
        model="gemini-2.5-flash",
        max_body_chars=8000,
    )

    assert result is not None
    assert result["title"] == "Test Title"
    assert result["url"] == "https://vnexpress.net/article-123.html"
    assert result["section"] == "Thời sự"
    assert result["summary"] == "A summary"
    assert "Entity 1" in result["main_entities"]
    assert "tag1" in result["tags"]
    assert result["body_text"] == "Article body text"


@patch("google.generativeai.GenerativeModel")
def test_extract_article_from_html_missing_required_returns_none(mock_model_class):
    """Test that missing title or url returns None."""
    mock_model = MagicMock()
    mock_model.generate_content.return_value.text = (
        '{"url": "https://vnexpress.net/x.html", "section": "Thế giới"}'
    )
    mock_model_class.return_value = mock_model

    result = extract_article_from_html(
        "<html>...</html>", api_key="key", model="gemini-2.5-flash"
    )

    assert result is None


@patch("google.generativeai.GenerativeModel")
def test_extract_article_from_html_empty_response_returns_none(mock_model_class):
    """Test that empty Gemini response returns None."""
    mock_model = MagicMock()
    mock_model.generate_content.return_value.text = ""
    mock_model_class.return_value = mock_model

    result = extract_article_from_html(
        "<html>...</html>", api_key="key", model="gemini-2.5-flash"
    )

    assert result is None


def test_extract_article_id_from_key():
    """Test extracting article_id from bronze S3 key."""
    key = "vnexpress/bronze/ingestion_date=2025-03-21/source=homepage/article_id=abc123def456.html"
    assert extract_article_id_from_key(key) == "abc123def456"

    key2 = "vnexpress/bronze/ingestion_date=2025-01-01/source=thoi-su/article_id=xyz789.html"
    assert extract_article_id_from_key(key2) == "xyz789"

    assert extract_article_id_from_key("invalid/key") == ""
    assert extract_article_id_from_key("") == ""
