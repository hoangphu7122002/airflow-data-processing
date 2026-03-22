import os
import pytest
from utils.url_utils import derive_article_id, extract_article_links, normalize_url


def test_derive_article_id():
    url1 = "https://vnexpress.net/thoi-su-123.html"
    url2 = "https://vnexpress.net/the-gioi-456.html"
    assert len(derive_article_id(url1)) == 16
    assert derive_article_id(url1) == derive_article_id(url1)
    assert derive_article_id(url1) != derive_article_id(url2)
    assert all(c in "0123456789abcdef" for c in derive_article_id(url1))


def test_normalize_url():
    assert normalize_url("https://vnexpress.net/article-123.html") == "https://vnexpress.net/article-123.html"
    assert normalize_url("/article-123.html") == "https://vnexpress.net/article-123.html"
    assert "vnexpress.net" in normalize_url("https://vnexpress.net/thoi-su-123.html")
    assert normalize_url("https://example.com/other.html") == ""


def test_extract_article_links():
    html_path = os.path.join(os.path.dirname(__file__), "test_data", "vnexpress_listing.html")
    with open(html_path) as f:
        html = f.read()
    pattern = r"https://vnexpress\.net/[a-z0-9-]+-\d+\.html"
    links = extract_article_links(html, pattern)
    assert len(links) >= 2
    assert all("vnexpress.net" in u for u in links)
    assert all("example.com" not in u for u in links)
