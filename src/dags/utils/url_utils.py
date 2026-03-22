import hashlib
import re
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup


def derive_article_id(url: str) -> str:
    """Derive a stable 16-char article ID from URL (for S3 bronze key)."""
    return hashlib.sha256(url.encode()).hexdigest()[:16]


def extract_article_id_from_key(key: str) -> str:
    """Extract article_id from bronze S3 key: .../article_id=<id>.html"""
    match = re.search(r"article_id=([^.]+)\.html", key)
    return match.group(1) if match else ""


def normalize_url(url: str, base: str = "https://vnexpress.net/") -> str:
    """Normalize URL: ensure vnexpress.net prefix, strip fragment and tracking params."""
    if not url or not url.strip():
        return ""
    url = url.strip()
    if url.startswith("//"):
        url = "https:" + url
    elif url.startswith("/"):
        url = urljoin(base, url)
    parsed = urlparse(url)
    if parsed.netloc and "vnexpress.net" not in parsed.netloc:
        return ""
    if not parsed.scheme:
        url = "https://" + url
        parsed = urlparse(url)
    if parsed.netloc and "vnexpress.net" not in parsed.netloc:
        return ""
    if not parsed.path:
        return ""
    if not parsed.netloc:
        url = urljoin(base, url)
    parsed = urlparse(url)
    clean = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
    if parsed.query:
        clean += "?" + parsed.query
    return clean


def extract_article_links(html: str, pattern: str) -> list[str]:
    """Extract article links from HTML that match the given regex pattern."""
    if not html:
        return []
    soup = BeautifulSoup(html, "html.parser")
    regex = re.compile(pattern)
    seen = set()
    links = []
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if not href:
            continue
        full_url = normalize_url(href)
        if not full_url:
            continue
        if regex.match(full_url) and full_url not in seen:
            seen.add(full_url)
            links.append(full_url)
    return links
