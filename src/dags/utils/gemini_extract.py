"""
Gemini LLM extraction: convert VnExpress article HTML to structured JSON.
"""
import json
import logging
import re
import time
from bs4 import BeautifulSoup

EXTRACTION_PROMPT = """Extract from this Vietnamese news article HTML the following fields in JSON:
- title (string)
- url (string, canonical article URL)
- section (string, e.g. Thời sự, Thế giới, Kinh doanh)
- published_at (ISO8601 or null)
- summary (string)
- main_entities (list of strings)
- tags (list of strings)
- body_text (string, max 5000 chars)

Respond with a JSON object and nothing else."""

MAX_RETRIES = 3
RETRY_BACKOFF_BASE = 60  # seconds


def _preprocess_html(html: str, max_body_chars: int) -> str:
    """Strip scripts/styles and truncate to reduce tokens."""
    if not html or not html.strip():
        return ""
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup.find_all(["script", "style"]):
        tag.decompose()
    text = soup.get_text(separator=" ", strip=True)
    if len(text) > max_body_chars:
        text = text[:max_body_chars] + "..."
    return text


def _extract_json_from_response(response_text: str) -> dict | None:
    """Parse JSON from model response, handling markdown code blocks."""
    if not response_text or not response_text.strip():
        return None
    text = response_text.strip()
    # Handle ```json ... ``` wrapper
    match = re.search(r"```(?:json)?\s*([\s\S]*?)\s*```", text)
    if match:
        text = match.group(1).strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return None


def extract_article_from_html(
    html: str,
    api_key: str,
    model: str,
    max_body_chars: int = 8000,
) -> dict | None:
    """
    Extract structured article fields from VnExpress HTML via Gemini.

    Returns dict with title, url, section, published_at, summary, main_entities,
    tags, body_text; or None on failure.
    """
    if not html or not api_key or not model:
        logging.warning("extract_article_from_html: missing html, api_key, or model")
        return None

    processed = _preprocess_html(html, max_body_chars)
    if not processed:
        logging.warning("extract_article_from_html: empty processed HTML")
        return None

    full_prompt = f"{EXTRACTION_PROMPT}\n\n---\nHTML content:\n{processed}"

    # Lazy import so Airflow can parse the silver DAG even if genai fails at image build time;
    # task runtime still requires google-generativeai installed.
    import google.generativeai as genai

    genai.configure(api_key=api_key)
    gemini_model = genai.GenerativeModel(model)

    for attempt in range(MAX_RETRIES):
        try:
            response = gemini_model.generate_content(full_prompt)
            if not response or not response.text:
                logging.warning("extract_article_from_html: empty Gemini response")
                return None
            data = _extract_json_from_response(response.text)
            if not data:
                logging.warning("extract_article_from_html: failed to parse JSON")
                return None
            if not data.get("title") or not data.get("url"):
                logging.warning(
                    "extract_article_from_html: missing required fields (title/url): %s",
                    data,
                )
                return None
            return data
        except Exception as e:
            err_str = str(e).lower()
            if "429" in str(e) or "rate" in err_str or "resource" in err_str:
                if attempt < MAX_RETRIES - 1:
                    delay = RETRY_BACKOFF_BASE * (attempt + 1)
                    logging.warning(
                        "extract_article_from_html: rate limit, retrying in %ds: %s",
                        delay,
                        e,
                    )
                    time.sleep(delay)
                    continue
            raise

    return None
