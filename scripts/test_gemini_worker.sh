#!/bin/bash
# Test Gemini extraction inside the airflow-worker container.
# Prerequisites: docker compose up -d; .env with GEMINI_API_KEY, GEMINI_MODEL
# Run from project root: ./scripts/test_gemini_worker.sh

set -e

echo "=== Testing Gemini extraction in airflow-worker ==="

docker compose exec airflow-worker bash -c '
cd /opt/airflow
export PYTHONPATH=.:dags

# Sample VnExpress-like HTML
SAMPLE_HTML="<html><body><h1 class=\"title-detail\">Test Article Title</h1><p class=\"description\">Summary text here.</p><div class=\"Normal\"><p>Article body paragraph.</p></div></body></html>"

python3 -c "
import os
from utils.gemini_extract import extract_article_from_html

api_key = os.environ.get(\"GEMINI_API_KEY\") or os.environ.get(\"gemini_api_key\")
model = os.environ.get(\"GEMINI_MODEL\", \"gemini-2.5-flash\")

if not api_key:
    print(\"ERROR: GEMINI_API_KEY not set in worker. Check .env and env_file in docker-compose.\")
    exit(1)

print(f\"Using model: {model}\")
result = extract_article_from_html(SAMPLE_HTML, api_key, model, max_body_chars=8000)
if result:
    print(\"SUCCESS: Gemini extraction worked!\")
    print(\"Extracted:\", {k: str(v)[:80] for k, v in result.items()})
else:
    print(\"FAILED: extract_article_from_html returned None\")
    exit(1)
"
'

echo "=== Done ==="
