---
name: gemini-silver-extraction
description: >-
  Gemini LLM extraction: prompts, JSON parsing, schema validation, rate limits.
  Use when implementing silver layer with Gemini for HTML-to-structured extraction.
---

# Gemini Silver Extraction Patterns

## API Key and Model

```python
api_key = Variable.get("gemini_api_key")
# Use google-generativeai or REST; never commit keys
```

## Extraction Prompt Schema

```
Extract from this Vietnamese news article HTML the following fields in JSON:
- title (string)
- section (string, e.g. Thời sự, Thế giới, Kinh doanh)
- published_at (ISO8601 or null)
- summary (string)
- main_entities (list of strings)
- tags (list of strings)
- body_text (string, max 5000 chars)

Respond with a JSON object and nothing else.
```

## Parse and Validate

```python
import json

response_text = model.generate_content(prompt).text
data = json.loads(response_text)
if not data.get("title") or not data.get("url"):
    logging.warning(f"Missing required fields for article: {data}")
    return None
return data
```

## Rate Limits and Retries

```python
for attempt in range(3):
    try:
        result = call_gemini(html)
        return result
    except Exception as e:
        if "429" in str(e) or "rate" in str(e).lower():
            await asyncio.sleep(60 * (attempt + 1))
            continue
        raise
```

## Silver Schema Fields

- `article_id`, `url`, `title`, `section`, `published_at`, `summary`, `body_text`
- `main_entities`, `tags` (arrays)
- `first_seen_at`, `last_seen_at`, `ingestion_date`

## Deduplicate by article_id

```python
df = df.sort_values(["article_id", "last_seen_at"])
df = df.drop_duplicates(subset=["article_id"], keep="last")
```
