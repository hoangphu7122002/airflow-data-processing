---
name: config-yaml-bronze-silver-gold
description: >-
  YAML config structure for bronze, silver, gold layers. Use when creating or
  updating pipeline configs.
---

# Config YAML – Bronze, Silver, Gold

## Bronze Config

```yaml
data_config:
  auth_url: "https://..."
  api_url: "https://..."
  s3_output_prefix: "bronze/flow_name/"
  validation_rules:
    columns:
      - feeder_id
      - timestamp
      - demand_mw
      - ingestion_ts
    constraints:
      timestamp_not_null: true
      demand_mw_positive: true
      demand_mw_range: [0, 100]
```

## Silver Config

```yaml
data_config:
  bronze_prefix: "bronze/flow_name/"
  silver_prefix: "silver/flow_name/"
  validation_rules:
    columns:
      - feeder_id
      - timestamp
      - demand_mw
    constraints:
      timestamp_not_null: true
```

## Gold Config

```yaml
data_config:
  gold_csv_prefix: "gold/flow_name/"
  gold_csv_subdivision: "gold/flow_name/subdivision/"
  gold_csv_division: "gold/flow_name/division/"
```

## VnExpress Bronze (Crawler)

```yaml
data_config:
  seed_urls:
    - "https://vnexpress.net/"
  allowed_url_patterns: "https://vnexpress\\.net/[^/]+-[0-9+\\.html"
  s3_output_prefix: "vnexpress/bronze/"
```

## VnExpress Silver (Gemini)

```yaml
data_config:
  gemini_model: "gemini-1.5-flash"
  max_body_chars: 8000
  batch_size: 20
  silver_prefix: "vnexpress/silver/"
```
