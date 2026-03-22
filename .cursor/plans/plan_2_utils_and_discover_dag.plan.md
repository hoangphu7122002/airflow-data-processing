---
name: Plan 2 – Utils Tests and Discover DAG
overview: Add unit tests for utils, create url_utils (normalize_url, extract_article_links), and implement vnexpress_discover_links_dag. Completes Phase 2 and Phase 4 of the main plan.
todos: []
isProject: false
---

# Plan 2: Utils Tests and Discover DAG

After Plan 1, implement utils tests and the Link Discovery DAG. Reference: [vnexpress_manual_step_by_step_plan.plan.md](vnexpress_manual_step_by_step_plan.plan.md).

---

## Phases in This Plan


| Phase | Goal                                                       |
| ----- | ---------------------------------------------------------- |
| 1     | Unit tests for helper and s3_utils                         |
| 2     | Create url_utils.py (normalize_url, extract_article_links) |
| 3     | Create vnexpress_discover_links_dag.py                     |


---

## Phase 1: Utils Unit Tests

**Goal:** Tests for load_yml_configs, validate_running_env, write_parquet_to_s3.


| Step | Action                                                                       | Reference                                                                        |
| ---- | ---------------------------------------------------------------------------- | -------------------------------------------------------------------------------- |
| 1.1  | Create tests/test_utils.py: test_load_yml_configs, test_validate_running_env | [07-data-quality-and-testing.mdc](.cursor/rules/07-data-quality-and-testing.mdc) |
| 1.2  | Add test_write_parquet_to_s3 with @mock_aws (moto)                           | [validation-testing](.cursor/skills/validation-testing/SKILL.md)                 |


**Check:** `PYTHONPATH=.:src/dags pytest tests/test_utils.py -v` passes.

---

## Phase 2: url_utils

**Goal:** URL normalization and article link extraction from HTML.


| Step | Action                                                      | Reference                                                      |
| ---- | ----------------------------------------------------------- | -------------------------------------------------------------- |
| 2.1  | Create utils/url_utils.py: normalize_url(url)               | [02-ingestion-layer.mdc](.cursor/rules/02-ingestion-layer.mdc) |
| 2.2  | Add extract_article_links(html, pattern) with BeautifulSoup | [02-ingestion-layer.mdc](.cursor/rules/02-ingestion-layer.mdc) |


**Check:** Unit test with fixture HTML; assert only vnexpress.net article URLs.

---

## Phase 3: Discover Links DAG

**Goal:** DAG that fetches seed URLs, extracts links, pushes to SQS.


| Step | Action                                                     | Reference                                                            |
| ---- | ---------------------------------------------------------- | -------------------------------------------------------------------- |
| 3.1  | Create vnexpress_full_flow/vnexpress_discover_links_dag.py | [06-airflow-dags.mdc](.cursor/rules/06-airflow-dags.mdc)             |
| 3.2  | @dag schedule 0 2 * * *; @task discover_links              | [dag-airflow-patterns](.cursor/skills/dag-airflow-patterns/SKILL.md) |
| 3.3  | Load config, fetch seeds, extract links, push JSON to SQS  | [02-ingestion-layer.mdc](.cursor/rules/02-ingestion-layer.mdc)       |


**Check:** Trigger DAG; SQS has messages.

---

## Next Plan

After completing Plan 2, proceed to [plan_3_fetch_html_dag.plan.md](plan_3_fetch_html_dag.plan.md) (Phase 5: HTML Fetch).