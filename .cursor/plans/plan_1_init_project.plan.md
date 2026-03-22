---
name: Plan 1 – Init Project
overview: Initialize the VnExpress crawler project: directory structure, docker-compose, config skeleton, Localstack init, and Airflow connection/variables. Complete this before implementing DAGs (Plan 2+).
todos: []
isProject: false
---

# Plan 1: Init Project — VnExpress Crawler

This plan initializes the project skeleton. You execute each step manually. After completion, the stack runs and you can proceed to [vnexpress_manual_step_by_step_plan.plan.md](vnexpress_manual_step_by_step_plan.plan.md) Phase 2+.

---

## Phases in This Plan

| Phase | Goal |
|-------|------|
| 1 | Create directory structure |
| 2 | Ensure docker-compose.yml |
| 3 | Create config skeleton (bronze, silver, gold) |
| 4 | Create placeholder utils and tests |
| 5 | Init Localstack (S3 bucket, SQS queue) |
| 6 | Configure Airflow (connection, variables) |

---

## Phase 1: Directory Structure

**Goal:** Create the folder layout for DAGs, configs, utils, and tests.


| Step | Action                                                                                         | Reference                                                                                     |
| ---- | ---------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------- |
| 1.1  | Create `src/dags/vnexpress_full_flow/` (DAG flow folder)                                      | [06-airflow-dags.mdc](.cursor/rules/06-airflow-dags.mdc)                                     |
| 1.2  | Create `src/dags/configs/bronze/`, `src/dags/configs/silver/`, `src/dags/configs/gold/`        | [config-yaml-bronze-silver-gold](.cursor/skills/config-yaml-bronze-silver-gold/SKILL.md)     |
| 1.3  | Create `src/dags/utils/` (shared helpers)                                                    | [resource-reference-codebase-patterns](.cursor/skills/resource-reference-codebase-patterns/SKILL.md) |
| 1.4  | Create `tests/`, `tests/test_data/` (fixtures)                                                | [07-data-quality-and-testing.mdc](.cursor/rules/07-data-quality-and-testing.mdc)             |
| 1.5  | Create `plugins/`, `config/` (empty; used by docker-compose volumes)                           | [12-docker-compose-testing.mdc](.cursor/rules/12-docker-compose-testing.mdc)                 |


**Check:** All directories exist; `src/dags` contains `vnexpress_full_flow`, `configs`, `utils`.

---

## Phase 2: Docker Compose

**Goal:** Stack runs with Postgres, Redis, Localstack, Airflow.


| Step | Action                                                                                         | Reference                                                                                     |
| ---- | ---------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------- |
| 2.1  | Ensure `docker-compose.yml` at project root with services: postgres, redis, localstack, airflow-webserver, airflow-scheduler, airflow-worker, airflow-init | [12-docker-compose-testing.mdc](.cursor/rules/12-docker-compose-testing.mdc)                 |
| 2.2  | Verify volumes: `./src/dags:/opt/airflow/dags`, `./src:/opt/airflow/src`, `./tests:/opt/airflow/tests` | [docker-compose-local-testing](.cursor/skills/docker-compose-local-testing/SKILL.md)          |
| 2.3  | Verify Airflow env: `AWS_ENDPOINT_URL: "http://localstack:4566"`, `AWS_ACCESS_KEY_ID: "test"`, `AWS_SECRET_ACCESS_KEY: "test"` | [12-docker-compose-testing.mdc](.cursor/rules/12-docker-compose-testing.mdc)                 |
| 2.4  | Run `docker compose up -d`; wait for services healthy                                         | [LOCAL_SETUP.md](LOCAL_SETUP.md)                                                             |


**Check:** http://localhost:8080 opens; login admin/admin.

---

## Phase 3: Config Skeleton

**Goal:** YAML config files for bronze, silver, gold (minimal content).


| Step | Action                                                                                         | Reference                                                                                     |
| ---- | ---------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------- |
| 3.1  | Create `src/dags/configs/bronze/vnexpress_bronze.yml` with data_config: seed_urls, allowed_url_patterns, s3_output_prefix, batch_size | [config-yaml-bronze-silver-gold](.cursor/skills/config-yaml-bronze-silver-gold/SKILL.md)     |
| 3.2  | Create `src/dags/configs/silver/vnexpress_silver.yml` with data_config: gemini_model, max_body_chars, batch_size, silver_prefix | [config-yaml-bronze-silver-gold](.cursor/skills/config-yaml-bronze-silver-gold/SKILL.md)     |
| 3.3  | Create `src/dags/configs/gold/vnexpress_gold.yml` with data_config: gold_csv_prefix (optional) | [config-yaml-bronze-silver-gold](.cursor/skills/config-yaml-bronze-silver-gold/SKILL.md)     |


**Check:** Files exist; YAML parses (e.g. `python -c "import yaml; yaml.safe_load(open('src/dags/configs/bronze/vnexpress_bronze.yml'))"`).

---

## Phase 4: Placeholder Utils and Tests

**Goal:** Empty or stub modules so imports work; tests directory ready.


| Step | Action                                                                                         | Reference                                                                                     |
| ---- | ---------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------- |
| 4.1  | Create `src/dags/vnexpress_full_flow/__init__.py` (empty)                                      | [06-airflow-dags.mdc](.cursor/rules/06-airflow-dags.mdc)                                     |
| 4.2  | Create `src/dags/utils/__init__.py` (empty or re-exports)                                      | [resource-reference-codebase-patterns](.cursor/skills/resource-reference-codebase-patterns/SKILL.md) |
| 4.3  | Create `src/dags/utils/helper.py` with stub: `def load_yml_configs(path): ...` (or full impl) | [resource-reference-codebase-patterns](.cursor/skills/resource-reference-codebase-patterns/SKILL.md) |
| 4.4  | Create `src/dags/utils/s3_utils.py` with stub: `def write_parquet_to_s3(...): ...` (or full impl) | [s3-sqs-ingestion](.cursor/skills/s3-sqs-ingestion/SKILL.md)                                 |
| 4.5  | Create `tests/__init__.py` (empty); add `pytest.ini` or `pyproject.toml` with pytest config     | [07-data-quality-and-testing.mdc](.cursor/rules/07-data-quality-and-testing.mdc)             |


**Check:** `from utils.helper import load_yml_configs` works when run from `src/dags` (PYTHONPATH).

---

## Phase 5: Init Localstack

**Goal:** S3 bucket and SQS queue exist in Localstack.


| Step | Action                                                                                         | Reference                                                                                     |
| ---- | ---------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------- |
| 5.1  | Run `AWS_ENDPOINT_URL=http://localhost:4566 awslocal s3 mb s3://vnexpress-data`               | [docker-compose-local-testing](.cursor/skills/docker-compose-local-testing/SKILL.md)          |
| 5.2  | Run `AWS_ENDPOINT_URL=http://localhost:4566 awslocal sqs create-queue --queue-name vnexpress-url-frontier` | [12-docker-compose-testing.mdc](.cursor/rules/12-docker-compose-testing.mdc)                 |


**Check:** `AWS_ENDPOINT_URL=http://localhost:4566 awslocal s3 ls` and `awslocal sqs list-queues` show bucket and queue.

---

## Phase 6: Airflow Connection and Variables

**Goal:** Airflow can talk to Localstack (S3, SQS).


| Step | Action                                                                                         | Reference                                                                                     |
| ---- | ---------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------- |
| 6.1  | Airflow UI → Admin → Connections → Add: `aws_dag_executor`, Amazon Web Services, Login `test`, Password `test`, Extra `{"endpoint_url": "http://localstack:4566"}` | [12-docker-compose-testing.mdc](.cursor/rules/12-docker-compose-testing.mdc)                 |
| 6.2  | Airflow UI → Admin → Variables → Add: `vnexpress_s3_bucket` = `vnexpress-data`, `vnexpress_sqs_queue_url` = `http://localstack:4566/000000000000/vnexpress-url-frontier`, `vnexpress_environment` = `local` | [LOCAL_SETUP.md](LOCAL_SETUP.md) |


**Check:** Connection and variables appear in Airflow UI.

---

## Target Directory Tree (After Init)

```
project-root/
├── docker-compose.yml
├── src/
│   └── dags/
│       ├── vnexpress_full_flow/
│       │   └── __init__.py
│       ├── configs/
│       │   ├── bronze/
│       │   │   └── vnexpress_bronze.yml
│       │   ├── silver/
│       │   │   └── vnexpress_silver.yml
│       │   └── gold/
│       │       └── vnexpress_gold.yml
│       └── utils/
│           ├── __init__.py
│           ├── helper.py
│           └── s3_utils.py
├── tests/
│   ├── __init__.py
│   └── test_data/
├── plugins/          (empty)
├── config/            (empty)
└── .cursor/
    ├── rules/
    └── plans/
```

---

## Next Plan

After completing Plan 1, proceed to [vnexpress_manual_step_by_step_plan.plan.md](vnexpress_manual_step_by_step_plan.plan.md) **Phase 2: Utils** and **Phase 4: Link Discovery (Discover DAG)**.

---

## Key References

- **Docker Compose:** [.cursor/rules/12-docker-compose-testing.mdc](.cursor/rules/12-docker-compose-testing.mdc)
- **Config:** [.cursor/skills/config-yaml-bronze-silver-gold/SKILL.md](.cursor/skills/config-yaml-bronze-silver-gold/SKILL.md)
- **Local setup:** [LOCAL_SETUP.md](LOCAL_SETUP.md)
