---
name: docker-compose-local-testing
description: >-
  All services run via Docker Compose: S3, SQS (Localstack), Airflow, Postgres,
  Redis. Use when testing, developing, or configuring the pipeline locally.
---

# Docker Compose Local Testing

**All services run in Docker Compose.** S3 and SQS are provided by Localstack. No real AWS needed for local dev/test.

## Stack (docker-compose)

- **Postgres** – Airflow metadata
- **Redis** – Celery broker
- **Localstack** – S3 + SQS (port 4566)
- **Airflow** – webserver, scheduler, worker, triggerer, init

## Start Stack

```bash
docker compose up -d
```

## Localstack: S3 + SQS

Localstack runs in Docker; other services reach it at `http://localstack:4566`.

**Init bucket and queue** (run once after Localstack is up):

```bash
AWS_ENDPOINT_URL=http://localhost:4566 awslocal s3 mb s3://vnexpress-data
AWS_ENDPOINT_URL=http://localhost:4566 awslocal sqs create-queue --queue-name vnexpress-url-frontier
```

Or use an init script that runs these commands.

## Airflow → Localstack

**Environment** (in docker-compose for Airflow services):

```yaml
AWS_ACCESS_KEY_ID: "test"
AWS_SECRET_ACCESS_KEY: "test"
AWS_DEFAULT_REGION: "us-east-1"
AWS_ENDPOINT_URL: "http://localstack:4566"
```

**Connection** (Airflow UI → Connections):

- Connection Id: `aws_dag_executor`
- Type: Amazon Web Services
- Login: `test`
- Password: `test`
- Extra: `{"endpoint_url": "http://localstack:4566"}`

**Variables** (Airflow UI → Variables):

| Key | Value |
|-----|-------|
| `vnexpress_s3_bucket` | `vnexpress-data` |
| `vnexpress_sqs_queue_url` | `http://localstack:4566/000000000000/vnexpress-url-frontier` |
| `vnexpress_environment` | `local` |

## DAG Code (unchanged)

DAGs use `S3Hook(aws_conn_id="aws_dag_executor")` and `SqsHook(aws_conn_id="aws_dag_executor")`. Same code runs against Localstack (local) or real AWS (prod) by changing connection and Variables only.

## Service Discovery

Within Docker network, services use service names:

- `localstack:4566` – S3, SQS
- `postgres:5432` – Airflow DB
- `redis:6379` – Celery broker

## Run Tests (in Docker)

```bash
docker compose run --rm airflow-cli bash -c "PYTHONPATH=.:src/dags pytest tests/"
```

Or exec into a running container and run pytest.
