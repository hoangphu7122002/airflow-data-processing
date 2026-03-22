# Local setup (VnExpress crawler)

## 1. Start the stack

From project root:

```bash
docker compose up -d
```

Services: Postgres, Redis, Localstack, ClickHouse, Airflow (webserver, scheduler, worker, init), FastAPI (Plan 8).

## 2. Init Localstack (bucket + queue)

After Localstack is up (port 4566), run once:

```bash
AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=us-east-1 \
  aws --endpoint-url=http://localhost:4566 s3 mb s3://vnexpress-data

AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=us-east-1 \
  aws --endpoint-url=http://localhost:4566 sqs create-queue --queue-name vnexpress-url-frontier
```

(Or use `awslocal` if installed: `AWS_ENDPOINT_URL=http://localhost:4566 awslocal s3 mb s3://vnexpress-data`)

## 3. Airflow UI: Variables and connection

Open **http://localhost:8080** (admin / admin or your configured user).

### Variables

Admin → Variables → Add:

| Key | Value (local) |
|-----|----------------|
| `vnexpress_s3_bucket` | `vnexpress-data` |
| `vnexpress_sqs_queue_url` | `http://localstack:4566/000000000000/vnexpress-url-frontier` |
| `vnexpress_environment` | `local` |
| `gemini_api_key` | *(your Google AI API key; add when doing Phase 3 silver)* |
| `gemini_model` | `gemini-2.5-flash` *(optional; config fallback)* |
| `clickhouse_enabled` | `true` *(to load gold into ClickHouse; default false)* |

### Connection

Admin → Connections → Add:

- **Connection Id**: `aws_dag_executor`
- **Connection Type**: Amazon Web Services
- **Login**: `test`
- **Password**: `test`
- **Extra**: `{"endpoint_url": "http://localstack:4566"}`

This makes S3Hook and SqsHook use Localstack. For production, remove or override `endpoint_url` and set real bucket/queue in Variables.

## 4. Run DAGs (after Plan 2+)

1. Trigger `vnexpress_discover_links_dag` → fills SQS with article URLs.
2. Trigger `vnexpress_fetch_html_dag` → workers drain SQS, save HTML to S3 bronze.
3. (Phase 3) Trigger `vnexpress_silver_gemini_dag` → Gemini extracts structured data to silver.
4. (Phase 4) Trigger `vnexpress_gold_load_dag` → dedupes silver, writes gold Parquet to S3; if `clickhouse_enabled=true`, loads into ClickHouse.

### Verify discover DAG (Plan 2)

Run the verification script to set variables, add connection, trigger the DAG, and check SQS:

```bash
./scripts/verify_discover_dag.sh
```

Or manually: set variables and connection in UI, trigger `vnexpress_discover_links_dag`, then:

```bash
AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=us-east-1 \
  aws --endpoint-url=http://localhost:4566 sqs receive-message \
  --queue-url http://localhost:4566/000000000000/vnexpress-url-frontier \
  --max-number-of-messages 5
```

### Verify fetch DAG (Plan 3)

After discover has populated SQS, run the fetch verification script:

```bash
./scripts/verify_fetch_dag.sh
```

Or manually: trigger `vnexpress_discover_links_dag`, wait for completion; then trigger `vnexpress_fetch_html_dag`. Check S3 bronze:

```bash
AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=us-east-1 \
  aws --endpoint-url=http://localhost:4566 s3 ls s3://vnexpress-data/vnexpress/bronze/ --recursive
```

### Verify gold DAG (Plan 5)

After discover → fetch → silver have run, trigger `vnexpress_gold_load_dag` (use logical date = ingestion_date with silver data). Check S3 gold:

```bash
aws --endpoint-url=http://localhost:4566 s3 ls s3://vnexpress-data/vnexpress/gold/ --recursive
```

With ClickHouse enabled (`clickhouse_enabled=true`):

```bash
docker compose exec clickhouse clickhouse-client --query "SELECT count(), section FROM vnexpress_articles GROUP BY section"
```

## 5. Run tests

Run the full test suite inside the Airflow container:

```bash
./scripts/run_tests.sh
```

Or manually:

```bash
docker compose exec -T airflow-scheduler bash -c "cd /opt/airflow && PYTHONPATH=.:dags pytest tests/ -v"
```

## 6. Full pipeline (E2E)

To run the full pipeline (discover → fetch → silver → gold) and verify all layers:

```bash
./scripts/verify_e2e.sh
```

This script triggers each DAG in sequence, waits for completion, then verifies SQS, S3 bronze (HTML), S3 silver (Parquet), S3 gold (Parquet), and optionally ClickHouse row count. Ensure `gemini_api_key` is set in Airflow Variables before running (required for silver extraction). With `.env` containing `GEMINI_API_KEY`, airflow-init will set it automatically.

### Test Gemini in worker

To verify Gemini extraction works inside the worker:

```bash
./scripts/test_gemini_worker.sh
```

## 7. API (FastAPI)

The VnExpress article API runs on port 8000. Start with: `docker compose up -d fastapi`.

| Endpoint | Description |
|----------|-------------|
| `GET http://localhost:8000/health` | Health check |
| `GET http://localhost:8000/articles` | List articles (query params: `section`, `limit`, `offset`, `date_from`, `date_to`) |
| `GET http://localhost:8000/articles/{article_id}` | Get single article |
| `POST http://localhost:8000/trigger/{dag_id}` | Trigger an Airflow DAG (optional; requires `AIRFLOW_URL`, `AIRFLOW_USER`, `AIRFLOW_PASSWORD`) |
| `GET http://localhost:8000/docs` | OpenAPI (Swagger) documentation |
| `GET http://localhost:8000/redoc` | ReDoc documentation |

Data is read from ClickHouse (`vnexpress_articles` table). Ensure the gold DAG has run with `clickhouse_enabled=true` to populate data.
