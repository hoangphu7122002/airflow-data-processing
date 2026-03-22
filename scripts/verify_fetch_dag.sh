#!/bin/bash
# Verify vnexpress_fetch_html_dag: run discover (if needed), trigger fetch, check S3 bronze.
# Run from project root: ./scripts/verify_fetch_dag.sh
# Prerequisites: docker compose up -d, Localstack init, Plan 2 verify (discover populates SQS)

set -e

echo "=== Ensure variables and connection (from Plan 1/2) ==="
docker compose exec -T airflow-scheduler airflow variables set vnexpress_s3_bucket vnexpress-data 2>/dev/null || true
docker compose exec -T airflow-scheduler airflow variables set vnexpress_sqs_queue_url "http://localstack:4566/000000000000/vnexpress-url-frontier" 2>/dev/null || true

echo "=== Optionally run discover DAG to populate SQS (if queue empty) ==="
docker compose exec -T airflow-scheduler airflow dags unpause vnexpress_discover_links_dag 2>/dev/null || true
docker compose exec -T airflow-scheduler airflow dags trigger vnexpress_discover_links_dag 2>/dev/null || true
echo "Waiting 60s for discover to complete..."
sleep 60

echo "=== Unpausing and triggering vnexpress_fetch_html_dag ==="
docker compose exec -T airflow-scheduler airflow dags unpause vnexpress_fetch_html_dag
docker compose exec -T airflow-scheduler airflow dags trigger vnexpress_fetch_html_dag

echo "=== Waiting 90s for fetch_html_worker tasks to run ==="
sleep 90

echo "=== Checking S3 bronze for HTML files ==="
AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=us-east-1 \
  aws --endpoint-url=http://localhost:4566 s3 ls s3://vnexpress-data/vnexpress/bronze/ --recursive

echo "=== Done. Verify DAG run in Airflow UI: http://localhost:8080 ==="
