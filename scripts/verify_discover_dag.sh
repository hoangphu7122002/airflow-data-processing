#!/bin/bash
# Verify vnexpress_discover_links_dag: set variables, add connection, trigger DAG, check SQS.
# Run from project root: ./scripts/verify_discover_dag.sh
# Prerequisites: docker compose up -d, Localstack init (s3 mb, sqs create-queue)

set -e

echo "=== Setting Airflow variables ==="
docker compose exec -T airflow-scheduler airflow variables set vnexpress_s3_bucket vnexpress-data || true
docker compose exec -T airflow-scheduler airflow variables set vnexpress_sqs_queue_url "http://localstack:4566/000000000000/vnexpress-url-frontier" || true
docker compose exec -T airflow-scheduler airflow variables set vnexpress_environment local || true

echo "=== Adding aws_dag_executor connection (if missing) ==="
docker compose exec -T airflow-scheduler airflow connections add aws_dag_executor \
  --conn-type amazon_web_services \
  --conn-login test \
  --conn-password test \
  --conn-extra '{"endpoint_url": "http://localstack:4566"}' 2>/dev/null || echo "(connection may already exist)"

echo "=== Unpausing and triggering vnexpress_discover_links_dag ==="
docker compose exec -T airflow-scheduler airflow dags unpause vnexpress_discover_links_dag
docker compose exec -T airflow-scheduler airflow dags trigger vnexpress_discover_links_dag

echo "=== Waiting 60s for discover_links task to run ==="
sleep 60

echo "=== Checking SQS for messages ==="
AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=us-east-1 \
  aws --endpoint-url=http://localhost:4566 sqs receive-message \
  --queue-url http://localhost:4566/000000000000/vnexpress-url-frontier \
  --max-number-of-messages 5

echo "=== Done. Verify DAG run in Airflow UI: http://localhost:8080 ==="
