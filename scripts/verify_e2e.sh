#!/bin/bash
# End-to-end verification: run discover → fetch → silver → gold, verify all layers.
# Run from project root: ./scripts/verify_e2e.sh
# Prerequisites: docker compose up -d, Localstack init (s3 mb, sqs create-queue), gemini_api_key set for silver

set -e

SLEEP_DISCOVER=60
SLEEP_FETCH=90
SLEEP_SILVER=120
SLEEP_GOLD=60

echo "=== E2E: Ensure variables and connection ==="
docker compose exec -T airflow-scheduler airflow variables set vnexpress_s3_bucket vnexpress-data 2>/dev/null || true
docker compose exec -T airflow-scheduler airflow variables set vnexpress_sqs_queue_url "http://localstack:4566/000000000000/vnexpress-url-frontier" 2>/dev/null || true
docker compose exec -T airflow-scheduler airflow variables set vnexpress_environment local 2>/dev/null || true

docker compose exec -T airflow-scheduler airflow connections add aws_dag_executor \
  --conn-type amazon_web_services \
  --conn-login test \
  --conn-password test \
  --conn-extra '{"endpoint_url": "http://localstack:4566"}' 2>/dev/null || echo "(connection may exist)"

echo ""
echo "=== 1/4 Trigger discover_links_dag ==="
docker compose exec -T airflow-scheduler airflow dags unpause vnexpress_discover_links_dag
docker compose exec -T airflow-scheduler airflow dags trigger vnexpress_discover_links_dag
echo "Waiting ${SLEEP_DISCOVER}s for discover..."
sleep $SLEEP_DISCOVER

echo ""
echo "=== 2/4 Trigger fetch_html_dag ==="
docker compose exec -T airflow-scheduler airflow dags unpause vnexpress_fetch_html_dag
docker compose exec -T airflow-scheduler airflow dags trigger vnexpress_fetch_html_dag
echo "Waiting ${SLEEP_FETCH}s for fetch..."
sleep $SLEEP_FETCH

echo ""
echo "=== 3/4 Trigger silver_gemini_dag ==="
echo "(Requires gemini_api_key Variable; set in Airflow UI if not already)"
docker compose exec -T airflow-scheduler airflow dags unpause vnexpress_silver_gemini_dag
docker compose exec -T airflow-scheduler airflow dags trigger vnexpress_silver_gemini_dag
echo "Waiting ${SLEEP_SILVER}s for silver (Gemini extraction)..."
sleep $SLEEP_SILVER

echo ""
echo "=== 4/4 Trigger gold_load_dag ==="
docker compose exec -T airflow-scheduler airflow dags unpause vnexpress_gold_load_dag
docker compose exec -T airflow-scheduler airflow dags trigger vnexpress_gold_load_dag
echo "Waiting ${SLEEP_GOLD}s for gold..."
sleep $SLEEP_GOLD

echo ""
echo "=== Verify SQS drained (approx 0 messages after fetch consumed them) ==="
AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=us-east-1 \
  aws --endpoint-url=http://localhost:4566 sqs get-queue-attributes \
  --queue-url http://localhost:4566/000000000000/vnexpress-url-frontier \
  --attribute-names ApproximateNumberOfMessages 2>/dev/null || echo "SQS check skipped"

echo ""
echo "=== Verify S3 bronze has HTML ==="
AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=us-east-1 \
  aws --endpoint-url=http://localhost:4566 s3 ls s3://vnexpress-data/vnexpress/bronze/ --recursive || true

echo ""
echo "=== Verify S3 silver has Parquet ==="
AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=us-east-1 \
  aws --endpoint-url=http://localhost:4566 s3 ls s3://vnexpress-data/vnexpress/silver/ --recursive || true

echo ""
echo "=== Verify S3 gold has Parquet ==="
AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=us-east-1 \
  aws --endpoint-url=http://localhost:4566 s3 ls s3://vnexpress-data/vnexpress/gold/ --recursive || true

if docker compose exec -T airflow-scheduler airflow variables get clickhouse_enabled 2>/dev/null | grep -qi true; then
  echo ""
  echo "=== ClickHouse enabled: query row count ==="
  docker compose exec -T clickhouse clickhouse-client --query "SELECT count() FROM vnexpress_articles" 2>/dev/null || echo "ClickHouse query skipped"
fi

echo ""
echo "=== E2E verification done. Check Airflow UI: http://localhost:8080 ==="
