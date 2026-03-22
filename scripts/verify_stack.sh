#!/bin/bash
# Verify Docker stack: build, up, health, LocalStack init, Airflow vars, pytest.
# Run from project root: ./scripts/verify_stack.sh

set -e
cd "$(dirname "$0")/.."

echo "=== 1. Stopping any existing stack ==="
docker compose down 2>/dev/null || true

echo ""
echo "=== 2. Building images (minimal deps; use SKIP_BUILD=1 to skip) ==="
if [ -z "${SKIP_BUILD:-}" ]; then
  docker compose build 2>&1 | tail -40
else
  echo "(skipped - images already built)"
fi

echo ""
echo "=== 3. Starting stack ==="
docker compose up -d

echo ""
echo "=== 4. Waiting for Postgres and Redis (30s) ==="
sleep 30

echo ""
echo "=== 5. Checking container status ==="
docker compose ps

echo ""
echo "=== 6. Waiting for Airflow init to complete (90s) ==="
sleep 90

echo ""
echo "=== 7. Initialize LocalStack (bucket + queue) ==="
AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=us-east-1 \
  aws --endpoint-url=http://localhost:4566 s3 mb s3://vnexpress-data 2>/dev/null || echo "(bucket may exist)"
AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=us-east-1 \
  aws --endpoint-url=http://localhost:4566 sqs create-queue --queue-name vnexpress-url-frontier 2>/dev/null || echo "(queue may exist)"

echo ""
echo "=== 8. Set Airflow variables and connection ==="
docker compose exec -T airflow-scheduler airflow variables set vnexpress_s3_bucket vnexpress-data 2>/dev/null || true
docker compose exec -T airflow-scheduler airflow variables set vnexpress_sqs_queue_url "http://localstack:4566/000000000000/vnexpress-url-frontier" 2>/dev/null || true
docker compose exec -T airflow-scheduler airflow variables set vnexpress_environment local 2>/dev/null || true
docker compose exec -T airflow-scheduler airflow connections add aws_dag_executor \
  --conn-type amazon_web_services --conn-login test --conn-password test \
  --conn-extra '{"endpoint_url": "http://localstack:4566"}' 2>/dev/null || echo "(connection may exist)"

echo ""
echo "=== 9. Run pytest (in airflow-scheduler) ==="
docker compose exec -T airflow-scheduler bash -c "cd /opt/airflow && PYTHONPATH=/opt/airflow:/opt/airflow/dags pytest tests/ -v --tb=short" 2>&1 | tail -100 || true

echo ""
echo "=== 10. Quick container health check ==="
for svc in postgres redis localstack airflow-webserver airflow-scheduler; do
  if docker compose ps "$svc" 2>/dev/null | grep -q "Up"; then
    echo "  OK $svc"
  else
    echo "  ?? $svc (check: docker compose logs $svc)"
  fi
done

echo ""
echo "=== Done. Airflow UI: http://localhost:8080 ==="
