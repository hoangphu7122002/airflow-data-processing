#!/bin/bash
# Run full test suite in Docker (Airflow container).
# Usage: ./scripts/run_tests.sh
# Prerequisites: docker compose up -d

set -e

echo "=== Running pytest in Airflow container ==="
docker compose exec -T airflow-scheduler bash -c \
  "cd /opt/airflow && PYTHONPATH=.:dags pytest tests/ -v"

echo "=== Tests complete ==="
