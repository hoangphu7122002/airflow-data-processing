"""Optional DAG trigger endpoint - calls Airflow REST API."""
import os

import httpx
from fastapi import APIRouter, HTTPException

router = APIRouter()


@router.post("/trigger/{dag_id}")
def trigger_dag(dag_id: str):
    """
    Trigger an Airflow DAG via REST API.
    Requires AIRFLOW_URL, AIRFLOW_USER, AIRFLOW_PASSWORD env vars.
    """
    base_url = os.environ.get("AIRFLOW_URL", "http://airflow-webserver:8080")
    user = os.environ.get("AIRFLOW_USER", "admin")
    password = os.environ.get("AIRFLOW_PASSWORD", "admin")

    url = f"{base_url.rstrip('/')}/api/v1/dags/{dag_id}/dagRuns"
    payload = {}

    try:
        with httpx.Client(timeout=30) as client:
            resp = client.post(
                url,
                json=payload,
                auth=(user, password),
                headers={"Content-Type": "application/json"},
            )
            resp.raise_for_status()
            return resp.json()
    except httpx.HTTPStatusError as e:
        raise HTTPException(status_code=e.response.status_code, detail=str(e.response.text))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
