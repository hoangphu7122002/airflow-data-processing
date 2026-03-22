# Airflow image for VnExpress crawler (CeleryExecutor)
# Uses minimal deps for faster build; for full: docker compose build --build-arg REQS=requirements_local.txt
FROM python:3.12-slim
ARG REQS=requirements_local_minimal.txt

ENV AIRFLOW_HOME=/opt/airflow
ENV AIRFLOW_UID=50000

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/* \
    && useradd -ms /bin/bash -u ${AIRFLOW_UID} airflow

WORKDIR ${AIRFLOW_HOME}

COPY requirements_local.txt requirements_local_minimal.txt ./
RUN pip install --no-cache-dir -r ${REQS}

# Celery workers / scheduler run as AIRFLOW_UID (50000) and must write logs here.
# Without this, FileProcessorHandler fails: PermissionError on /opt/airflow/logs.
RUN mkdir -p \
    "${AIRFLOW_HOME}/logs" \
    "${AIRFLOW_HOME}/dags" \
    "${AIRFLOW_HOME}/plugins" \
    "${AIRFLOW_HOME}/config" \
    && chown -R airflow:root "${AIRFLOW_HOME}" \
    && chmod -R u+rwX "${AIRFLOW_HOME}"

USER airflow

EXPOSE 8080 5555

ENTRYPOINT ["airflow"]
CMD ["version"]
