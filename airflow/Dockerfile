FROM apache/airflow:2.8.3-python3.11

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential librdkafka-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
COPY requirements.txt /opt/airflow/requirements.txt
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt

ENV PYTHONPATH="/opt/airflow/dags:${PYTHONPATH}"
