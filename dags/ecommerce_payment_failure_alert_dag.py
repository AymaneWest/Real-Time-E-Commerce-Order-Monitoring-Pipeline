"""
Airflow DAG: threshold check on recent payment failures indexed in Elasticsearch.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from elasticsearch import Elasticsearch

logger = logging.getLogger(__name__)

ES_URL = os.environ.get("ELASTICSEARCH_URL", "http://elasticsearch:9200")
THRESHOLD = float(os.environ.get("PAYMENT_FAILURE_RATE_THRESHOLD", "0.25"))
LOOKBACK_MIN = int(os.environ.get("ALERT_LOOKBACK_MINUTES", "15"))


def evaluate_payment_failure_rate() -> dict:
    es = Elasticsearch(ES_URL, request_timeout=30)

    resp = es.search(
        index="ecommerce-payments-*",
        size=0,
        query={
            "bool": {
                "filter": [
                    {"range": {"@timestamp": {"gte": f"now-{LOOKBACK_MIN}m"}}},
                    {"term": {"event_type.keyword": "payment"}},
                ]
            }
        },
        aggs={
            "by_status": {
                "terms": {"field": "status.keyword", "size": 10},
            }
        },
    )
    buckets = (resp.get("aggregations") or {}).get("by_status", {}).get("buckets", [])

    total = 0
    failed = 0
    for b in buckets:
        c = int(b.get("doc_count", 0))
        total += c
        if b.get("key") == "FAILED":
            failed = c

    rate = (failed / total) if total else 0.0
    alert = rate >= THRESHOLD and total >= 8

    payload = {
        "lookback_minutes": LOOKBACK_MIN,
        "threshold": THRESHOLD,
        "total_payments": total,
        "failed_payments": failed,
        "failure_rate": round(rate, 4),
        "alert_triggered": alert,
    }

    if alert:
        logger.warning(
            "FAKE ALERT: payment failure rate %.2f%% exceeds threshold %.2f%% over last %s minutes (%s/%s)",
            rate * 100,
            THRESHOLD * 100,
            LOOKBACK_MIN,
            failed,
            total,
        )
    else:
        logger.info("Payment health OK: %s", payload)

    return payload


default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=20),
}

with DAG(
    dag_id="ecommerce_payment_failure_alert",
    default_args=default_args,
    description="Queries Elasticsearch for payment failure rate; logs a fake alert if high.",
    schedule_interval="*/10 * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["ecommerce", "elasticsearch", "alerting"],
) as dag:
    PythonOperator(
        task_id="check_payment_failure_rate",
        python_callable=evaluate_payment_failure_rate,
    )
