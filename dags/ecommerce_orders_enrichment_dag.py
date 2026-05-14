"""
Airflow DAG: consume raw orders, enrich (join + fraud flag), emit enriched or DLQ.
"""
from __future__ import annotations

import json
import logging
import random
from datetime import datetime, timedelta
from typing import Any

from airflow import DAG
from airflow.operators.python import PythonOperator

from order_pipeline_common import (
    FAKE_PRODUCTS,
    FAKE_USERS,
    flush_producer,
    kafka_consumer,
    kafka_producer,
    produce_json,
    utc_now_iso,
)

logger = logging.getLogger(__name__)

MAX_MESSAGES = 400
POLL_TIMEOUT_SEC = 8.0


def enrich_orders_batch() -> dict[str, Any]:
    consumer = kafka_consumer(
        "airflow-order-enrichment",
        topics=["orders"],
    )
    producer = kafka_producer()
    processed = 0
    dlq = 0

    try:
        end_at = MAX_MESSAGES
        while processed < end_at:
            msg = consumer.poll(POLL_TIMEOUT_SEC)
            if msg is None:
                break
            if msg.error():
                logger.warning("Kafka consumer error: %s", msg.error())
                break

            raw = msg.value()
            if raw is None:
                continue

            try:
                payload = json.loads(raw.decode("utf-8"))
            except json.JSONDecodeError as exc:
                dlq += 1
                produce_json(
                    producer,
                    "orders.dlq",
                    {
                        "reason": "invalid_json",
                        "error": str(exc),
                        "raw_preview": raw.decode("utf-8", errors="replace")[:2000],
                        "failed_at": utc_now_iso(),
                    },
                )
                continue

            order_id = payload.get("order_id")
            user_id = payload.get("user_id")
            sku = payload.get("product_sku")
            amount = payload.get("amount")

            if not order_id or amount is None:
                dlq += 1
                produce_json(
                    producer,
                    "orders.dlq",
                    {
                        "reason": "missing_required_fields",
                        "original": payload,
                        "failed_at": utc_now_iso(),
                    },
                )
                continue

            user_profile = FAKE_USERS.get(user_id)
            product_profile = FAKE_PRODUCTS.get(sku or "")

            if user_profile is None:
                user_profile = {
                    "name": "unknown",
                    "country": "unknown",
                    "tier": "unknown",
                }

            if product_profile is None:
                product_profile = {
                    "name": payload.get("product_name") or "unknown",
                    "category": payload.get("category") or "unknown",
                }

            try:
                amount_f = float(amount)
            except (TypeError, ValueError):
                dlq += 1
                produce_json(
                    producer,
                    "orders.dlq",
                    {
                        "reason": "invalid_amount",
                        "original": payload,
                        "failed_at": utc_now_iso(),
                    },
                )
                continue

            is_suspicious = amount_f > 5000.0
            risk_score = round(min(1.0, max(0.0, (amount_f / 10000.0) + (0.25 if is_suspicious else 0.0))), 3)

            enriched = {
                **payload,
                "user_profile": user_profile,
                "product_profile": product_profile,
                "is_suspicious": is_suspicious,
                "risk_score": risk_score,
                "enrichment_version": "v1",
                "enriched_at": utc_now_iso(),
                "sampled_qa_flag": random.random() < 0.02,
            }

            produce_json(producer, "orders.enriched", enriched)
            processed += 1
    finally:
        flush_producer(producer)
        consumer.close()

    logger.info("Enrichment batch complete processed=%s dlq=%s", processed, dlq)
    return {"processed": processed, "dlq": dlq}


default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=20),
}

with DAG(
    dag_id="ecommerce_orders_enrichment",
    default_args=default_args,
    description="Consumes orders, enriches with reference joins, emits enriched/DLQ topics.",
    schedule_interval="*/2 * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["ecommerce", "kafka", "enrichment", "dlq"],
) as dag:
    PythonOperator(
        task_id="enrich_orders_from_kafka",
        python_callable=enrich_orders_batch,
    )
