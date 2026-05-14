"""
Shared helpers for the e-commerce order monitoring DAGs.
"""
from __future__ import annotations

import json
import os
import uuid
from datetime import datetime, timezone
from typing import Any

from confluent_kafka import Consumer, Producer

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")

# Synthetic reference data for joins (simulates warehouse DB / MDM).
FAKE_USERS: dict[str, dict[str, Any]] = {
    "u-1001": {"name": "Avery Chen", "country": "US", "tier": "gold"},
    "u-1002": {"name": "Jordan Smith", "country": "CA", "tier": "silver"},
    "u-1003": {"name": "Riley Johnson", "country": "GB", "tier": "bronze"},
    "u-1004": {"name": "Morgan Lee", "country": "US", "tier": "platinum"},
    "u-1005": {"name": "Casey Rivera", "country": "DE", "tier": "silver"},
}

FAKE_PRODUCTS: dict[str, dict[str, Any]] = {
    "sku-500": {"name": "Noise-Canceling Headphones", "category": "electronics"},
    "sku-501": {"name": "Mechanical Keyboard", "category": "electronics"},
    "sku-502": {"name": "Ergonomic Chair", "category": "furniture"},
    "sku-503": {"name": "Stainless Cookware Set", "category": "home"},
    "sku-504": {"name": "Running Shoes", "category": "apparel"},
    "sku-505": {"name": "Smart Watch", "category": "electronics"},
}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def new_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


def kafka_producer() -> Producer:
    return Producer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "client.id": "airflow-ecommerce-producer",
            "enable.idempotence": True,
        }
    )


def kafka_consumer(group_id: str, topics: list[str]) -> Consumer:
    c = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
        }
    )
    c.subscribe(topics)
    return c


def produce_json(producer: Producer, topic: str, payload: dict[str, Any]) -> None:
    key = str(payload.get("order_id") or payload.get("shipment_id") or payload.get("payment_id") or "")
    producer.produce(
        topic,
        key=key.encode("utf-8") if key else None,
        value=json.dumps(payload).encode("utf-8"),
    )


def flush_producer(producer: Producer, timeout: float = 30.0) -> None:
    producer.flush(timeout=timeout)
