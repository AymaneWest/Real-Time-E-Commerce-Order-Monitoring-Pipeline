"""
Airflow DAG: synthetic multi-topic order saga (orders → payments → shipments).
"""
from __future__ import annotations

import logging
import random
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from faker import Faker

from order_pipeline_common import (
    FAKE_PRODUCTS,
    FAKE_USERS,
    flush_producer,
    kafka_producer,
    new_id,
    produce_json,
    utc_now_iso,
)

logger = logging.getLogger(__name__)

faker = Faker()


def generate_order_saga_batch() -> None:
    producer = kafka_producer()
    batch_size = random.randint(18, 42)
    user_ids = list(FAKE_USERS.keys())
    skus = list(FAKE_PRODUCTS.keys())

    for _ in range(batch_size):
        order_id = new_id("ord")
        user_id = random.choice(user_ids)
        sku = random.choice(skus)
        product = FAKE_PRODUCTS[sku]
        qty = random.randint(1, 4)
        unit_price = round(random.uniform(9.99, 1200.0), 2)
        if random.random() < 0.08:
            unit_price = round(random.uniform(2500.0, 8900.0), 2)
        amount = round(unit_price * qty, 2)

        order_payload = {
            "order_id": order_id,
            "user_id": user_id,
            "product_sku": sku,
            "product_name": product["name"],
            "category": product["category"],
            "quantity": qty,
            "amount": amount,
            "currency": "USD",
            "status": "PLACED",
            "shipping_city": faker.city(),
            "created_at": utc_now_iso(),
        }
        produce_json(producer, "orders", order_payload)

        payment_ok = random.random() > 0.12
        payment_payload = {
            "order_id": order_id,
            "payment_id": new_id("pay"),
            "status": "SUCCESS" if payment_ok else "FAILED",
            "amount": amount,
            "currency": "USD",
            "processor": random.choice(["stripe", "adyen", "braintree"]),
            "decline_reason": None if payment_ok else random.choice(["insufficient_funds", "stolen_card", "issuer_declined"]),
            "processed_at": utc_now_iso(),
        }
        produce_json(producer, "payments", payment_payload)

        if payment_ok:
            shipment_payload = {
                "order_id": order_id,
                "shipment_id": new_id("ship"),
                "status": random.choice(["DISPATCHED", "DELIVERED"]),
                "warehouse": random.choice(["SEA-1", "DFW-2", "ORD-1", "LHR-1"]),
                "carrier": random.choice(["UPS", "FedEx", "DHL"]),
                "updated_at": utc_now_iso(),
            }
            produce_json(producer, "shipments", shipment_payload)

    flush_producer(producer)
    logger.info("Published saga batch size=%s (orders+payments+conditional shipments)", batch_size)


default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
}

with DAG(
    dag_id="ecommerce_order_events_producer",
    default_args=default_args,
    description="Simulates correlated e-commerce events into Kafka topics.",
    schedule_interval="*/5 * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["ecommerce", "kafka", "producer"],
) as dag:
    PythonOperator(
        task_id="emit_order_saga_events",
        python_callable=generate_order_saga_batch,
    )
