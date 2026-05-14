# Real-Time E-Commerce Order Monitoring Pipeline

End-to-end **Docker Compose** stack that models an event-driven order saga: **Kafka** topics for `orders`, `payments`, `shipments`, `orders.enriched`, and `orders.dlq`; **Apache Airflow** DAGs to produce, enrich, and alert; **Logstash** to index Kafka streams into **Elasticsearch**; **Kibana** for dashboards.

## Architecture

1. **Producer DAG** (`ecommerce_order_events_producer`) every **5 minutes** emits correlated JSON events: order placed → payment outcome → shipment (only on successful payment). Amounts are randomized; some exceed **$5000** to exercise fraud-style flags downstream.
2. **Enrichment DAG** (`ecommerce_orders_enrichment`) every **2 minutes** consumes `orders`, joins to in-memory user/product reference data, sets `is_suspicious` when `amount > 5000`, and publishes to **`orders.enriched`** or **`orders.dlq`** on bad payloads.
3. **Alert DAG** (`ecommerce_payment_failure_alert`) every **10 minutes** queries **`ecommerce-payments-*`** in Elasticsearch for recent `FAILED` share and logs a **fake alert** if the rate exceeds `PAYMENT_FAILURE_RATE_THRESHOLD` (and there is enough volume).
4. **Logstash** tails **`orders.enriched`**, **`payments`**, and **`shipments`** into daily indices: `ecommerce-orders-enriched-*`, `ecommerce-payments-*`, `ecommerce-shipments-*`.

## Prerequisites

- Docker Desktop (or Docker Engine + Compose v2) with enough RAM for Elasticsearch + Kafka + Airflow (roughly **8 GB+** recommended).

## Quick start

From this directory:

```bash
docker compose up -d
```

Wait for health checks (especially Elasticsearch and Kafka). Then open:

| Service | URL | Notes |
|--------|-----|--------|
| Airflow | http://localhost:8080 | Default user `airflow` / password `airflow` (set `_AIRFLOW_WWW_USER_*` in `.env` to change) |
| Kibana | http://localhost:5601 | Create a **Data view** like `ecommerce-*` |
| Elasticsearch | http://localhost:9200 | Security disabled for local dev |
| Kafka (host) | `localhost:9092` | Inside Compose, services use `kafka:29092` |

Airflow DAGs are **unpaused by default** (`AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=false`). Trigger the producer once manually if you do not want to wait for the cron tick.

## Kibana starter ideas

- **Data view**: `ecommerce-*`
- **Lens**: count of documents over time split by `event_type.keyword`
- **Lens**: sum of `amount` on `ecommerce-orders-enriched-*` by minute (revenue proxy)
- **Lens**: filter `status.keyword = FAILED` on payments index for failure trends

## Configuration

See `.env.example` for variables copied into `.env`:

- `KAFKA_BOOTSTRAP_SERVERS` — broker list for DAGs (Compose default `kafka:29092`)
- `ELASTICSEARCH_URL` — for the alert DAG
- `PAYMENT_FAILURE_RATE_THRESHOLD` — decimal fraction (default `0.25`)
- `ALERT_LOOKBACK_MINUTES` — ES range window (default `15`)
- `FERNET_KEY` — required stable key across restarts; generate with `python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"`

## Inspecting Kafka locally

```bash
docker exec -it ecommerce-kafka kafka-console-consumer --bootstrap-server localhost:29092 --topic orders --from-beginning --max-messages 5
```

Replace `orders` with `payments`, `shipments`, `orders.enriched`, or `orders.dlq`.

## Shut down

```bash
docker compose down
```

To also remove the Postgres volume: `docker compose down -v`.

## Project layout

- `docker-compose.yml` — stack topology
- `airflow/` — custom Airflow image (`confluent-kafka`, `faker`, `elasticsearch`)
- `dags/` — DAG definitions and `order_pipeline_common.py`
- `logstash/pipeline/logstash.conf` — Kafka → Elasticsearch routing
