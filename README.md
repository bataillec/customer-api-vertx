# customer-api-vertx

Reactive Scala 3 / Vert.x microservice for customers, subscriptions and entitlements. See platform/docs/ARCHITECTURE.md.

## Audit

Audit events are written to a Kafka topic as platform event envelopes v1 (`event_id`, `event_type`, `occurred_at`, `producer`, `schema_version`, `customer_id` and extras). The record key is the `customer_id`.

| Variable | Default | Meaning |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka brokers |
| `AUDIT_TOPIC` | `customer.events.v1` | Topic receiving the audit events |

Start a local broker with `podman compose up -d`, then read the topic with:

```
podman compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic customer.events.v1 --from-beginning --property print.key=true
```
