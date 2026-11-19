# customer-api-vertx

Reactive Scala 3 / Vert.x microservice that manages customers, their plan subscriptions and the usage quotas (entitlements) that come with them. See platform/docs/ARCHITECTURE.md for the wider platform.

- **Customers:** create, read, update (PATCH), soft-delete and list with keyset pagination.
- **Subscriptions and entitlements:** a customer subscribes to a plan, and each plan grants quotas per feature. Consuming a feature is checked and updated atomically in Postgres, and quotas reset per period (except `STORAGE_GB`, which measures what is held).
- **Audit:** every change is written to a Kafka topic as a platform event envelope.
- **Architecture:** an HTTP verticle validates requests against `openapi.yaml` and talks to the customer and audit verticles over the clustered (Hazelcast) event bus. Postgres is the store, with Flyway migrations.

The API contract is `src/main/resources/openapi.yaml`, and a running instance serves it at `GET /openapi.json`.

## Setup

Prerequisites:

- Java 25 and Maven 3.10 (`mise install` sets both up from `mise.toml`)
- A PostgreSQL database. The defaults are host `localhost:5432`, database, user and password all `customers`. Override with `PG_HOST`, `PG_PORT`, `PG_DATABASE`, `PG_USER`, `PG_PASSWORD` and `PG_POOL_SIZE`.
- A Kafka broker for the audit topic: `podman compose up -d` (or `docker compose up -d`) starts one on `localhost:9092`.

The schema is created by Flyway when the customer verticle starts, so there is nothing to run by hand.

Build the shaded jar (the tests start Postgres with Testcontainers, so they need a running Docker or Podman; add `-DskipTests` to skip them):

```
mvn package
```

Then start the three processes described in the next section.

## Running with Docker

`compose.full.yaml` builds one image and starts the whole platform: Postgres, Kafka (KRaft), one HTTP verticle, two customer verticles and one audit verticle.

```
podman compose -f compose.full.yaml up -d --build      # or: docker compose -f compose.full.yaml up -d --build
curl localhost:8080/health/ready
```

- Set `HTTP_PORT` to publish the API on another host port (`HTTP_PORT=18081 podman compose ...`).
- The tests are not run during the image build (they need Testcontainers). Run `mvn test` separately.
- Hazelcast (5701) is not published: the verticles find each other by service name on the compose network, using `docker/cluster.xml`.
- `podman compose -f compose.full.yaml down -v` stops everything and deletes the data.

## Running without Docker

The service is three verticles that talk over the clustered (Hazelcast) event bus. The shaded jar's entry point only starts `HttpServerVerticle`, so on its own it answers every API call with 503 `NO_HANDLERS`. Start one process per verticle, all with the same cluster options:

```
CP=target/customer-api-vertx-1.0.0-SNAPSHOT.jar
java -jar $CP -c                                               # HttpServerVerticle (HTTP_PORT, default 8080)
java -cp $CP io.vertx.launcher.application.VertxApplication me.bataillec.portfolio.customer.CustomerVerticle -c
java -cp $CP io.vertx.launcher.application.VertxApplication me.bataillec.portfolio.customer.AuditVerticle -c
```

`GET /health/ready` is UP once the three verticles have joined the cluster. `GET /openapi.json` serves the API contract.

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

## Known limitations

These are deliberate scope cuts for a portfolio project, not oversights. Both must be addressed before any real deployment.

### No authentication

- **HTTP API:** no endpoint is authenticated or authorised. Anyone who can reach the HTTP port can create, update and delete customers and consume quotas. Production would put it behind an API gateway or add JWT/OAuth2 handling on the router.
- **Cluster:** Hazelcast (`cluster.xml`) listens on all interfaces (ports 5701-5703) with no member authentication or TLS. Any host that can reach those ports can join the cluster, send to the `customer` event bus address directly (bypassing HTTP) or read all event bus traffic. Production would bind the cluster to a private interface or network, and enable Hazelcast security and TLS.

Until then, run the service only on a trusted local or private network and don't expose the HTTP or Hazelcast ports.

### Timeouts don't cancel work, and consume is not idempotent

- The HTTP side gives up after 30 s with a 504, but nothing cancels the request in the customer verticle. If Postgres is slow or the connection pool wait exceeds 30 s, the transaction can still commit after the client has received the 504.
- `POST` consume has no idempotency key. A client that retries after a 504 (or any lost response) can consume the quota twice.

Clients should treat a 504 on consume as "outcome unknown" and check `GET /entitlements` before retrying. A fix would add an idempotency key or request id to consume, stored with the usage change so that replays are no-ops, and propagate cancellation or deadlines to the customer verticle.
