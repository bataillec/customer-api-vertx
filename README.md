# customer-api-vertx

[![CI](https://github.com/bataillec/customer-api-vertx/actions/workflows/ci.yml/badge.svg)](https://github.com/bataillec/customer-api-vertx/actions/workflows/ci.yml)

Reactive Scala 3 / Vert.x microservice that manages customers, their plan subscriptions and the usage quotas (entitlements) that come with them. See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the diagrams of this service, and platform/docs/ARCHITECTURE.md for the wider platform.

- **Customers:** create, read, update (PATCH), soft-delete and list with keyset pagination.
- **Subscriptions and entitlements:** a customer subscribes to a plan, and each plan grants quotas per feature. Consuming a feature is checked and updated atomically in Postgres, and quotas reset per period (except `STORAGE_GB`, which measures what is held).
- **Audit:** every change is written to a Kafka topic as a platform event envelope.
- **Architecture:** an HTTP verticle validates requests against `openapi.yaml` and talks to the customer and audit verticles over the clustered (Hazelcast) event bus. Postgres is the store, with Flyway migrations.

The API contract is `src/main/resources/openapi.yaml`, and a running instance serves it at `GET /openapi.json`.

## Setup

Prerequisites:

- Java 25 and Maven 3.10 (`mise install` sets both up from `mise.toml`)
- A PostgreSQL database. The defaults are host `localhost:5432`, database, user and password all `customers`. Override with `PG_HOST`, `PG_PORT`, `PG_DATABASE`, `PG_USER`, `PG_PASSWORD` and `PG_POOL_SIZE`.
- A Kafka broker for the audit topic, reachable at `KAFKA_BOOTSTRAP_SERVERS` (default `localhost:9092`). To skip the manual setup, run everything with Docker instead (see below).

The schema is created by Flyway when the customer verticle starts, so there is nothing to run by hand.

Build the shaded jar. This runs the unit tests only, which need no database and no container (see [Testing](#testing)):

```
mvn package
```

Then start the three processes described in the next section.

## Running with Docker

`compose.yaml` builds one image and starts the whole platform: Postgres, Kafka (KRaft), one HTTP verticle, two customer verticles and one audit verticle.

```
podman compose up -d --build      # or: docker compose up -d --build
curl localhost:8080/health/ready
```

- Two web UIs come with it:
  - **pgAdmin** at http://localhost:5050 shows the database. The `customers` server is already registered: enter the password `customers` the first time you connect.
  - **Kafka UI** at http://localhost:8090 shows the `customer.events.v1` audit topic and its messages.
- Set `HTTP_PORT`, `PGADMIN_PORT` or `KAFKA_UI_PORT` to publish a service on another host port (`HTTP_PORT=18081 podman compose ...`).
- Postgres 18 stores its data differently, so it uses a new volume, `pgdata18`. The data of the former Postgres 17 `pgdata` volume stays untouched and unused: delete it with `podman volume rm customer-api-vertx_pgdata` once you don't need it.
- The tests are not run during the image build. Run them separately (see [Testing](#testing)).
- Hazelcast (5701) is not published: the verticles find each other by service name on the compose network, using `docker/cluster.xml`.
- `podman compose down -v` stops everything and deletes the data.

## Running without Docker

The service is three verticles that talk over the clustered (Hazelcast) event bus. The shaded jar's entry point only starts `HttpServerVerticle`, so on its own it answers every API call with 503 `NO_HANDLERS`. Start one process per verticle, all with the same cluster options:

```
CP=target/customer-api-vertx-1.0.0-SNAPSHOT.jar
java -jar $CP -c                                               # HttpServerVerticle (HTTP_PORT, default 8080)
java -cp $CP io.vertx.launcher.application.VertxApplication me.bataillec.portfolio.customer.CustomerVerticle -c
java -cp $CP io.vertx.launcher.application.VertxApplication me.bataillec.portfolio.customer.AuditVerticle -c
```

`GET /health/ready` is UP once the three verticles have joined the cluster. `GET /openapi.json` serves the API contract.

## Testing

There are two kinds of tests, and they run at different Maven phases:

| Command | Runs | Needs |
|---|---|---|
| `mvn test` | the unit tests (`src/test/.../unit`), with mocks | nothing but the JDK |
| `mvn verify` | the unit tests, then the integration tests (`src/test/.../integration`), then the 85% coverage gate | a Docker or Podman engine |
| `mvn verify -DskipITs` | the unit tests and the coverage gate, without the integration tests | nothing but the JDK |

`mvn package` runs the unit tests only: the integration tests belong to the later `integration-test` phase. The coverage gate counts the unit tests only.

### Integration tests

They use [Testcontainers](https://testcontainers.com/) to start the real things, with the same images as `compose.yaml` (`postgres:18.7` and `apache/kafka:4.4.0`). The containers start once per JVM and the suites share them, each with a database or topic of its own. `ResilienceIT` starts containers of its own because it pauses them.

| Suite | What it checks |
|---|---|
| `MigrationsIT` | Flyway V1 to V6 on an empty database, the seeded quotas, the email uniqueness, and the data fix of V4 |
| `PGStorageIT` | the real SQL of `PGStorage`: pagination, plan changes, quota reset, and concurrent consumption |
| `CustomerApiIT` | the whole app (HTTP, event bus, Postgres, Kafka) through its public API: lifecycle rules, validation, problem responses, health, `/openapi.json` |
| `AuditKafkaIT` | the audit events on the Kafka topic: envelope, key, content, no event on a failed request, ordering per customer |
| `ResilienceIT` | the app with Postgres or Kafka paused: health reporting, 504 instead of a hang, recovery |
| `ClusterIT` | three Hazelcast nodes with the real `cluster.xml`: load spreading, a quota exact across two writers, failover |

The suites run one after the other, in a single JVM. A full `mvn verify` takes 3 to 4 minutes (a little more the first time, while the images are pulled).

Kafka audit delivery is deliberately fire-and-forget (no outbox), so the audit tests poll the topic with a timeout and never assert guaranteed delivery.

`ClusterIT` joins its nodes through the fixed Hazelcast ports 5701 to 5703 of `cluster.xml`: free them first (the Docker stack does not publish them, but another local Hazelcast node would).

### End-to-end tests

The integration tests run the app inside the test JVM. The end-to-end tests check what they cannot: the **packaged** platform. They build the `Dockerfile`, then start Postgres, Kafka and the app as separate containers (`http`, `audit`, and two `customer` nodes, as in `compose.yaml`), and talk to it through its public HTTP port only. A broken image, entrypoint or `docker/cluster.xml` fails them.

The scenarios are written in Gherkin, in business language, so that people who do not code can read them. They are in `src/test/resources/features/`:

| Feature | What it describes |
|---|---|
| `smoke.feature` | the services start, find each other and answer; the plans and the API description are published |
| `customer-journey.feature` | registering, subscribing, consuming a quota, upgrading, suspending, deleting and reactivating a customer |
| `audit-trail.feature` | the changes of a customer are recorded, in order, on the Kafka topic |
| `cluster-topology.feature` | one customer service lost: still working; all lost: a clear 503; then recovery |
| `resilience.feature` | the database or the audit broker stops responding: readiness says so, then recovers |

The scenarios tagged `@destructive` stop or freeze containers. The hooks put the platform back afterwards, so that the next scenario finds it healthy.

| Command | Runs |
|---|---|
| `mvn verify -Pe2e` | everything `mvn verify` does, then the end-to-end scenarios |
| `mvn -Pe2e test-compile failsafe:integration-test failsafe:verify` | only the end-to-end scenarios |
| `mvn verify -Pe2e -De2e.image=<image>` | the same, with an image you already built instead of building the `Dockerfile` |

They are opt-in because building the image and starting the platform takes a few minutes (about 3 minutes for the 18 scenarios once the Maven cache is warm). They need the same Docker or Podman setup as the integration tests (see below), and do not use the host ports 5701 to 5703 or 8080: the containers have their own network and Testcontainers maps the HTTP port to a random one.

A readable report is written to `target/cucumber-reports/e2e.html` (and `e2e.json`). It lists each scenario with its steps and results, and can be shared as it is.

To add a scenario, write it in a `.feature` file with the existing steps. A new sentence needs a step definition in `src/test/scala/.../e2e/` (`CustomerSteps`, `AuditSteps` or `PlatformSteps`), which is where the HTTP and JSON details belong. Keep the wording of the features free of them.

Keep the scenarios to the journeys and the failure modes of the whole platform. Detailed validation and edge cases are the job of `CustomerApiIT`.

### With Podman

Testcontainers needs to find the engine, and the Ryuk cleanup container does not work with a rootless Podman socket:

```
systemctl --user enable --now podman.socket
export DOCKER_HOST=unix:///run/user/$(id -u)/podman/podman.sock
export TESTCONTAINERS_RYUK_DISABLED=true
mvn verify
```

With Ryuk disabled, Testcontainers removes the containers itself when the JVM exits. After a crashed run, list the leftovers with `podman ps`.

Without these variables the integration suites abort. Check for `Run completed` in the Maven output to be sure the tests actually ran: a compile failure ends the build before them, and it can be a transient one if an IDE compiles the same `target/classes` at the same time (a retry fixes it).

### In CI

The runner needs a Docker engine (a hosted `ubuntu-latest` runner has one). Run `mvn verify`, and cache the two images and the Maven repository between runs to save the pull. Do not set `TESTCONTAINERS_RYUK_DISABLED` there: Ryuk is the safe cleanup when it works. Keep `mvn verify -DskipITs` for a fast first job if the pipeline has several.

The GitHub Actions workflow [.github/workflows/ci.yml](.github/workflows/ci.yml) does this in stages:

| Job | What it runs | Output |
|---|---|---|
| `unit` | `mvn verify -DskipITs` (unit tests and the 85% coverage gate) | test and coverage reports |
| `integration` | `mvn verify` (Postgres, Kafka and Hazelcast cluster suites) | test reports |
| `package` | `mvn package -DskipTests` | fat JAR |
| `docker` | builds the `Dockerfile` | image, passed on to `e2e` |
| `e2e` | the Cucumber scenarios, against the image built by `docker` | Cucumber reports |
| `publish` | on `main` and `v*` tags, once the tests pass: pushes the tested image to GHCR | `ghcr.io/bataillec/customer-api-vertx`; the JAR is attached to the release on tags |

## Audit

Audit events are written to a Kafka topic as platform event envelopes v1 (`event_id`, `event_type`, `occurred_at`, `producer`, `schema_version`, `customer_id` and extras). The record key is the `customer_id`.

| Variable | Default | Meaning |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka brokers |
| `AUDIT_TOPIC` | `customer.events.v1` | Topic receiving the audit events |

With the Docker stack running, read the topic with:

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
