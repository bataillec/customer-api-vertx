# Architecture

How `customer-api-vertx` is built and how a request travels through it. The diagrams
are [Mermaid](https://mermaid.js.org/): GitHub, GitLab and IntelliJ render them in the preview.

Contents:

1. [System context (C4 level 1)](#1-system-context-c4-level-1)
2. [Containers (C4 level 2)](#2-containers-c4-level-2)
3. [Components inside each verticle (C4 level 3)](#3-components-inside-each-verticle-c4-level-3)
4. [Event bus contract](#4-event-bus-contract)
5. [Sequence diagrams](#5-sequence-diagrams)
6. [Database](#6-database)
7. [Error handling](#7-error-handling)
8. [Health checks](#8-health-checks)
9. [Deployment](#9-deployment)
10. [Design decisions and caveats](#10-design-decisions-and-caveats)

## 1. System context (C4 level 1)

```mermaid
C4Context
    title customer-api-vertx: system context
    Person(client, "API client", "A backend service or an operator calling the REST API")
    System(api, "Customer API", "Manages customers, plan subscriptions and usage quotas (entitlements)")
    SystemDb_Ext(pg, "PostgreSQL 18", "Source of truth: customers, subscriptions, entitlements, plan quotas")
    SystemQueue_Ext(kafka, "Apache Kafka", "Topic customer.events.v1: audit events (platform envelope v1)")
    System_Ext(consumers, "Platform consumers", "Other platform services reading the audit topic")
    Rel(client, api, "Calls", "HTTP/JSON, RFC 9457 problems on error")
    Rel(api, pg, "Reads and writes", "Vert.x PG client, Flyway migrations")
    Rel(api, kafka, "Publishes audit events from the outbox", "Kafka producer, acks=all, idempotent")
    Rel(consumers, kafka, "Consume", "")
```

## 2. Containers (C4 level 2)

One Docker image, three kinds of process. Each process runs one verticle, and the verticles only talk to each other
through the **clustered event bus** (Vert.x on Hazelcast). `customer` is horizontally scalable: the event bus
round-robins requests across its instances.

```mermaid
C4Container
    title customer-api-vertx: containers
    Person(client, "API client")

    System_Boundary(platform, "Customer API (one image, one process per verticle)") {
        Container(http, "HttpServerVerticle", "Scala 3, Vert.x Web + OpenAPI", "Validates requests against openapi.yaml, forwards them over the event bus, maps replies and failures to HTTP. Exposes /health and /openapi.json")
        Container(customer, "CustomerVerticle x N", "Scala 3, Vert.x PG client, Flyway", "Business rules and SQL. Runs the migrations at start. Writes each audit event to the outbox table, in the transaction of the change")
        Container(audit, "AuditVerticle", "Scala 3, Vert.x PG and Kafka clients", "Outbox relay: reads the unsent audit events from Postgres, writes them to Kafka, marks them sent")
        ContainerDb(hz, "Hazelcast cluster", "TCP/IP join, port 5701", "Event bus membership and subscriptions (not published outside the network)")
    }

    ContainerDb(pg, "PostgreSQL", "postgres:18.7")
    ContainerQueue(kafka, "Kafka", "apache/kafka:4.4.0, KRaft")
    Rel(client, http, "REST", "HTTP :8080")
    Rel(http, customer, "request/reply", "event bus address 'customer'")
    Rel(http, customer, "ping", "address 'customer.health'")
    Rel(http, audit, "ping", "address 'audit.health'")
    Rel(customer, pg, "SQL", "pool of 16, transactions")
    Rel(audit, pg, "outbox", "reads unsent rows, marks them sent; advisory lock = leader")
    Rel(audit, kafka, "produce", "key = customer_id")
    Rel_D(http, hz, "joins")
    Rel_D(customer, hz, "joins")
    Rel_D(audit, hz, "joins")
```

The same view as a plain flowchart, with the compose services as they run (`compose.yaml`):

```mermaid
flowchart LR
    client([API client]) -->|HTTP :8080| http

    subgraph cluster["Vert.x cluster (Hazelcast, 5701)"]
        http["http<br/>HttpServerVerticle"]
        c1["customer-1<br/>CustomerVerticle"]
        c2["customer-2<br/>CustomerVerticle"]
        aud["audit<br/>AuditVerticle"]
    end

    http -- " customer " --> c1
    http -- " customer " --> c2
    http -. " customer.health / audit.health " .-> c1
    http -. " ping " .-> aud
    c1 -->|SQL| pg[(PostgreSQL)]
    c2 -->|SQL, incl. outbox rows| pg
    aud -->|relay the outbox| pg
    aud -->|produce| kafka[[Kafka<br/>customer.events.v1]]
    pgadmin[pgAdmin :5050] -.-> pg
    kui[Kafka UI :8090] -.-> kafka
```

| Process    | Entry class          | Scales          | Depends on          | Config (env)                                                                  |
|------------|----------------------|-----------------|---------------------|-------------------------------------------------------------------------------|
| `http`     | `HttpServerVerticle` | yes (stateless) | the event bus       | `HTTP_PORT` (8080)                                                            |
| `customer` | `CustomerVerticle`   | yes, any number | Postgres, event bus | `PG_HOST`, `PG_PORT`, `PG_DATABASE`, `PG_USER`, `PG_PASSWORD`, `PG_POOL_SIZE` |
| `audit`    | `AuditVerticle`      | one relays, the others stand by | Kafka, Postgres, event bus (health only) | `KAFKA_BOOTSTRAP_SERVERS`, `AUDIT_TOPIC`, the `PG_*` of `customer`, `OUTBOX_POLL_INTERVAL_MS` (200), `OUTBOX_BATCH_SIZE` (100), `OUTBOX_RETENTION_DAYS` (7) |

## 3. Components inside each verticle (C4 level 3)

```mermaid
flowchart TB
    subgraph HttpServerVerticle
        direction TB
        ar["ApiRoutes<br/>OpenAPI router, health checks, /openapi.json"]
        cs["CustomerSenderImpl<br/>builds DeliveryOptions (headers) and sends the request"]
        pr["Problem<br/>failure handler: exception to application/problem+json"]
        hr["HttpResponse<br/>reply envelope"]
        ar --> cs
        ar --> pr
        ar --> hr
    end

    subgraph CustomerVerticle
        direction TB
        ch["CustomerHandlerImpl<br/>consumer on 'customer': dispatch on operationId header"]
        co["CustomerOperations<br/>business rules, transactions, outbox writes"]
        st["Storage / PGStorage<br/>SQL"]
        as["OutboxRecorder<br/>builds the envelope, INSERT INTO outbox"]
        ch --> co
        co --> st
        co --> as
    end

    subgraph AuditVerticle
        direction TB
        ah["health consumer on 'audit.health'"]
        ao["AuditOperation"]
        rl["OutboxRelay<br/>leader loop"]
        os["PGOutboxStore<br/>advisory lock, fetch, mark sent, prune"]
        kw["KafkaAuditWriter<br/>producer + admin client"]
        ah --> ao --> kw
        ao --> rl
        rl --> os
        rl --> kw
    end

    cs ==>|" event bus 'customer' "| ch
    st --> pg[(PostgreSQL)]
    os --> pg
    kw --> kafka[[Kafka]]
```

| Package    | Role                                                                                                                                                                                                   |
|------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `http`     | `ApiRoutes` (router built from the OpenAPI contract), `Problem` (RFC 9457 bodies), `HttpResponse` (the reply envelope that crosses the event bus), `WithHttpResponse` (turns a domain object into one) |
| `eventbus` | `*Sender` (caller side) and `*Handler` (consumer side) for `customer`, plus `SenderUtils` (health ping/pong) and `EventbusUtils` (30 s timeout, header helpers)                            |
| `customer` | `CustomerOperations` (rules), `Storage`/`PGStorage` (SQL), `CustomerCursor` (keyset cursor codec)                                                                                                      |
| `audit`    | `Outbox`/`OutboxRecorder` (write side), `OutboxRelay`, `OutboxStore`/`PGOutboxStore` (read side), `AuditOperation`, `AuditLogger`, `KafkaAuditWriter`                                                                                                                                                  |
| `domain`   | Case classes, enums (`CustomerStatus`, `PlanType`, `SubscriptionStatus`) and the sealed `DomainError` family                                                                                           |
| `config`   | `*Config.fromEnv()`: one config class per verticle                                                                                                                                                     |
| `json`     | `JsonSupport`: Jackson codec for the domain objects                                                                                                                                                    |

The seams (`CustomerOps`, `Storage`, `Outbox`, `OutboxStore`, `AuditOps`, `AuditLogger`) are traits, so every layer is unit-tested
with mocks.

## 4. Event bus contract

| Address           | Pattern                     | Sender               | Consumer              | Body                                | Headers                                                         |
|-------------------|-----------------------------|----------------------|-----------------------|-------------------------------------|-----------------------------------------------------------------|
| `customer`        | request/reply, 30 s timeout | `CustomerSenderImpl` | `CustomerHandlerImpl` | request JSON as a String (or none)  | `operationId`, `id` (path), `limit`, `cursor`, `status` (query) |
| `customer.health` | request/reply, 4 s timeout  | `/health/ready`      | `CustomerHandlerImpl` | `"ping"`                            | none                                                            |
| `audit.health`    | request/reply, 4 s timeout  | `/health/ready`      | `AuditVerticle`       | `"ping"`, replies with the outbox backlog as JSON | none                                                            |

**Reply of `customer`**: a JSON String of an `HttpResponse`: `{ "statusCode", "contentType", "body", "headers" }`. The
HTTP verticle does not interpret business results: it copies the status, headers and body (adding `trace_id` to problem
bodies). A business failure (404, 409, 429...) is therefore a *successful* event bus reply carrying a problem. Only
infrastructure failures are `ReplyException`s (see [Error handling](#7-error-handling)).

**Audit envelope v1** (record key = `customer_id`):

```json
{
  "event_id": "uuid",
  "event_type": "CUSTOMER_CREATED | CUSTOMER_UPDATED | SUBSCRIPTION_CHANGED | QUOTA_EXCEEDED | QUOTA_CONSUMED",
  "occurred_at": "2026-11-20T10:15:30Z",
  "producer": "customer-api",
  "schema_version": 1,
  "customer_id": "uuid",
  "...extras": "plan_type, previous_plan_type, company_name, old_status, new_status, feature_key, amount, consumed_quota, allocated_quota"
}
```

## 5. Sequence diagrams

### 5.1 An HTTP request, in general

Every API call follows this path. `CustomerSenderImpl` turns the HTTP request into headers plus a JSON body,
`CustomerHandlerImpl` turns it back into a typed call.

```mermaid
sequenceDiagram
    autonumber
    actor C as Client
    participant R as Router (ApiRoutes)<br/>HttpServerVerticle
    participant S as CustomerSenderImpl
    participant EB as Event bus (Hazelcast)
    participant H as CustomerHandlerImpl<br/>CustomerVerticle
    participant O as CustomerOperations
    participant P as PGStorage
    participant DB as PostgreSQL
    C ->> R: HTTP request
    R ->> R: BodyHandler (64 KiB limit)<br/>validate path, query, body against openapi.yaml
    alt invalid request
        R -->> C: 400 problem VALIDATION_FAILED
    else valid
        R ->> S: operation handler (by operationId)
        S ->> EB: request("customer", body, headers{operationId, id, limit...}, timeout 30 s)
        EB ->> H: one customer instance (round-robin)
        H ->> H: decode headers and body<br/>(bad UUID or number: 400 problem)
        H ->> O: typed call
        O ->> P: SQL
        P ->> DB: query
        DB -->> P: rows
        P -->> O: domain objects
        O -->> H: Future[domain object] or DomainError
        H -->> EB: reply(HttpResponse JSON)
        EB -->> S: Message[String]
        S -->> R: String
        R ->> R: HttpResponse.fromJsonString, add trace_id to problems
        R -->> C: status, headers (Location, Retry-After), body
    end
    note over R, EB: No handler: 503 NO_HANDLERS. No reply in 30 s: 504 TIMEOUT_COMMUNICATION
```

### 5.2 Create a customer (`POST /customers`)

One transaction: the customer and its audit event commit together.

```mermaid
sequenceDiagram
    autonumber
    actor C as Client
    participant H as HttpServerVerticle
    participant CU as CustomerVerticle
    participant DB as PostgreSQL
    C ->> H: POST /customers {email, companyName}
    H ->> CU: request("customer", operationId=createCustomer)
    CU ->> DB: BEGIN
    CU ->> DB: INSERT INTO customers ... RETURNING *
    alt email already used (23505 on lower(email))
        DB -->> CU: unique violation
        CU ->> DB: ROLLBACK
        CU -->> H: reply 409 EMAIL_ALREADY_USED
        H -->> C: 409 application/problem+json
    else inserted
        DB -->> CU: the row
        CU ->> DB: INSERT INTO outbox (CUSTOMER_CREATED)
        CU ->> DB: COMMIT
        CU -->> H: reply 201 + Location
        H -->> C: 201 Created, Location: /customers/{id}
    end
    note over CU, DB: The audit verticle publishes the event to Kafka afterwards (5.7)
```

### 5.3 Subscribe to a plan (`POST /customers/{id}/subscriptions`)

One transaction. The `SELECT ... FOR UPDATE` on the customer row serializes every operation on a customer, whichever
node runs it.

```mermaid
sequenceDiagram
    autonumber
    actor C as Client
    participant H as HttpServerVerticle
    participant CU as CustomerVerticle
    participant DB as PostgreSQL
    C ->> H: POST /customers/{id}/subscriptions {planType}
    H ->> CU: request("customer", operationId=subscribe, id)
    CU ->> DB: BEGIN
    CU ->> DB: SELECT status FROM customers WHERE id=$1 FOR UPDATE
    alt no row
        CU -->> H: 404 CUSTOMER_NOT_FOUND (rollback)
    else SUSPENDED or DELETED
        CU -->> H: 409 CUSTOMER_NOT_ACTIVE (rollback)
    else ACTIVE
        CU ->> DB: SELECT plan_type FROM subscriptions WHERE status='ACTIVE'
        alt same plan
            CU -->> H: 409 PLAN_ALREADY_ACTIVE (rollback)
        else new or different plan
            CU ->> DB: UPDATE subscriptions SET status='CANCELED', end_date=now()
            CU ->> DB: INSERT INTO subscriptions (customer_id, plan_type) RETURNING *
            CU ->> DB: SELECT quotas FROM plan_quotas WHERE plan_type=$1
            CU ->> DB: DELETE FROM entitlements WHERE feature_key not in the plan
            loop each feature of the plan (sorted)
                CU ->> DB: INSERT INTO entitlements ... ON CONFLICT (customer_id, feature_key)<br/>DO UPDATE allocated_quota (consumed_quota is kept)
            end
            CU ->> DB: INSERT INTO outbox (SUBSCRIPTION_CHANGED, plan_type, previous_plan_type)
            CU ->> DB: COMMIT
            CU -->> H: 201 subscription + entitlements, Location
        end
    end
    H -->> C: the status of the reply
```

### 5.4 Consume a quota (`POST /customers/{id}/entitlements/consume`)

The hot path is one guarded `UPDATE`. The follow-up queries only run on a miss, to explain it.

```mermaid
sequenceDiagram
    autonumber
    actor C as Client
    participant H as HttpServerVerticle
    participant CU as CustomerVerticle
    participant DB as PostgreSQL
    C ->> H: POST .../entitlements/consume {featureKey, amount}
    H ->> CU: request("customer", operationId=consumeEntitlement)
    CU ->> DB: BEGIN
    CU ->> DB: SELECT status FROM customers WHERE id=$1 FOR UPDATE
    alt customer not ACTIVE
        CU -->> H: 404 or 409 CUSTOMER_NOT_ACTIVE
    else ACTIVE
        CU ->> DB: UPDATE entitlements SET consumed = (reset_date passed ? 0 : consumed) + amount,<br/>reset_date = (passed ? next month : reset_date)<br/>WHERE amount <= allocated - current usage<br/>AND an ACTIVE subscription EXISTS RETURNING ...
        alt one row updated
            DB -->> CU: allocated, consumed
            CU ->> DB: INSERT INTO outbox (QUOTA_CONSUMED, feature_key, amount, consumed_quota, allocated_quota)
            CU ->> DB: COMMIT (the event commits with the consumption)
            CU -->> H: 200 {allocated, consumed, remaining}
        else no row
            CU ->> DB: SELECT entitlement (feature_key)
            alt none
                CU -->> H: 404 ENTITLEMENT_NOT_FOUND
            else exists
                CU ->> DB: SELECT active plan
                alt no active subscription
                    CU -->> H: 409 NO_ACTIVE_SUBSCRIPTION
                else quota exhausted
                    CU ->> DB: INSERT INTO outbox (QUOTA_EXCEEDED, feature_key)
                    CU ->> DB: COMMIT (the refusal is a value, so the event is kept)
                    CU -->> H: 429 QUOTA_EXCEEDED + Retry-After (seconds to reset_date, absent if amount > allocation or no reset)
                end
            end
        end
    end
    H -->> C: the status of the reply
```

### 5.5 Update or delete a customer (`PATCH /customers/{id}`)

Deleting is a `PATCH` with `status = DELETED`: a soft delete that also cancels the active subscription. The entitlements
stay, so deleting then reactivating cannot be used to reset a quota.

```mermaid
sequenceDiagram
    autonumber
    actor C as Client
    participant H as HttpServerVerticle
    participant CU as CustomerVerticle
    participant DB as PostgreSQL
    C ->> H: PATCH /customers/{id} {companyName?, status?}
    H ->> CU: request("customer", operationId=updateCustomer)
    CU ->> DB: BEGIN
    CU ->> DB: SELECT status FROM customers WHERE id=$1 FOR UPDATE
    alt no row
        CU -->> H: 404 CUSTOMER_NOT_FOUND
    else already DELETED, stays DELETED, and companyName given
        CU -->> H: 409 CUSTOMER_NOT_ACTIVE
    else
        opt new status is DELETED
            CU ->> DB: UPDATE subscriptions SET status='CANCELED', end_date=now() WHERE ACTIVE
        end
        CU ->> DB: UPDATE customers SET company_name=COALESCE(..), status=COALESCE(..), updated_at=now() RETURNING *
        CU ->> DB: INSERT INTO outbox (CUSTOMER_UPDATED, old_status, new_status, stored company_name)
        CU ->> DB: COMMIT
        CU -->> H: 200 customer
    end
    H -->> C: the status of the reply
```

### 5.6 List customers (`GET /customers`)

Keyset pagination, newest first. The cursor is opaque: `base64url("<created_at>|<id>")` of the last item of the previous
page.

```mermaid
sequenceDiagram
    autonumber
    actor C as Client
    participant H as HttpServerVerticle
    participant CU as CustomerVerticle
    participant DB as PostgreSQL
    C ->> H: GET /customers?limit=50&cursor=...&status=ACTIVE
    H ->> CU: request (headers limit, cursor, status #59; limit defaults to 50)
    CU ->> CU: decode the cursor (invalid: 400 INVALID_CURSOR)
    CU ->> DB: SELECT * FROM customers WHERE status filter<br/>AND (created_at, id) < (cursor) ORDER BY created_at DESC, id DESC LIMIT limit+1
    DB -->> CU: up to limit+1 rows
    CU ->> CU: keep limit rows #59; the extra row means a next page exists: nextCursor = cursor of the last kept row
    CU -->> H: 200 {items, nextCursor}
    H -->> C: 200
```

Without a `status` filter the query excludes `DELETED` customers.

### 5.7 Audit delivery (transactional outbox)

An event is a row of the `outbox` table, written in the transaction of the change it describes: it commits or rolls back
with it. The audit verticle then relays the rows to Kafka.

```mermaid
sequenceDiagram
    autonumber
    participant CU as CustomerOperations
    participant OR as OutboxRecorder
    participant DB as PostgreSQL
    participant RL as OutboxRelay (audit verticle)
    participant KW as KafkaAuditWriter
    participant K as Kafka
    CU ->> OR: record(tx, eventType, customerId, extras)
    OR ->> OR: build the envelope (event_id, event_type, occurred_at, producer, schema_version, customer_id)
    OR ->> DB: INSERT INTO outbox (customer_id, payload) on the transaction of the change
    note over CU, DB: COMMIT, or ROLLBACK: the row follows the change
    loop every OUTBOX_POLL_INTERVAL_MS, at once while batches are full
        RL ->> DB: pg_try_advisory_lock (once: the leader keeps its connection)
        RL ->> DB: SELECT unsent rows ORDER BY id LIMIT OUTBOX_BATCH_SIZE
        loop each customer in parallel, its rows one after the other
            RL ->> KW: audit(payload)
            KW ->> K: producer.send(topic, key=customer_id, value=payload) acks=all, idempotent
            alt failure
                K -->> KW: error
                RL ->> RL: stop this customer here: its next rows wait for the next round
            end
        end
        RL ->> DB: UPDATE outbox SET sent_at = now() WHERE id = ANY(the rows acknowledged)
    end
    RL ->> DB: once a minute: DELETE rows sent more than OUTBOX_RETENTION_DAYS ago
```

- **Order.** `outbox.id` is assigned under the customer row lock, so it grows with the commit order of one customer.
  The relay reads by `id`, and sends a customer's rows one after the other, each acknowledged before the next. Records
  of one customer share a key, hence a partition.
- **One relay at a time.** Several `audit` processes may run: the one that holds the Postgres advisory lock (on a
  connection of its own) relays, the others retry every 5 s. If the leader dies, its connection closes, the lock is
  released and another one takes over. `FOR UPDATE SKIP LOCKED` was not used: with several relays it would let a later
  row of a customer overtake an earlier one that another relay holds.
- **At-least-once.** A crash between the Kafka acknowledgement and the `sent_at` update publishes the row again, with
  the same `event_id`. Consumers deduplicate on `event_id`.
- **Kafka down.** Nothing is lost and the API keeps answering: the rows wait in the table, and the relay retries
  (2 s backoff). `/health/ready` reports the backlog.

### 5.8 Readiness (`GET /health/ready`)

```mermaid
sequenceDiagram
    autonumber
    actor P as Probe
    participant H as HttpServerVerticle
    participant CH as CustomerHandlerImpl
    participant AH as AuditVerticle
    participant DB as PostgreSQL
    participant K as Kafka
    P ->> H: GET /health/ready
    par customer-verticle
        H ->> CH: request("customer.health", "ping", 4 s)
        CH ->> DB: SELECT 1
        DB -->> CH: ok
        CH -->> H: "pong" (or fail 503)
    and audit-verticle
        H ->> AH: request("audit.health", "ping", 4 s)
        AH ->> K: admin.describeCluster() (3 s)
        K -->> AH: ok
        AH ->> DB: outbox backlog (count, age of the oldest), 1 s, best effort
        AH -->> H: the backlog as JSON, reported as the check's details (or fail 503 when Kafka is down)
    end
    H -->> P: 200 UP, or 503 DOWN with the failing check
```

`GET /health/live` only checks that the HTTP event loop answers.

## 6. Database

PostgreSQL 18, schema managed by Flyway (`src/main/resources/db/migration`). The `customer` verticle runs the migrations
when it starts, before it registers on the event bus, so a node serves requests only once the schema is current.

```mermaid
erDiagram
    customers ||--o{ subscriptions: "has (ON DELETE CASCADE)"
    customers ||--o{ entitlements: "has (ON DELETE CASCADE)"
    plan_quotas ||..o{ subscriptions: "defines quotas of plan_type"
    plan_quotas ||..o{ entitlements: "template for feature_key"

    customers {
        uuid id PK "gen_random_uuid()"
        varchar_255 email "unique on lower(email)"
        varchar_255 company_name "nullable"
        customer_status status "ACTIVE, SUSPENDED, DELETED"
        timestamptz created_at
        timestamptz updated_at
    }

    subscriptions {
        uuid id PK
        uuid customer_id FK
        plan_type plan_type "FREE_TIER, PRO, ENTERPRISE"
        subscription_status status "ACTIVE, CANCELED, EXPIRED"
        timestamptz start_date
        timestamptz end_date "nullable"
    }

    entitlements {
        uuid id PK
        uuid customer_id FK
        varchar_100 feature_key "unique with customer_id"
        bigint allocated_quota "CHECK >= 0"
        bigint consumed_quota "CHECK >= 0, default 0"
        timestamptz reset_date "NULL: usage never resets"
    }

    plan_quotas {
        plan_type plan_type PK
        varchar_100 feature_key PK
        bigint quota "CHECK >= 0"
        boolean resets_monthly "false for STORAGE_GB"
        timestamptz updated_at
    }
```

`plan_quotas` is related to the other tables by value (`plan_type`, `feature_key`), not by foreign key: it is the
catalogue the entitlements are copied from when a customer subscribes (dotted lines).

### Constraints and indexes

| Object                        | Definition                                      | Why                                                                                           |
|-------------------------------|-------------------------------------------------|-----------------------------------------------------------------------------------------------|
| `customers_email_lower_key`   | `UNIQUE (lower(email))`                         | an email is unique whatever its case, `DELETED` customers included since they can return (V5) |
| `unique_active_subscription`  | `UNIQUE (customer_id) WHERE status = 'ACTIVE'`  | one active subscription per customer; canceled and expired rows are unlimited (V1)            |
| `idx_subscriptions_customer`  | `(customer_id, start_date DESC)`                | subscription history of a customer                                                            |
| `unique_customer_feature`     | `UNIQUE (customer_id, feature_key)`             | one entitlement per feature; the `ON CONFLICT` target of the upsert, and the lookup index     |
| `customers_created_at_id_idx` | `(created_at DESC, id DESC)`                    | keyset pagination of `GET /customers` (V6)                                                    |
| `CHECK (... >= 0)`            | on `allocated_quota`, `consumed_quota`, `quota` | no negative usage                                                                             |
| FKs `ON DELETE CASCADE`       | `subscriptions`, `entitlements` to `customers`  | the API soft-deletes, the cascade is a safety net                                             |

### Seeded plans (`plan_quotas`)

| Plan         | `API_CALLS_MONTHLY` | `STORAGE_GB` |
|--------------|---------------------|--------------|
| `FREE_TIER`  | 1 000               | 1            |
| `PRO`        | 100 000             | 50           |
| `ENTERPRISE` | 10 000 000          | 1 000        |

`STORAGE_GB` has `resets_monthly = false`: it measures what is held, so its `reset_date` is `NULL` and its usage never
resets. `API_CALLS_MONTHLY` resets on the first day of the next month (UTC).

### Migrations

| Version | Change                                                                |
|---------|-----------------------------------------------------------------------|
| V1      | enums, `customers`, `subscriptions`, `entitlements`                   |
| V2      | `plan_quotas` and its seed                                            |
| V3      | `resets_monthly`, nullable `entitlements.reset_date`                  |
| V4      | data fix: cancel the active subscription of customers already deleted |
| V5      | case-insensitive unique email                                         |
| V6      | pagination index                                                      |

### State machines

```mermaid
stateDiagram-v2
    direction LR
    state "Customer" as CS {
[*] --> ACTIVE: POST /customers
ACTIVE --> SUSPENDED: PATCH status
SUSPENDED --> ACTIVE: PATCH status
ACTIVE --> DELETED: PATCH status (cancels the subscription)
SUSPENDED --> DELETED: PATCH status (cancels the subscription)
DELETED --> ACTIVE: PATCH status (email kept, entitlements kept)
}
```

```mermaid
stateDiagram-v2
    direction LR
    [*] --> ACTIVE: subscribe
    ACTIVE --> CANCELED: another plan chosen, or customer deleted
    CANCELED --> [*]
    note right of ACTIVE
        At most one per customer.
        Subscribing to the same plan: 409.
        EXPIRED exists in the enum, nothing sets it yet.
    end note
```

### Quota lifecycle

- **Subscribe:** the entitlements are aligned with the plan. Missing features are inserted, existing ones get the new
  `allocated_quota` and keep their `consumed_quota`, the ones outside the plan are deleted. Changing plan never frees
  quota.
- **Consume:** one conditional `UPDATE`. When `reset_date` has passed, the usage restarts from 0 and `reset_date` moves
  to the first day of the next month, in the same statement.
- **Read:** `GET /entitlements` returns the *effective* view (`Entitlement.effective(now)`): a period that already ended
  shows 0 consumed even before the next consume rewrites the row.

## 7. Error handling

Errors come from four places and all end as `application/problem+json` (`title`, `status`, `code`, `detail`, and
`trace_id` when the request carried a W3C `traceparent` header).

```mermaid
flowchart TD
    req[Request] --> val{OpenAPI validation}
    val -- invalid --> e400["400 VALIDATION_FAILED<br/>(Problem.failureHandler)"]
    val -- no route --> e404["404 NOT_FOUND / 405 with Allow header"]
    val -- ok --> eb[event bus request]
    eb -- no consumer --> e503["503 NO_HANDLERS"]
    eb -- 30 s without reply --> e504["504 TIMEOUT_COMMUNICATION"]
    eb -- msg . fail --> erec["RECIPIENT_FAILURE with the failure code"]
    eb -- reply --> dec{reply is}
    dec -- decode error in handler --> e400b["400 VALIDATION_FAILED<br/>(bad UUID, number, enum)"]
    dec -- DomainError --> edom["DomainError status and code<br/>404, 409, 429, 400, 500"]
    dec -- success --> ok["2xx"]
    eb -- anything else --> e500["500 INTERNAL_ERROR (logged)"]
```

| `DomainError`          | Status                                    | Code                     |
|------------------------|-------------------------------------------|--------------------------|
| `InvalidCursor`        | 400                                       | `INVALID_CURSOR`         |
| `CustomerNotFound`     | 404                                       | `CUSTOMER_NOT_FOUND`     |
| `EntitlementNotFound`  | 404                                       | `ENTITLEMENT_NOT_FOUND`  |
| `EmailAlreadyUsed`     | 409                                       | `EMAIL_ALREADY_USED`     |
| `CustomerNotActive`    | 409                                       | `CUSTOMER_NOT_ACTIVE`    |
| `PlanAlreadyActive`    | 409                                       | `PLAN_ALREADY_ACTIVE`    |
| `NoActiveSubscription` | 409                                       | `NO_ACTIVE_SUBSCRIPTION` |
| `QuotaExceeded`        | 429 (`Retry-After` when waiting can help) | `QUOTA_EXCEEDED`         |
| `PlanNotConfigured`    | 500                                       | `PLAN_NOT_CONFIGURED`    |

## 8. Health checks

| Endpoint            | Checks                                                                                               | Status           |
|---------------------|------------------------------------------------------------------------------------------------------|------------------|
| `GET /health/live`  | the HTTP verticle answers                                                                            | 200              |
| `GET /health/ready` | `customer-verticle` (a customer node runs `SELECT 1`) and `audit-verticle` (Kafka `describeCluster`; the outbox backlog is reported, not checked) | 200 UP, 503 DOWN |

Timeouts are nested so that each layer fails by itself before the one above gives up: Kafka admin 3 s, event bus ping 4
s, health check 5 s.

## 9. Deployment

```mermaid
flowchart LR
    subgraph net["compose network"]
        http["http<br/>:8080 published"]
        c1[customer-1]
        c2[customer-2]
        aud[audit]
        pg[(postgres:18.7<br/>volume pgdata18)]
        kf[[kafka 4.4.0 KRaft<br/>volume kafkadata]]
        pga[pgAdmin :5050]
        kui[Kafka UI :8090]
    end
    http <-->|" Hazelcast 5701<br/>(not published) "| c1
    http <--> c2
    http <--> aud
    c1 --> pg
    c2 --> pg
    aud --> kf
    pga -.-> pg
    kui -.-> kf
```

- One image (`Dockerfile`, multi-stage Maven build, JRE 25) for the three verticles. The container command picks the
  verticle.
- `docker/entrypoint.sh` starts the Vert.x application with `-c --cluster-host <container IP>` and `docker/cluster.xml`:
  Hazelcast joins by TCP/IP on the compose service names.
- `restart: unless-stopped`; Postgres and Kafka have health checks and the verticles wait for them.
- Locally, the same three processes can be started by hand (see the [README](../README.md#running-without-docker)).

## 10. Design decisions and caveats

- **Verticles as services over the event bus.** The HTTP layer has no business logic and the business layer has no HTTP.
  A customer node can be added or lost at runtime (`ClusterIT`, `cluster-topology.feature`).
- **Business results travel as data.** The customer verticle replies with a ready `HttpResponse`, so the HTTP verticle
  stays a thin translator.
- **Correctness is in Postgres, not in memory.** The customer row lock (`FOR UPDATE`) serializes the operations of one
  customer across nodes, and the consume `UPDATE` is atomic and guarded, so a quota is exact with several writers.
- **Audit is a transactional outbox.** Every event is written to the `outbox` table in the transaction of its change, so
  a rolled back change leaves no event and a committed one cannot lose its event, even if the process dies right after
  the commit. The audit verticle relays the rows to Kafka (5.7). Consequently:
    - delivery is at-least-once: consumers deduplicate on `event_id`;
    - a refused consume (429) still keeps its `QUOTA_EXCEEDED` event: the refusal leaves the transaction as a value and
      only becomes a failure after the commit;
    - Kafka being down does not affect the API, only the delay of the events. A long outage grows the table, which
      `/health/ready` reports (`outbox_backlog`, `outbox_oldest_age_seconds`);
    - a successful consume emits `QUOTA_CONSUMED` in the same transaction, so it costs one more `INSERT` on the hot path,
      and fails the consume if the insert fails. It is an audit event: high-volume metering stays a separate topic.
- **Timeouts do not cancel work.** After the 30 s the client gets a 504 but the transaction can still commit. Consume
  has no idempotency key, so a retry after a 504 can consume twice. Check `GET /entitlements` first.
- **No authentication** on the HTTP API or on the Hazelcast cluster. Run it on a trusted network only.

See the [README](../README.md) for setup, tests and the full list of known limitations.
