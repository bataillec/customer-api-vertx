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
    Rel(api, kafka, "Publishes audit events", "Kafka producer, acks=all, idempotent")
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
        Container(customer, "CustomerVerticle x N", "Scala 3, Vert.x PG client, Flyway", "Business rules and SQL. Runs the migrations at start. Sends audit events")
        Container(audit, "AuditVerticle", "Scala 3, Vert.x Kafka client", "Writes each audit event to Kafka")
        ContainerDb(hz, "Hazelcast cluster", "TCP/IP join, port 5701", "Event bus membership and subscriptions (not published outside the network)")
    }

    ContainerDb(pg, "PostgreSQL", "postgres:18.7")
    ContainerQueue(kafka, "Kafka", "apache/kafka:4.4.0, KRaft")
    Rel(client, http, "REST", "HTTP :8080")
    Rel(http, customer, "request/reply", "event bus address 'customer'")
    Rel(http, customer, "ping", "address 'customer.health'")
    Rel(http, audit, "ping", "address 'audit.health'")
    Rel(customer, audit, "send (fire and forget)", "address 'audit'")
    Rel(customer, pg, "SQL", "pool of 16, transactions")
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
    c1 -- " audit " --> aud
    c2 -- " audit " --> aud
    http -. " customer.health / audit.health " .-> c1
    http -. " ping " .-> aud
    c1 -->|SQL| pg[(PostgreSQL)]
    c2 -->|SQL| pg
    aud -->|produce| kafka[[Kafka<br/>customer.events.v1]]
    pgadmin[pgAdmin :5050] -.-> pg
    kui[Kafka UI :8090] -.-> kafka
```

| Process    | Entry class          | Scales          | Depends on          | Config (env)                                                                  |
|------------|----------------------|-----------------|---------------------|-------------------------------------------------------------------------------|
| `http`     | `HttpServerVerticle` | yes (stateless) | the event bus       | `HTTP_PORT` (8080)                                                            |
| `customer` | `CustomerVerticle`   | yes, any number | Postgres, event bus | `PG_HOST`, `PG_PORT`, `PG_DATABASE`, `PG_USER`, `PG_PASSWORD`, `PG_POOL_SIZE` |
| `audit`    | `AuditVerticle`      | one is enough   | Kafka, event bus    | `KAFKA_BOOTSTRAP_SERVERS`, `AUDIT_TOPIC`                                      |

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
        co["CustomerOperations<br/>business rules, transactions, audit calls"]
        st["Storage / PGStorage<br/>SQL"]
        as["AuditSenderImpl<br/>builds the envelope, eventBus.send"]
        ch --> co
        co --> st
        co --> as
    end

    subgraph AuditVerticle
        direction TB
        ah["AuditHandlerImpl<br/>consumer on 'audit'"]
        ao["AuditOperation"]
        kw["KafkaAuditWriter<br/>producer + admin client"]
        ah --> ao --> kw
    end

    cs ==>|" event bus 'customer' "| ch
    as ==>|" event bus 'audit' "| ah
    st --> pg[(PostgreSQL)]
    kw --> kafka[[Kafka]]
```

| Package    | Role                                                                                                                                                                                                   |
|------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `http`     | `ApiRoutes` (router built from the OpenAPI contract), `Problem` (RFC 9457 bodies), `HttpResponse` (the reply envelope that crosses the event bus), `WithHttpResponse` (turns a domain object into one) |
| `eventbus` | `*Sender` (caller side) and `*Handler` (consumer side) for `customer` and `audit`, plus `SenderUtils` (health ping/pong) and `EventbusUtils` (30 s timeout, header helpers)                            |
| `customer` | `CustomerOperations` (rules), `Storage`/`PGStorage` (SQL), `CustomerCursor` (keyset cursor codec)                                                                                                      |
| `audit`    | `AuditOperation`, `AuditLogger`, `KafkaAuditWriter`                                                                                                                                                    |
| `domain`   | Case classes, enums (`CustomerStatus`, `PlanType`, `SubscriptionStatus`) and the sealed `DomainError` family                                                                                           |
| `config`   | `*Config.fromEnv()`: one config class per verticle                                                                                                                                                     |
| `json`     | `JsonSupport`: Jackson codec for the domain objects                                                                                                                                                    |

The seams (`CustomerOps`, `Storage`, `AuditSender`, `AuditOps`, `AuditLogger`) are traits, so every layer is unit-tested
with mocks.

## 4. Event bus contract

| Address           | Pattern                     | Sender               | Consumer              | Body                                | Headers                                                         |
|-------------------|-----------------------------|----------------------|-----------------------|-------------------------------------|-----------------------------------------------------------------|
| `customer`        | request/reply, 30 s timeout | `CustomerSenderImpl` | `CustomerHandlerImpl` | request JSON as a String (or none)  | `operationId`, `id` (path), `limit`, `cursor`, `status` (query) |
| `audit`           | `send`, no reply            | `AuditSenderImpl`    | `AuditHandlerImpl`    | the event envelope as a JSON String | none                                                            |
| `customer.health` | request/reply, 4 s timeout  | `/health/ready`      | `CustomerHandlerImpl` | `"ping"`                            | none                                                            |
| `audit.health`    | request/reply, 4 s timeout  | `/health/ready`      | `AuditHandlerImpl`    | `"ping"`                            | none                                                            |

**Reply of `customer`**: a JSON String of an `HttpResponse`: `{ "statusCode", "contentType", "body", "headers" }`. The
HTTP verticle does not interpret business results: it copies the status, headers and body (adding `trace_id` to problem
bodies). A business failure (404, 409, 429...) is therefore a *successful* event bus reply carrying a problem. Only
infrastructure failures are `ReplyException`s (see [Error handling](#7-error-handling)).

**Audit envelope v1** (record key = `customer_id`):

```json
{
  "event_id": "uuid",
  "event_type": "CUSTOMER_CREATED | CUSTOMER_UPDATED | SUBSCRIPTION_CHANGED | QUOTA_EXCEEDED",
  "occurred_at": "2026-11-20T10:15:30Z",
  "producer": "customer-api",
  "schema_version": 1,
  "customer_id": "uuid",
  "...extras": "plan_type, previous_plan_type, company_name, old_status, new_status, feature_key"
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

A single statement, so no explicit transaction. The audit event is sent after the insert succeeded.

```mermaid
sequenceDiagram
    autonumber
    actor C as Client
    participant H as HttpServerVerticle
    participant CU as CustomerVerticle
    participant DB as PostgreSQL
    participant A as AuditVerticle
    participant K as Kafka
    C ->> H: POST /customers {email, companyName}
    H ->> CU: request("customer", operationId=createCustomer)
    CU ->> DB: INSERT INTO customers ... RETURNING *
    alt email already used (23505 on lower(email))
        DB -->> CU: unique violation
        CU -->> H: reply 409 EMAIL_ALREADY_USED
        H -->> C: 409 application/problem+json
    else inserted
        DB -->> CU: the row
        CU -) A: send("audit", CUSTOMER_CREATED)
        CU -->> H: reply 201 + Location
        H -->> C: 201 Created, Location: /customers/{id}
        A ->> K: produce(key = customer_id, value = envelope)
    end
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
    participant A as AuditVerticle
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
            CU -) A: send(SUBSCRIPTION_CHANGED, plan_type, previous_plan_type)
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
    participant A as AuditVerticle
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
            CU ->> DB: COMMIT
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
                    CU -) A: send(QUOTA_EXCEEDED, feature_key)
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
    participant A as AuditVerticle
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
        CU -) A: send(CUSTOMER_UPDATED, company_name, old_status, new_status)
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

### 5.7 Audit delivery

```mermaid
sequenceDiagram
    autonumber
    participant CU as CustomerOperations
    participant AS as AuditSenderImpl
    participant EB as Event bus
    participant AH as AuditHandlerImpl
    participant KW as KafkaAuditWriter
    participant K as Kafka
    CU ->> AS: send(action, customerId, extras)
    AS ->> AS: build the envelope (event_id, event_type, occurred_at, producer, schema_version, customer_id)
    AS -) EB: send("audit", JSON) no reply expected
    EB ->> AH: Message[String]
    AH ->> KW: audit(msg)
    KW ->> K: producer.send(topic, key=customer_id, value=msg) acks=all, idempotent
    alt failure
        K -->> KW: error
        KW -->> AH: failed Future
        AH ->> AH: log the error, the event is lost
    end
```

Records of one customer share a key, hence a partition, so their order is kept.

### 5.8 Readiness (`GET /health/ready`)

```mermaid
sequenceDiagram
    autonumber
    actor P as Probe
    participant H as HttpServerVerticle
    participant CH as CustomerHandlerImpl
    participant AH as AuditHandlerImpl
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
        AH -->> H: "pong" (or fail 503)
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
| `GET /health/ready` | `customer-verticle` (a customer node runs `SELECT 1`) and `audit-verticle` (Kafka `describeCluster`) | 200 UP, 503 DOWN |

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
- **Audit is fire-and-forget.** There is no outbox: if the audit verticle or Kafka is down, events are logged and lost.
  Consequently:
    - in `subscribe`, `update` and the `QUOTA_EXCEEDED` path, the event is sent *before* the transaction commits, so a
      failed commit can leave an event for a change that did not happen;
    - a successful consume emits no event (high volume).
- **Timeouts do not cancel work.** After the 30 s the client gets a 504 but the transaction can still commit. Consume
  has no idempotency key, so a retry after a 504 can consume twice. Check `GET /entitlements` first.
- **No authentication** on the HTTP API or on the Hazelcast cluster. Run it on a trusted network only.

See the [README](../README.md) for setup, tests and the full list of known limitations.
