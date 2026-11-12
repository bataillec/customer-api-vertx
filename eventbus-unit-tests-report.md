# Eventbus unit tests: findings report

Package `me.bataillec.portfolio.customer.eventbus`. No production code was changed. The proposed fixes below are suggestions only.

## Result

| Spec | Cases |
|---|---|
| `AuditSenderImplSpec` | 10 |
| `CustomerSenderImplSpec` | 34 |
| `AuditHandlerImplSpec` | 16 |
| `CustomerHandlerImplSpec` | 52 |
| **New** | **112** |

Full unit suite: 278 tests, 0 failures.

Statement coverage (scoverage, unit suites only): `AuditSenderImpl`, `CustomerSenderImpl`, `AuditHandlerImpl` and `CustomerHandlerImpl` are each at 100% of statements and branches, as are `EventbusUtils` and `SenderUtils`.
The whole project is at 54.3% statements with unit suites only, because the integration specs are excluded from `mvn test`. The pom's 85% gate runs at `verify`.

Shared test code: `support/EventBusFixtures.scala` (`EventBusFixture`, `TestMessages`). It replaces the private `ConsumerFixture` of `SenderUtilsSpec`, which now uses it.

## Known gaps (documented by tests, not fixed)

| # | Finding | Test | Suggested fix |
|---|---|---|---|
| 1 | `CustomerSenderImpl` methods **throw synchronously** on invalid JSON body or missing path param, instead of returning a failed `Future`. `ApiRoutes.op` expects `.onFailure`. It works only because the Vert.x Web router turns a handler exception into `ctx.fail`. | `CustomerSenderImplSpec`: "throw synchronously (no failed Future)…" (4 body + 6 id cases) | Wrap each method body: `Try(...).fold(Future.failedFuture, identity)` |
| 2 | `AuditHandlerImpl` swallows failures with `Try`, which does **not** catch `InterruptedException` (or `LinkageError`, `VirtualMachineError`). They escape the consumer. | `AuditHandlerImplSpec`: "let an InterruptedException escape" | `catch { case NonFatal(e) => … }` is the same, so decide whether to also catch `InterruptedException` and restore the interrupt flag |
| 3 | `AuditHandlerImpl.close()` propagates the exception of `operations.close()`. | "propagate the exception of operations.close()" | Probably acceptable. Wrap and log if shutdown should never throw |
| 4 | `CustomerHandlerImpl` failure messages are inconsistent. The synchronous path sends `exception.toString` (e.g. `java.lang.Exception: Unknown OperationId x`). The async path sends `e.getMessage`, which can be `null`. | "fail with 500 and the exception text…", "fail with 500 and a null message…" | Use `Option(e.getMessage).getOrElse(e.toString)` on both paths |
| 5 | `CustomerHandlerImpl` does not validate required JSON fields. `{"company_name":"ACME"}` reaches `createCustomer` as `CreateCustomer(null, Some("ACME"))`. Validation relies entirely on the OpenAPI contract upstream. | "forward a body without email (known gap…)" | Keep the contract as the guard, or add a null check in the decoder |
| 6 | An invalid `status`, a non-numeric `limit` or a malformed UUID gives **500** from the handler (400 would be more accurate). The contract normally stops these first. | the `listCustomers` and `reject an invalid request` groups | Map decoding errors to a 400 Problem |
| 7 | A `null` extra value in `AuditSenderImpl.send` is written as a JSON `null`. | "write a null value as a JSON null" | Filter nulls out if it matters |

## Observed but not tested

- `AuditSenderImpl.send`: an extra with key `action` or `customer_id` overwrites the core field, because extras are written last. Skipped on request. It could corrupt the audit trail. *Fix:* write the extras first, or reject reserved keys.
- The audit health check is a plain `pong`. It does not verify that the audit file is writable, unlike the customer one, which pings Postgres.
- `AuditHandlerImpl` calls `operations.audit` on the event loop, so a blocking file write blocks it.
- A synchronous throw from `operations.ping()` in `CustomerHandlerImpl` escapes the health consumer. This is the same known gap as in `SenderUtilsSpec`.

## How to run

```
mise exec -- mvn test                      # all unit suites (the pom restricts wildcardSuites to ...unit)
mise exec -- mvn scoverage:report          # coverage report: target/site/scoverage/index.html
```

`-Dsuites=` does not isolate one spec, because the pom fixes `wildcardSuites`. `-DwildcardSuites=<fqcn>` is accepted but also ran the full package in this setup. The unit suite takes about 5 s, so filter the output by spec name instead.
