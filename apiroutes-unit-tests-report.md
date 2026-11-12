# ApiRoutes unit tests: findings report

Class `me.bataillec.portfolio.customer.http.ApiRoutes`. No production code was changed. The proposed fixes below are suggestions only.

## Result

| Use-case | Cases |
|---|---|
| A. Routing: each operation calls its sender method | 9 |
| B. Response mapping | 5 |
| C. Sender failure | 6 |
| D. Contract validation | 10 |
| E. Routing errors | 4 |
| F. Body limit | 1 |
| G. `/openapi.json` | 2 |
| H. Health | 6 |
| I. `build()` fails without the contract | 1 |
| **New (`ApiRoutesSpec`)** | **44** |

Full unit suite: 322 tests, 0 failures (278 before this spec).

Statement coverage (scoverage, unit suites only): `ApiRoutes` is at 100% of statements (72/72) and 100% of branches.
The whole project is at 60.8% statements and 48.9% branches with unit suites only, because the integration specs are excluded from `mvn test`. The pom's 85% gate runs at `verify`.

Harness: a real `Vertx`, the real Router from `ApiRoutes.build` served on a random port and called through `WebClient`, and a Mockito `CustomerSender` reset between tests. The health tests register tiny event bus consumers on `CUSTOMER_HEALTH_ADDRESS` / `AUDIT_HEALTH_ADDRESS` and unregister them after each test. Every await is bounded to 5 s (10 s for the timeout test). Nothing was added to `support/`.

## Known gaps (documented by tests, not fixed)

| # | Finding | Test | Suggested fix |
|---|---|---|---|
| 1 | **`/openapi.json` sets the wrong header.** `ApiRoutes` calls `putHeader(CONTENT_TYPE_FIELD, …)`, and `CONTENT_TYPE_FIELD` is `"contentType"` (the JSON field name). The response has no `Content-Type` and a stray `contentType: application/json` header. | G: "answer 200 but with a wrong 'contentType' header…" | Use `CONTENT_TYPE` instead of `CONTENT_TYPE_FIELD` |
| 2 | **406 is never produced.** The OpenAPI router does not enforce `Accept`, so `GET /customers` with `Accept: text/plain` returns 200. The 406 error handler is dead code. | E: "the Accept header is not supported: still served…" | Enforce `Accept` in a handler, or drop 406 from `ROUTING_ERROR_STATUSES` |
| 3 | **415 is never produced.** The contract validation rejects the body first, so an unsupported `Content-Type` gives 400 `VALIDATION_FAILED`. The 415 error handler is dead code. | E: "the Content-Type is not supported: 400…" | Drop 415 from `ROUTING_ERROR_STATUSES`, or map it explicitly if 415 is wanted |
| 4 | A sender that **throws synchronously** gives 500 only because Vert.x Web turns the exception into `ctx.fail`. `op` handles only failed Futures. Same as gap #1 of the eventbus report. | C: "the sender throws synchronously…" | Wrap the handler call in `Try` and convert it to a failed Future |
| 5 | A sender that **replies with a non-JSON string** makes `makeResponse` throw inside `Future.map`. The result is a generic 500, with the stack trace only in the log. | C: "the sender replies with something that is not JSON…" | Decode defensively and answer with a clear 500 problem |

## Observed but not tested

- `HEALTH_CHECK_TIMEOUT` (10 s) can never fire, because the 4 s `HEALTH_PING_TIMEOUT_MS` always wins. The ready-timeout test therefore takes about 4 s.
- The contract is applied to requests only. Whatever status or content type the sender returns is forwarded as is.
- The body limit test only covers a body over `BODY_LIMIT` (rejected with 413). A body exactly at the limit is not tested.
- `Problem.withTraceId` takes the second `-` separated part of `traceparent` without validating the format. This is outside `ApiRoutes`.
- Cosmetic: the comment "dependencies,each" in `ApiRoutes` has a missing space.

## How to run

```
DOCKER_HOST=unix:///run/user/1000/podman/podman.sock TESTCONTAINERS_RYUK_DISABLED=true mise exec -- mvn test
mise exec -- mvn scoverage:report          # coverage report: target/site/scoverage/index.html
```

`-Dsuites=` does not isolate one spec, because the pom fixes `wildcardSuites`. Filter the output by `ApiRoutesSpec` instead. The unit suite takes about 10 s.
