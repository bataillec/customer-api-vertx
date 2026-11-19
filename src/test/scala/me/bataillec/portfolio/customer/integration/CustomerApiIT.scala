package me.bataillec.portfolio.customer.integration

import io.vertx.core.http.HttpMethod
import io.vertx.core.json.JsonObject
import me.bataillec.portfolio.customer.support.{ApiResponse, AppFixture}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.util.UUID
import java.util.concurrent.{Executors, TimeUnit}
import scala.jdk.CollectionConverters.*

/** The whole app (HTTP, event bus, Postgres, Kafka) driven through its public HTTP API. */
class CustomerApiIT extends AnyWordSpec with Matchers with AppFixture {

  private def newCustomerId(): String = {
    val created = api.createCustomer()
    created.status shouldBe 201
    created.string("id")
  }

  private def problem(response: ApiResponse, status: Int, code: String): Unit = {
    response.status shouldBe status
    response.header("content-type").map(_.takeWhile(_ != ';')) shouldBe Some("application/problem+json")
    response.code shouldBe code
    response.json.getInteger("status") shouldBe status
    response.json.getString("title") should not be empty
  }

  private def entitlements(customerId: String): Map[String, JsonObject] =
    api.get(s"/customers/$customerId/entitlements").jsonArray.asScala.map(_.asInstanceOf[JsonObject]).map(e => e.getString("feature_key") -> e).toMap

  "GET /plans" should {
    "list the configured plans with their quotas" in {
      val response = api.get("/plans")

      response.status shouldBe 200
      val plans = response.jsonArray.asScala.map(_.asInstanceOf[JsonObject]).map(p => p.getString("plan_type") -> p.getJsonObject("quotas")).toMap
      plans.keySet shouldBe Set("FREE_TIER", "PRO", "ENTERPRISE")
      plans("PRO").getJsonObject("API_CALLS_MONTHLY").getLong("quota") shouldBe 100000L
      plans("PRO").getJsonObject("API_CALLS_MONTHLY").getBoolean("resets_monthly") shouldBe true
      plans("PRO").getJsonObject("STORAGE_GB").getBoolean("resets_monthly") shouldBe false
    }
  }

  "customers" should {
    "be created, then read back" in {
      val created = api.createCustomer("jane@example.com", Some("Acme"))

      created.status shouldBe 201
      val id = created.string("id")
      created.header("location") shouldBe Some(s"/customers/$id")
      created.string("email") shouldBe "jane@example.com"
      created.string("company_name") shouldBe "Acme"
      created.string("status") shouldBe "ACTIVE"
      created.json.getString("created_at") should not be null

      val fetched = api.get(s"/customers/$id")
      fetched.status shouldBe 200
      fetched.json shouldBe created.json
    }

    "be created without a company name" in {
      val created = api.createCustomer(companyName = None)

      created.status shouldBe 201
      created.json.containsKey("company_name") shouldBe false
    }

    "have their company name updated" in {
      val id = api.createCustomer(companyName = Some("Old")).string("id")

      val updated = api.patch(s"/customers/$id", JsonObject().put("company_name", "New"))

      updated.status shouldBe 200
      updated.string("company_name") shouldBe "New"
      updated.string("status") shouldBe "ACTIVE"
      api.get(s"/customers/$id").string("company_name") shouldBe "New"
    }

    "refuse an email already used, whatever its case" in {
      api.createCustomer("taken@example.com").status shouldBe 201

      problem(api.createCustomer("taken@example.com"), 409, "EMAIL_ALREADY_USED")
      problem(api.createCustomer("TAKEN@Example.com"), 409, "EMAIL_ALREADY_USED")
    }

    "return 404 for an unknown customer" in {
      problem(api.get(s"/customers/${UUID.randomUUID()}"), 404, "CUSTOMER_NOT_FOUND")
      problem(api.patch(s"/customers/${UUID.randomUUID()}", JsonObject().put("company_name", "x")), 404, "CUSTOMER_NOT_FOUND")
    }

    "be listed newest first, page by page with the cursor" in {
      val ids = (1 to 5).map(_ => newCustomerId()).reverse

      val first = api.get("/customers?limit=2")
      first.status shouldBe 200
      first.json.getJsonArray("items").asScala.map(_.asInstanceOf[JsonObject].getString("id")).toList shouldBe ids.take(2)
      val cursor1 = first.string("next_cursor")
      cursor1 should not be null

      val second = api.get(s"/customers?limit=2&cursor=$cursor1")
      second.json.getJsonArray("items").asScala.map(_.asInstanceOf[JsonObject].getString("id")).toList shouldBe ids.slice(2, 4)

      val third = api.get(s"/customers?limit=2&cursor=${second.string("next_cursor")}")
      third.json.getJsonArray("items").asScala.map(_.asInstanceOf[JsonObject].getString("id")).toList shouldBe ids.drop(4)
      third.json.containsKey("next_cursor") shouldBe false
    }

    "be listed by status, the DELETED ones being hidden by default" in {
      val active  = newCustomerId()
      val deleted = newCustomerId()
      api.setStatus(deleted, "DELETED").status shouldBe 200

      def ids(path: String) = api.get(path).json.getJsonArray("items").asScala.map(_.asInstanceOf[JsonObject].getString("id")).toList

      ids("/customers") shouldBe List(active)
      ids("/customers?status=DELETED") shouldBe List(deleted)
    }

    "reject a cursor that is garbage" in {
      problem(api.get("/customers?cursor=garbage"), 400, "INVALID_CURSOR")
    }
  }

  "subscriptions" should {
    "grant the entitlements of the plan" in {
      val id = newCustomerId()

      val subscribed = api.subscribe(id, "FREE_TIER")

      subscribed.status shouldBe 201
      subscribed.header("location") shouldBe Some(s"/customers/$id/subscriptions")
      subscribed.string("plan_type") shouldBe "FREE_TIER"
      subscribed.string("status") shouldBe "ACTIVE"
      val granted = subscribed.json.getJsonArray("entitlements").asScala.map(_.asInstanceOf[JsonObject]).map(e => e.getString("feature_key") -> e).toMap
      granted("API_CALLS_MONTHLY").getLong("allocated_quota") shouldBe 1000L
      granted("API_CALLS_MONTHLY").getLong("consumed_quota") shouldBe 0L
      granted("API_CALLS_MONTHLY").getString("reset_date") should not be null
      granted("STORAGE_GB").containsKey("reset_date") shouldBe false

      entitlements(id).keySet shouldBe Set("API_CALLS_MONTHLY", "STORAGE_GB")
    }

    "list the history, newest first, with the previous one canceled by a plan change" in {
      val id = newCustomerId()
      api.subscribe(id, "FREE_TIER").status shouldBe 201
      api.subscribe(id, "PRO").status shouldBe 201

      val history = api.get(s"/customers/$id/subscriptions")

      history.status shouldBe 200
      history.jsonArray.asScala.map(_.asInstanceOf[JsonObject]).map(s => (s.getString("plan_type"), s.getString("status"))).toList shouldBe
        List(("PRO", "ACTIVE"), ("FREE_TIER", "CANCELED"))
    }

    "keep the usage on a plan change, only the allocation follows" in {
      val id = newCustomerId()
      api.subscribe(id, "FREE_TIER")
      api.consume(id, "API_CALLS_MONTHLY", 300).status shouldBe 200

      api.subscribe(id, "PRO").status shouldBe 201

      val e = entitlements(id)("API_CALLS_MONTHLY")
      e.getLong("allocated_quota") shouldBe 100000L
      e.getLong("consumed_quota") shouldBe 300L
    }

    "refuse to subscribe again to the active plan" in {
      val id = newCustomerId()
      api.subscribe(id, "PRO").status shouldBe 201

      problem(api.subscribe(id, "PRO"), 409, "PLAN_ALREADY_ACTIVE")
    }

    "refuse an unknown customer" in {
      problem(api.subscribe(UUID.randomUUID().toString, "PRO"), 404, "CUSTOMER_NOT_FOUND")
    }

    "refuse a customer that is not ACTIVE" in {
      val id = newCustomerId()
      api.setStatus(id, "SUSPENDED").status shouldBe 200

      problem(api.subscribe(id, "PRO"), 409, "CUSTOMER_NOT_ACTIVE")
    }

    "refuse the history and entitlements of an unknown customer" in {
      problem(api.get(s"/customers/${UUID.randomUUID()}/subscriptions"), 404, "CUSTOMER_NOT_FOUND")
      problem(api.get(s"/customers/${UUID.randomUUID()}/entitlements"), 404, "CUSTOMER_NOT_FOUND")
    }
  }

  "consuming an entitlement" should {
    "report what remains" in {
      val id = newCustomerId()
      api.subscribe(id, "FREE_TIER")

      val first  = api.consume(id, "API_CALLS_MONTHLY", 100)
      val second = api.consume(id, "API_CALLS_MONTHLY", 50)

      first.status shouldBe 200
      first.json shouldBe JsonObject().put("feature_key", "API_CALLS_MONTHLY").put("allocated_quota", 1000).put("consumed_quota", 100).put("remaining_quota", 900)
      second.json.getLong("remaining_quota") shouldBe 850L
      entitlements(id)("API_CALLS_MONTHLY").getLong("consumed_quota") shouldBe 150L
    }

    "answer 429 with a Retry-After once a monthly quota is exhausted" in {
      val id = newCustomerId()
      api.subscribe(id, "FREE_TIER")
      api.consume(id, "API_CALLS_MONTHLY", 1000).status shouldBe 200

      val refused = api.consume(id, "API_CALLS_MONTHLY", 1)

      problem(refused, 429, "QUOTA_EXCEEDED")
      refused.header("retry-after").map(_.toLong).getOrElse(0L) should be > 0L
    }

    "answer 429 without Retry-After when the usage never resets" in {
      val id = newCustomerId()
      api.subscribe(id, "FREE_TIER")
      api.consume(id, "STORAGE_GB", 1).status shouldBe 200

      val refused = api.consume(id, "STORAGE_GB", 1)

      problem(refused, 429, "QUOTA_EXCEEDED")
      refused.header("retry-after") shouldBe None
    }

    "answer 429 without Retry-After when the amount is bigger than the whole allocation" in {
      val id = newCustomerId()
      api.subscribe(id, "FREE_TIER")

      val refused = api.consume(id, "API_CALLS_MONTHLY", 5000)

      problem(refused, 429, "QUOTA_EXCEEDED")
      refused.header("retry-after") shouldBe None
    }

    "answer 404 for a feature the customer has no entitlement to" in {
      val id = newCustomerId()
      api.subscribe(id, "FREE_TIER")

      problem(api.consume(id, "NOPE", 1), 404, "ENTITLEMENT_NOT_FOUND")
    }

    "answer 404 for an unknown customer" in {
      problem(api.consume(UUID.randomUUID().toString, "API_CALLS_MONTHLY", 1), 404, "CUSTOMER_NOT_FOUND")
    }

    "answer 409 NO_ACTIVE_SUBSCRIPTION when the subscription was canceled" in {
      val id = newCustomerId()
      api.subscribe(id, "FREE_TIER")
      api.setStatus(id, "DELETED").status shouldBe 200
      api.setStatus(id, "ACTIVE").status shouldBe 200

      problem(api.consume(id, "API_CALLS_MONTHLY", 1), 409, "NO_ACTIVE_SUBSCRIPTION")
    }

    "never let concurrent calls exceed the quota" in {
      val id = newCustomerId()
      api.subscribe(id, "FREE_TIER")
      api.consume(id, "API_CALLS_MONTHLY", 970).status shouldBe 200 // 30 left

      val pool = Executors.newFixedThreadPool(16)
      val statuses =
        try {
          val calls = (1 to 60).map(_ => pool.submit(() => api.consume(id, "API_CALLS_MONTHLY", 1).status))
          calls.map(_.get(60, TimeUnit.SECONDS).intValue()).toList
        } finally pool.shutdown()

      statuses.count(_ == 200) shouldBe 30
      statuses.count(_ == 429) shouldBe 30
      entitlements(id)("API_CALLS_MONTHLY").getLong("consumed_quota") shouldBe 1000L
    }
  }

  "the customer lifecycle" should {
    "cancel the subscription on delete but keep the entitlements and their usage" in {
      val id = newCustomerId()
      api.subscribe(id, "FREE_TIER")
      api.consume(id, "API_CALLS_MONTHLY", 200).status shouldBe 200

      val deleted = api.setStatus(id, "DELETED")

      deleted.status shouldBe 200
      deleted.string("status") shouldBe "DELETED"
      api.get(s"/customers/$id/subscriptions").jsonArray.getJsonObject(0).getString("status") shouldBe "CANCELED"
      entitlements(id)("API_CALLS_MONTHLY").getLong("consumed_quota") shouldBe 200L
    }

    "let a DELETED customer come back to ACTIVE, with the usage kept until it subscribes again" in {
      val id = newCustomerId()
      api.subscribe(id, "FREE_TIER")
      api.consume(id, "API_CALLS_MONTHLY", 200).status shouldBe 200
      api.setStatus(id, "DELETED").status shouldBe 200

      api.setStatus(id, "ACTIVE").string("status") shouldBe "ACTIVE"
      entitlements(id)("API_CALLS_MONTHLY").getLong("consumed_quota") shouldBe 200L

      api.subscribe(id, "FREE_TIER").status shouldBe 201
      api.consume(id, "API_CALLS_MONTHLY", 1).json.getLong("consumed_quota") shouldBe 201L
    }

    "keep the email of a deleted customer reserved" in {
      val created = api.createCustomer("reserved@example.com")
      api.setStatus(created.string("id"), "DELETED").status shouldBe 200

      problem(api.createCustomer("reserved@example.com"), 409, "EMAIL_ALREADY_USED")
    }

    "refuse editing the company name of a DELETED customer" in {
      val id = newCustomerId()
      api.setStatus(id, "DELETED").status shouldBe 200

      problem(api.patch(s"/customers/$id", JsonObject().put("company_name", "x")), 409, "CUSTOMER_NOT_ACTIVE")
    }

    "refuse to consume while the customer is SUSPENDED" in {
      val id = newCustomerId()
      api.subscribe(id, "FREE_TIER")
      api.setStatus(id, "SUSPENDED").status shouldBe 200

      problem(api.consume(id, "API_CALLS_MONTHLY", 1), 409, "CUSTOMER_NOT_ACTIVE")
    }

    "allow renaming and reactivating a DELETED customer in one call" in {
      val id = newCustomerId()
      api.setStatus(id, "DELETED").status shouldBe 200

      val updated = api.patch(s"/customers/$id", JsonObject().put("company_name", "Back").put("status", "ACTIVE"))

      updated.status shouldBe 200
      updated.string("status") shouldBe "ACTIVE"
      updated.string("company_name") shouldBe "Back"
    }
  }

  "the validation" should {
    "reject a missing, malformed or too long email" in {
      problem(api.post("/customers", JsonObject()), 400, "VALIDATION_FAILED")
      problem(api.post("/customers", JsonObject().put("email", "not-an-email")), 400, "VALIDATION_FAILED")
      problem(api.post("/customers", JsonObject().put("email", "a" * 250 + "@example.com")), 400, "VALIDATION_FAILED")
    }

    "reject a company name that is too long" in {
      problem(api.post("/customers", JsonObject().put("email", "long@example.com").put("company_name", "x" * 256)), 400, "VALIDATION_FAILED")
    }

    "reject a body that is not JSON" in {
      val response = api.send(HttpMethod.POST, "/customers", Some("{not json"))

      problem(response, 400, "VALIDATION_FAILED")
    }

    "reject an id that is not a UUID" in {
      problem(api.get("/customers/not-a-uuid"), 400, "VALIDATION_FAILED")
    }

    "reject a limit out of range or a status that does not exist" in {
      problem(api.get("/customers?limit=0"), 400, "VALIDATION_FAILED")
      problem(api.get("/customers?limit=201"), 400, "VALIDATION_FAILED")
      problem(api.get("/customers?status=NOPE"), 400, "VALIDATION_FAILED")
    }

    "reject an unknown plan, a missing feature key and a non-positive amount" in {
      val id = newCustomerId()
      problem(api.subscribe(id, "GOLD"), 400, "VALIDATION_FAILED")
      problem(api.post(s"/customers/$id/entitlements/consume", JsonObject().put("amount", 1)), 400, "VALIDATION_FAILED")
      problem(api.consume(id, "API_CALLS_MONTHLY", 0), 400, "VALIDATION_FAILED")
    }

    "reject a body over 64 KB" in {
      val response = api.send(HttpMethod.POST, "/customers", Some(JsonObject().put("email", "big@example.com").put("company_name", "x" * (70 * 1024)).encode()))

      problem(response, 413, "HTTP_ERROR")
    }
  }

  "the errors" should {
    "answer 404 for a path that does not exist" in {
      problem(api.get("/nope"), 404, "NOT_FOUND")
    }

    "answer 405 with the Allow header for a method the path does not support" in {
      val id       = newCustomerId()
      val response = api.send(HttpMethod.DELETE, s"/customers/$id")

      problem(response, 405, "HTTP_ERROR")
      response.header("allow") shouldBe Some("GET, PATCH")
    }

    "carry the trace id of the request in the problem body" in {
      val traceId  = "0af7651916cd43dd8448eb211c80319c"
      val response = api.get(s"/customers/${UUID.randomUUID()}", Map("traceparent" -> s"00-$traceId-b7ad6b7169203331-01"))

      problem(response, 404, "CUSTOMER_NOT_FOUND")
      response.json.getString("trace_id") shouldBe traceId
    }

    "carry the trace id on a validation failure too" in {
      val traceId  = "0af7651916cd43dd8448eb211c80319c"
      val response = api.get("/customers/not-a-uuid", Map("traceparent" -> s"00-$traceId-b7ad6b7169203331-01"))

      response.json.getString("trace_id") shouldBe traceId
    }

    "omit the trace id without a traceparent" in {
      api.get(s"/customers/${UUID.randomUUID()}").json.containsKey("trace_id") shouldBe false
    }
  }

  "health and contract" should {
    "report the app alive" in {
      val live = api.get("/health/live")

      live.status shouldBe 200
      live.json.getString("status") shouldBe "UP"
    }

    "report the app ready once Postgres and Kafka are reachable" in {
      val ready = api.get("/health/ready")

      ready.status shouldBe 200
      ready.json.getString("status") shouldBe "UP"
      ready.json.getJsonArray("checks").asScala.map(_.asInstanceOf[JsonObject]).map(c => c.getString("id") -> c.getString("status")).toMap shouldBe
        Map("customer-verticle" -> "UP", "audit-verticle" -> "UP")
    }

    "publish the OpenAPI contract" in {
      val spec = api.get("/openapi.json")

      spec.status shouldBe 200
      spec.json.getString("openapi") should startWith("3.")
      spec.json.getJsonObject("paths").fieldNames().asScala.toSet should contain allOf ("/customers", "/customers/{id}", "/plans", "/health/ready")
    }
  }
}
