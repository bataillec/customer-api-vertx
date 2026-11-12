package me.bataillec.portfolio.customer.unit.http

import io.vertx.core.buffer.Buffer
import io.vertx.core.eventbus.{MessageConsumer, Message, ReplyException, ReplyFailure}
import io.vertx.core.file.FileSystemOptions
import io.vertx.core.http.HttpMethod
import io.vertx.core.json.JsonObject
import io.vertx.core.{Future, MultiMap, Vertx, VertxOptions}
import io.vertx.ext.web.RoutingContext
import io.vertx.ext.web.client.WebClient
import me.bataillec.portfolio.customer.Constants.*
import me.bataillec.portfolio.customer.eventbus.CustomerSender
import me.bataillec.portfolio.customer.http.{ApiRoutes, HttpResponse}
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.*
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks.*
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import java.util.concurrent.TimeUnit
import scala.collection.mutable
import scala.jdk.CollectionConverters.*

class ApiRoutesSpec extends AnyWordSpec with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {
  private val customerId  = "11111111-2222-3333-4444-555555555555"
  private val traceId     = "4bf92f3577b34da6a3ce929d0e0e4736"
  private val traceparent = s"00-$traceId-00f067aa0ba902b7-01"
  private val customer    =
    s"""{"id":"$customerId","email":"a@b.com","status":"ACTIVE","created_at":"2026-01-01T00:00:00Z","updated_at":"2026-01-01T00:00:00Z"}"""
  private val problem     = """{"title":"Customer not found","status":404,"code":"CUSTOMER_NOT_FOUND"}"""

  // region Fixture: one Vertx, one mocked sender, one real Router served on a random port

  private case class Res(status: Int, headers: MultiMap, body: String) {
    def contentType: String = Option(headers.get("content-type")).getOrElse("")
    def json: JsonObject    = JsonObject(body)
  }

  private val vertx  = Vertx.vertx()
  private val sender = mock(classOf[CustomerSender])
  private val client = WebClient.create(vertx)
  private val server = vertx.createHttpServer().requestHandler(await(ApiRoutes.build(vertx, sender))).listen(0)
  await(server)
  private val port   = server.result().actualPort()

  private val consumers = mutable.ListBuffer.empty[MessageConsumer[String]]

  override protected def beforeEach(): Unit = reset(sender)

  override protected def afterEach(): Unit = {
    consumers.foreach(c => await(c.unregister()))
    consumers.clear()
  }

  override protected def afterAll(): Unit = {
    await(vertx.close())
    ()
  }

  // endregion

  // region Helpers

  private def await[T](future: Future[T], seconds: Int = 5): T =
    future.toCompletionStage.toCompletableFuture.get(seconds, TimeUnit.SECONDS)

  private def call(method: String, path: String, body: Option[String] = None, headers: Map[String, String] = Map.empty, seconds: Int = 5): Res = {
    val request = client.request(HttpMethod.valueOf(method), port, "localhost", path)
    headers.foreach((k, v) => request.putHeader(k, v))
    body.foreach(_ => request.putHeader("Content-Type", "application/json"))
    headers.get("Content-Type").foreach(request.putHeader("Content-Type", _))
    val sent = body.fold(request.send())(b => request.sendBuffer(Buffer.buffer(b)))
    await(sent.map(r => Res(r.statusCode(), r.headers(), Option(r.bodyAsString()).getOrElse(""))), seconds)
  }

  /** Makes `GET /plans` (getPlans) answer with `response`. */
  private def replyPlans(response: HttpResponse): Unit =
    when(sender.getPlans(any[RoutingContext]())).thenReturn(Future.succeededFuture(response.toJson.encode()))

  private def failPlans(failure: Throwable): Unit =
    when(sender.getPlans(any[RoutingContext]())).thenReturn(Future.failedFuture[String](failure))

  private def assertProblem(res: Res, status: Int, code: String): Unit = {
    res.status shouldBe status
    res.contentType should startWith(PROBLEM_CONTENT_TYPE)
    res.json.getString(CODE_FIELD) shouldBe code
    res.json.getInteger(STATUS_FIELD) shouldBe status
  }

  private def register(address: String)(handler: Message[String] => Unit): Unit = {
    val consumer = vertx.eventBus().consumer[String](address, msg => handler(msg))
    await(consumer.completion())
    consumers += consumer
  }

  private def healthy(address: String): Unit = register(address)(_.reply("pong"))

  private def broken(address: String): Unit = register(address)(_.fail(503, "down"))

  private def silent(address: String): Unit = register(address)(_ => ())

  // endregion

  "ApiRoutes" should {

    // region A. Routing

    "route each operation to the matching sender method" when {
      val operations = Table(
        ("operation", "method", "path", "body", "stub"),
        ("createCustomer", "POST", "/customers", Some("""{"email":"a@b.com"}"""), (s: CustomerSender, rc: RoutingContext) => s.createCustomer(rc)),
        ("listCustomers", "GET", "/customers?limit=10", None, (s: CustomerSender, rc: RoutingContext) => s.getCustomers(rc)),
        ("getCustomer", "GET", s"/customers/$customerId", None, (s: CustomerSender, rc: RoutingContext) => s.getCustomer(rc)),
        ("updateCustomer", "PATCH", s"/customers/$customerId", Some("""{"status":"SUSPENDED"}"""), (s: CustomerSender, rc: RoutingContext) => s.updateCustomer(rc)),
        ("subscribe", "POST", s"/customers/$customerId/subscriptions", Some("""{"plan_type":"PRO"}"""), (s: CustomerSender, rc: RoutingContext) => s.subscribe(rc)),
        ("listSubscriptions", "GET", s"/customers/$customerId/subscriptions", None, (s: CustomerSender, rc: RoutingContext) => s.getSubscriptions(rc)),
        ("listEntitlements", "GET", s"/customers/$customerId/entitlements", None, (s: CustomerSender, rc: RoutingContext) => s.getEntitlements(rc)),
        ("listPlans", "GET", "/plans", None, (s: CustomerSender, rc: RoutingContext) => s.getPlans(rc)),
        (
          "consumeEntitlement",
          "POST",
          s"/customers/$customerId/entitlements/consume",
          Some("""{"feature_key":"API_CALLS_MONTHLY","amount":1}"""),
          (s: CustomerSender, rc: RoutingContext) => s.consumeEntitlement(rc)
        )
      )

      forAll(operations) { (name: String, method: String, path: String, body: Option[String], invoke: (CustomerSender, RoutingContext) => Future[String]) =>
        s"$name: $method $path" in {
          when(invoke(sender, any[RoutingContext]())).thenReturn(Future.succeededFuture(HttpResponse(200).toJson.encode()))

          call(method, path, body).status shouldBe 200

          invoke(verify(sender, times(1)), any[RoutingContext]())
          verifyNoMoreInteractions(sender)
        }
      }
    }

    // endregion

    // region B. Response mapping

    "map the sender reply to the HTTP response" when {
      "use the status, body and content type of the reply" in {
        when(sender.createCustomer(any[RoutingContext]())).thenReturn(Future.succeededFuture(HttpResponse(201, customer).toJson.encode()))

        val res = call("POST", "/customers", Some("""{"email":"a@b.com"}"""))

        res.status shouldBe 201
        res.body shouldBe customer
        res.contentType should startWith(DEFAULT_CONTENT_TYPE)
      }

      "copy the extra headers of the reply" in {
        val location = s"/customers/$customerId"
        when(sender.createCustomer(any[RoutingContext]()))
          .thenReturn(Future.succeededFuture(HttpResponse(201, customer, headers = Map("Location" -> location)).toJson.encode()))

        call("POST", "/customers", Some("""{"email":"a@b.com"}""")).headers.get("Location") shouldBe location
      }

      "keep the problem+json content type of a problem reply" in {
        when(sender.getCustomer(any[RoutingContext]()))
          .thenReturn(Future.succeededFuture(HttpResponse(404, problem, PROBLEM_CONTENT_TYPE).toJson.encode()))

        assertProblem(call("GET", s"/customers/$customerId"), 404, "CUSTOMER_NOT_FOUND")
      }

      "add the trace id of the traceparent header to a problem reply" in {
        when(sender.getCustomer(any[RoutingContext]()))
          .thenReturn(Future.succeededFuture(HttpResponse(404, problem, PROBLEM_CONTENT_TYPE).toJson.encode()))

        val res = call("GET", s"/customers/$customerId", headers = Map("traceparent" -> traceparent))

        res.json.getString(TRACE_ID_FIELD) shouldBe traceId
        res.json.getString(TITLE_FIELD) shouldBe "Customer not found"
      }

      "not add a trace id to a successful reply" in {
        when(sender.getCustomer(any[RoutingContext]())).thenReturn(Future.succeededFuture(HttpResponse(200, customer).toJson.encode()))

        val res = call("GET", s"/customers/$customerId", headers = Map("traceparent" -> traceparent))

        res.body shouldBe customer
      }
    }

    // endregion

    // region C. Sender failure

    "turn a sender failure into a problem+json response" when {
      "the Future fails with a TIMEOUT ReplyException" in {
        failPlans(new ReplyException(ReplyFailure.TIMEOUT, "timeout"))

        assertProblem(call("GET", "/plans"), 504, TIMEOUT_COMMUNICATION)
      }

      "the Future fails with a NO_HANDLERS ReplyException" in {
        failPlans(new ReplyException(ReplyFailure.NO_HANDLERS, "no handlers"))

        assertProblem(call("GET", "/plans"), 503, NO_HANDLERS)
      }

      "the Future fails with a RECIPIENT_FAILURE ReplyException: use its code as status" in {
        failPlans(new ReplyException(ReplyFailure.RECIPIENT_FAILURE, 409, "rejected"))

        assertProblem(call("GET", "/plans"), 409, RECIPIENT_FAILURE)
      }

      "the Future fails with an arbitrary exception: 500 without leaking the message" in {
        failPlans(new RuntimeException("secret internal detail"))

        val res = call("GET", "/plans")

        assertProblem(res, 500, INTERNAL_ERROR)
        res.body should not include "secret internal detail"
      }

      "the sender throws synchronously: 500 (known gap, only works because Vert.x Web catches the exception)" in {
        when(sender.getPlans(any[RoutingContext]())).thenThrow(new RuntimeException("boom"))

        assertProblem(call("GET", "/plans"), 500, INTERNAL_ERROR)
      }

      "the sender replies with something that is not JSON: 500 (known gap, nothing guards makeResponse)" in {
        when(sender.getPlans(any[RoutingContext]())).thenReturn(Future.succeededFuture("not json"))

        assertProblem(call("GET", "/plans"), 500, INTERNAL_ERROR)
      }
    }

    // endregion

    // region D. Contract validation

    "reject an invalid request with 400 and never call the sender" when {
      val invalid = Table(
        ("case", "method", "path", "body"),
        ("body without the required email", "POST", "/customers", Some("""{"company_name":"ACME"}""")),
        ("email that is not an email", "POST", "/customers", Some("""{"email":"not-an-email"}""")),
        ("malformed JSON body", "POST", "/customers", Some("""{"email":""")),
        ("missing body", "POST", "/customers", None),
        ("malformed customer id", "GET", "/customers/not-a-uuid", None),
        ("limit below the minimum", "GET", "/customers?limit=0", None),
        ("non numeric limit", "GET", "/customers?limit=abc", None),
        ("unknown status", "GET", "/customers?status=BOGUS", None),
        ("unknown plan_type", "POST", s"/customers/$customerId/subscriptions", Some("""{"plan_type":"GOLD"}""")),
        ("consume with amount 0", "POST", s"/customers/$customerId/entitlements/consume", Some("""{"feature_key":"API_CALLS_MONTHLY","amount":0}"""))
      )

      forAll(invalid) { (name: String, method: String, path: String, body: Option[String]) =>
        s"$name" in {
          assertProblem(call(method, path, body), 400, VALIDATION_FAILED)

          verifyNoInteractions(sender)
        }
      }
    }

    // endregion

    // region E. Routing errors

    "answer a routing error with problem+json" when {
      "the path is unknown: 404" in {
        assertProblem(call("GET", "/nope"), 404, NOT_FOUND)

        verifyNoInteractions(sender)
      }

      "the method is not allowed on the path: 405" in {
        assertProblem(call("DELETE", "/customers"), 405, HTTP_ERROR)

        verifyNoInteractions(sender)
      }

      // Known gap: the OpenAPI router does not enforce Accept, so the 406 error handler is never reached for contract routes.
      "the Accept header is not supported: still served (known gap, no 406)" in {
        when(sender.getCustomers(any[RoutingContext]())).thenReturn(Future.succeededFuture(HttpResponse(200).toJson.encode()))

        call("GET", "/customers", headers = Map("Accept" -> "text/plain")).status shouldBe 200
      }

      // Known gap: the contract validation rejects the body first, so this is a 400 and the 415 error handler is never reached.
      "the Content-Type is not supported: 400 from the contract validation (known gap, no 415)" in {
        assertProblem(call("POST", "/customers", Some("email=a@b.com"), Map("Content-Type" -> "text/plain")), 400, VALIDATION_FAILED)

        verifyNoInteractions(sender)
      }
    }

    // endregion

    // region F. Body limit

    "reject a body larger than BODY_LIMIT" in {
      val body = s"""{"email":"a@b.com","company_name":"${"x" * BODY_LIMIT}"}"""

      assertProblem(call("POST", "/customers", Some(body)), 413, HTTP_ERROR)

      verifyNoInteractions(sender)
    }

    // endregion

    // region G. /openapi.json

    "serve the contract on /openapi.json" when {
      // Known gap: ApiRoutes sets the header named CONTENT_TYPE_FIELD ("contentType", the JSON field name) instead of CONTENT_TYPE ("Content-Type").
      "answer 200 but with a wrong 'contentType' header instead of Content-Type (known gap)" in {
        val res = call("GET", "/openapi.json")

        res.status shouldBe 200
        res.headers.contains("Content-Type") shouldBe false
        res.headers.get(CONTENT_TYPE_FIELD) shouldBe DEFAULT_CONTENT_TYPE
      }

      "return the OpenAPI document with the 9 operations" in {
        val contract = call("GET", "/openapi.json").json

        val operationIds = contract
          .getJsonObject("paths")
          .getMap
          .values()
          .asScala
          .flatMap(path => JsonObject(path.asInstanceOf[java.util.Map[String, AnyRef]]).getMap.values().asScala)
          .collect { case op: java.util.Map[?, ?] if op.containsKey(OPERATION_ID) => op.get(OPERATION_ID) }
          .toSet

        contract.getString("openapi") shouldBe "3.0.3"
        operationIds should contain allElementsOf Set(
          createCustomer_ID,
          listCustomers_ID,
          getCustomer_ID,
          updateCustomer_ID,
          subscribe_ID,
          listSubscriptions_ID,
          listEntitlements_ID,
          listPlans_ID,
          consumeEntitlement_ID
        )
      }
    }

    // endregion

    // region H. Health

    "answer the health endpoints" when {
      "live: 200 without any dependency" in {
        call("GET", "/health/live").status shouldBe 200
      }

      "ready: 200 when the customer and audit pings succeed" in {
        healthy(CUSTOMER_HEALTH_ADDRESS)
        healthy(AUDIT_HEALTH_ADDRESS)

        call("GET", "/health/ready").status shouldBe 200
      }

      "ready: 503 when the customer ping fails" in {
        broken(CUSTOMER_HEALTH_ADDRESS)
        healthy(AUDIT_HEALTH_ADDRESS)

        call("GET", "/health/ready").status shouldBe 503
      }

      "ready: 503 when the audit ping fails" in {
        healthy(CUSTOMER_HEALTH_ADDRESS)
        broken(AUDIT_HEALTH_ADDRESS)

        call("GET", "/health/ready").status shouldBe 503
      }

      "ready: 503 when nobody listens on the customer address" in {
        healthy(AUDIT_HEALTH_ADDRESS)

        call("GET", "/health/ready").status shouldBe 503
      }

      "ready: 503 when the customer ping times out" in {
        silent(CUSTOMER_HEALTH_ADDRESS)
        healthy(AUDIT_HEALTH_ADDRESS)

        call("GET", "/health/ready", seconds = 10).status shouldBe 503
      }
    }

    // endregion

    // region I. Contract not loadable

    "fail to build when the contract cannot be loaded" in {
      // Without classpath resolving, "openapi.yaml" is looked up in the working directory, where there is none.
      val noClasspath = Vertx.vertx(new VertxOptions().setFileSystemOptions(new FileSystemOptions().setClassPathResolvingEnabled(false)))
      try {
        val result = ApiRoutes.build(noClasspath, sender)

        a[Exception] should be thrownBy await(result)
        result.failed() shouldBe true
      } finally await(noClasspath.close())
    }

    // endregion
  }
}
