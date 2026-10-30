package me.bataillec.portfolio.customer.http

import io.vertx.core.Vertx
import io.vertx.core.buffer.Buffer
import io.vertx.core.eventbus.Message
import io.vertx.core.http.{HttpMethod, HttpServer}
import io.vertx.core.json.JsonObject
import io.vertx.ext.web.client.{HttpRequest, WebClient}
import me.bataillec.portfolio.customer.Constants.*
import me.bataillec.portfolio.customer.domain.DomainError
import me.bataillec.portfolio.customer.eventbus.CustomerSender
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.UUID
import java.util.concurrent.TimeUnit

class ApiRoutesSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {
  private val vertx = Vertx.vertx()

  private def await[T](future: io.vertx.core.Future[T]): T = future.toCompletionStage.toCompletableFuture.get(10, TimeUnit.SECONDS)

  private val customerId = "11111111-1111-1111-1111-111111111111"

  @volatile private var reply: HttpResponse = HttpResponse(200, "{}")

  private val server: HttpServer = {
    vertx.eventBus().consumer[String](CUSTOMER_ADDRESS, (msg: Message[String]) => msg.reply(reply.toJson.encode()))
    await(ApiRoutes.build(vertx, new CustomerSender(vertx.eventBus())).compose(router => vertx.createHttpServer().requestHandler(router).listen(0)))
  }

  private val client = WebClient.create(vertx)

  override def afterAll(): Unit = await(vertx.close())

  private def request(method: HttpMethod, path: String): HttpRequest[Buffer] =
    client.request(method, server.actualPort(), "localhost", path)

  private def post(path: String, body: String) =
    await(request(HttpMethod.POST, path).putHeader(CONTENT_TYPE, DEFAULT_CONTENT_TYPE).sendBuffer(Buffer.buffer(body)))

  private def problem(response: io.vertx.ext.web.client.HttpResponse[Buffer], status: Int, code: String): JsonObject = {
    response.statusCode() shouldBe status
    response.getHeader(CONTENT_TYPE) shouldBe PROBLEM_CONTENT_TYPE
    val body = response.bodyAsJsonObject()
    body.getInteger("status") shouldBe status
    body.getString("code") shouldBe code
    body
  }

  "ApiRoutes" should "pass a success through unchanged" in {
    reply = HttpResponse(200, """{"id":"abc"}""", headers = Map("Location" -> "/here"))

    val response = await(request(HttpMethod.GET, s"/customers/$customerId").send())

    response.statusCode() shouldBe 200
    response.getHeader(CONTENT_TYPE) shouldBe DEFAULT_CONTENT_TYPE
    response.getHeader("Location") shouldBe "/here"
    response.bodyAsString() shouldBe """{"id":"abc"}"""
  }

  it should "return a domain error as a Problem with its headers" in {
    reply = Problem.fromDomainError(DomainError.QuotaExceeded("CALLS", Some(60)))

    val response = post(s"/customers/$customerId/entitlements/consume", """{"feature_key":"CALLS","amount":1}""")

    problem(response, 429, "QUOTA_EXCEEDED")
    response.getHeader(RETRY_AFTER_HEADER) shouldBe "60"
  }

  it should "add the trace id to a domain error" in {
    reply = Problem.fromDomainError(DomainError.CustomerNotFound(UUID.fromString(customerId)))
    val traceId = "4bf92f3577b34da6a3ce929d0e0e4736"

    val response = await(request(HttpMethod.GET, s"/customers/$customerId").putHeader(TRACEPARENT_FIELD, s"00-$traceId-00f067aa0ba902b7-01").send())

    problem(response, 404, "CUSTOMER_NOT_FOUND").getString("trace_id") shouldBe traceId
  }

  it should "report an unknown route as a Problem" in {
    problem(await(request(HttpMethod.GET, "/unknown").send()), 404, "NOT_FOUND")
  }

  it should "report an unsupported method as a Problem" in {
    problem(await(request(HttpMethod.DELETE, "/plans").send()), 405, "HTTP_ERROR")
  }

  it should "report an invalid path parameter as a validation failure" in {
    problem(await(request(HttpMethod.GET, "/customers/not-a-uuid").send()), 400, "VALIDATION_FAILED")
  }

  it should "reject a NUL character in a company name" in {
    problem(post("/customers", """{"email":"a@b.com","company_name":"a\u0000b"}"""), 400, "VALIDATION_FAILED")
  }

  it should "reject a NUL character in a feature key" in {
    problem(post(s"/customers/$customerId/entitlements/consume", """{"feature_key":"a\u0000b","amount":1}"""), 400, "VALIDATION_FAILED")
  }

  it should "accept a company name without a NUL character" in {
    reply = HttpResponse(201, "{}")

    post("/customers", """{"email":"a@b.com","company_name":"Acme & Sons\nLtd"}""").statusCode() shouldBe 201
  }

  it should "reject an amount that does not fit a bigint" in {
    problem(post(s"/customers/$customerId/entitlements/consume", """{"feature_key":"CALLS","amount":1000000000000000000000}"""), 400, "VALIDATION_FAILED")
  }
}
