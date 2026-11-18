package me.bataillec.portfolio.customer.unit.eventbus

import io.vertx.core.eventbus.*
import io.vertx.core.http.HttpServerRequest
import io.vertx.core.json.{DecodeException, JsonObject}
import io.vertx.core.{Future, Promise}
import io.vertx.ext.web.{RequestBody, RoutingContext}
import me.bataillec.portfolio.customer.Constants.*
import me.bataillec.portfolio.customer.eventbus.{CustomerSender, CustomerSenderImpl}
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.{any, eq as eqTo}
import org.mockito.Mockito.*
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks.*
import org.scalatest.wordspec.AnyWordSpec

import scala.jdk.CollectionConverters.*

class CustomerSenderImplSpec extends AnyWordSpec with Matchers {
  private val customerId = "11111111-2222-3333-4444-555555555555"

  // region Helpers

  /** One row per `CustomerSender` method. `headerNames` is the exact set of headers the method must send. */
  private case class Endpoint(
      name: String,
      operationId: String,
      hasBody: Boolean,
      hasPathParam: Boolean,
      headerNames: Set[String],
      call: (CustomerSender, RoutingContext) => Future[String]
  ) {
    override def toString: String = name
  }

  private val endpoints = Seq(
    Endpoint("createCustomer", createCustomer_ID, hasBody = true, hasPathParam = false, Set(OPERATION_ID), _.createCustomer(_)),
    Endpoint("getCustomers", listCustomers_ID, hasBody = false, hasPathParam = false, Set(OPERATION_ID, LIMIT_QUERY_PARAM), _.getCustomers(_)),
    Endpoint("getPlans", listPlans_ID, hasBody = false, hasPathParam = false, Set(OPERATION_ID), _.getPlans(_)),
    Endpoint("getCustomer", getCustomer_ID, hasBody = false, hasPathParam = true, Set(OPERATION_ID, ID_PATH_PARAM), _.getCustomer(_)),
    Endpoint("updateCustomer", updateCustomer_ID, hasBody = true, hasPathParam = true, Set(OPERATION_ID, ID_PATH_PARAM), _.updateCustomer(_)),
    Endpoint("subscribe", subscribe_ID, hasBody = true, hasPathParam = true, Set(OPERATION_ID, ID_PATH_PARAM), _.subscribe(_)),
    Endpoint("getSubscriptions", listSubscriptions_ID, hasBody = false, hasPathParam = true, Set(OPERATION_ID, ID_PATH_PARAM), _.getSubscriptions(_)),
    Endpoint("getEntitlements", listEntitlements_ID, hasBody = false, hasPathParam = true, Set(OPERATION_ID, ID_PATH_PARAM), _.getEntitlements(_)),
    Endpoint("consumeEntitlement", consumeEntitlement_ID, hasBody = true, hasPathParam = true, Set(OPERATION_ID, ID_PATH_PARAM), _.consumeEntitlement(_))
  )

  private def table(rows: Seq[Endpoint]) = Table("endpoint", rows*)

  private def replyWithBody(body: String): Message[String] = {
    val reply = mock(classOf[Message[String]])
    when(reply.body()).thenReturn(body)
    reply
  }

  private class Fixture(busReply: Future[Message[String]] = Future.succeededFuture(replyWithBody("reply"))) {
    val eb: EventBus               = mock(classOf[EventBus])
    val rc: RoutingContext         = mock(classOf[RoutingContext])
    val requestBody: RequestBody   = mock(classOf[RequestBody])
    val request: HttpServerRequest = mock(classOf[HttpServerRequest])
    val sender                     = new CustomerSenderImpl(eb)

    private val payloadCaptor = ArgumentCaptor.forClass(classOf[AnyRef])
    private val optionsCaptor = ArgumentCaptor.forClass(classOf[DeliveryOptions])

    when(eb.request[String](any[String](), any[AnyRef](), any[DeliveryOptions]())).thenReturn(busReply)
    when(rc.body()).thenReturn(requestBody)
    when(rc.request()).thenReturn(request)
    when(rc.pathParam(ID_PATH_PARAM)).thenReturn(customerId)
    when(requestBody.asJsonObject()).thenReturn(JsonObject().put("b", 2).put("a", 1))

    def withBody(json: JsonObject): this.type = {
      when(requestBody.asJsonObject()).thenReturn(json)
      this
    }

    def withoutPathParam(): this.type = {
      when(rc.pathParam(ID_PATH_PARAM)).thenReturn(null)
      this
    }

    def withQuery(limit: Option[String], cursor: Option[String], status: Option[String]): this.type = {
      when(request.getParam(LIMIT_QUERY_PARAM)).thenReturn(limit.orNull)
      when(request.getParam(CURSOR_QUERY_PARAM)).thenReturn(cursor.orNull)
      when(request.getParam(STATUS_QUERY_PARAM)).thenReturn(status.orNull)
      this
    }

    private def captured(): Unit =
      verify(eb).request[String](eqTo(CUSTOMER_ADDRESS), payloadCaptor.capture(), optionsCaptor.capture())

    def payload: AnyRef = { captured(); payloadCaptor.getValue }

    def options: DeliveryOptions = { captured(); optionsCaptor.getValue }

    def headers: Map[String, String] =
      options.getHeaders.names().asScala.map(n => n -> options.getHeaders.get(n)).toMap
  }

  // endregion

  "every method" should {
    forAll(table(endpoints)) { (e: Endpoint) =>
      s"${e.name}: send on the customer address with the right headers, payload and timeout, and return the reply as is" in {
        val f = new Fixture(Future.succeededFuture(replyWithBody(s"reply of ${e.name}")))

        val result = e.call(f.sender, f.rc)

        result.succeeded() shouldBe true
        result.result() shouldBe s"reply of ${e.name}"
        f.options.getHeaders.names().asScala.toSet shouldBe e.headerNames
        f.options.getHeaders.get(OPERATION_ID) shouldBe e.operationId
        f.options.getSendTimeout shouldBe 30000
        if e.hasPathParam then f.options.getHeaders.get(ID_PATH_PARAM) shouldBe customerId
        if e.hasBody then f.payload shouldBe """{"b":2,"a":1}""" else f.payload shouldBe null
      }
    }

    "bus failures" when {
      val failures = Table(
        ("case", "failure"),
        ("timeout", new ReplyException(ReplyFailure.TIMEOUT, "timeout")),
        ("no handler", new ReplyException(ReplyFailure.NO_HANDLERS, "no handlers")),
        ("recipient failure", new ReplyException(ReplyFailure.RECIPIENT_FAILURE, 500, "boom")),
        ("arbitrary throwable", new RuntimeException("boom"))
      )

      forAll(failures) { (name: String, failure: Throwable) =>
        s"return a failed Future on $name" in {
          val f = new Fixture(Future.failedFuture[Message[String]](failure))

          val result = f.sender.getPlans(f.rc)

          result.failed() shouldBe true
          result.cause() shouldBe failure
        }
      }
    }

    "stay pending until the bus replies" in {
      val reply = Promise.promise[Message[String]]()
      val f     = new Fixture(reply.future())

      val result = f.sender.getPlans(f.rc)
      result.isComplete shouldBe false

      reply.complete(replyWithBody("late"))
      result.result() shouldBe "late"
    }
  }

  "methods with a body" should {
    val withBody = endpoints.filter(_.hasBody)

    forAll(table(withBody)) { (e: Endpoint) =>
      s"${e.name}: send an empty body as '{}'" in {
        val f = new Fixture().withBody(null)

        e.call(f.sender, f.rc)

        f.payload shouldBe "{}"
      }

      s"${e.name}: return a failed Future and send nothing when the body is not valid JSON" in {
        val f = new Fixture()
        when(f.requestBody.asJsonObject()).thenThrow(new DecodeException("bad json"))

        val result = e.call(f.sender, f.rc)

        result.failed() shouldBe true
        result.cause().getMessage shouldBe "The given request body could not be parsed as JSON"
        verifyNoInteractions(f.eb)
      }
    }
  }

  "methods with a path param" should {
    forAll(table(endpoints.filter(_.hasPathParam))) { (e: Endpoint) =>
      s"${e.name}: return a failed Future and send nothing when the id is missing" in {
        val f = new Fixture().withoutPathParam()

        val result = e.call(f.sender, f.rc)

        result.failed() shouldBe true
        result.cause().getMessage shouldBe "The path param 'id' is not found in the path"
        verifyNoInteractions(f.eb)
      }
    }
  }

  "getCustomers" should {
    val cases = Table(
      ("case", "limit", "cursor", "status", "expected headers"),
      ("no query param: default limit", None, None, None, Map(LIMIT_QUERY_PARAM -> LIMIT_DEFAULT_VALUE)),
      ("limit only", Some("10"), None, None, Map(LIMIT_QUERY_PARAM -> "10")),
      ("cursor only: default limit", None, Some("abc"), None, Map(LIMIT_QUERY_PARAM -> LIMIT_DEFAULT_VALUE, CURSOR_QUERY_PARAM -> "abc")),
      ("status only: default limit", None, None, Some("ACTIVE"), Map(LIMIT_QUERY_PARAM -> LIMIT_DEFAULT_VALUE, STATUS_QUERY_PARAM -> "ACTIVE")),
      (
        "all query params",
        Some("25"),
        Some("abc"),
        Some("SUSPENDED"),
        Map(LIMIT_QUERY_PARAM -> "25", CURSOR_QUERY_PARAM -> "abc", STATUS_QUERY_PARAM -> "SUSPENDED")
      )
    )

    forAll(cases) { (name: String, limit: Option[String], cursor: Option[String], status: Option[String], expected: Map[String, String]) =>
      s"forward the query params: $name" in {
        val f = new Fixture().withQuery(limit, cursor, status)

        f.sender.getCustomers(f.rc)

        f.headers shouldBe expected + (OPERATION_ID -> listCustomers_ID)
      }
    }

    "forward a non-numeric limit without validating it" in {
      val f = new Fixture().withQuery(Some("abc"), None, None)

      f.sender.getCustomers(f.rc)

      f.headers(LIMIT_QUERY_PARAM) shouldBe "abc"
    }
  }
}
