package me.bataillec.portfolio.customer.unit.eventbus

import io.vertx.core.eventbus.Message
import io.vertx.core.json.{JsonArray, JsonObject}
import io.vertx.core.{Future, Handler, Promise}
import me.bataillec.portfolio.customer.Constants.*
import me.bataillec.portfolio.customer.customer.CustomerOps
import me.bataillec.portfolio.customer.domain.*
import me.bataillec.portfolio.customer.eventbus.CustomerHandlerImpl
import me.bataillec.portfolio.customer.http.HttpResponse
import me.bataillec.portfolio.customer.support.{EventBusFixture, TestMessages}
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.{any, anyInt, isNull, eq as eqTo}
import org.mockito.Mockito.*
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks.*
import org.scalatest.wordspec.AnyWordSpec

import java.time.Instant
import java.util.UUID
import scala.collection.immutable.ListMap

class CustomerHandlerImplSpec extends AnyWordSpec with Matchers {
  private val uuid    = UUID.fromString("11111111-2222-3333-4444-555555555555")
  private val subId   = UUID.fromString("66666666-7777-8888-9999-000000000000")
  private val instant = Instant.parse("2026-01-02T03:04:05Z")

  private val customer     = Customer(uuid, "a@b.c", Some("ACME"), CustomerStatus.ACTIVE, instant, instant)
  private val subscription = Subscription(subId, uuid, PlanType.PRO, SubscriptionStatus.ACTIVE, instant, None)
  private val entitlement  = Entitlement("api_calls", 100, 10, None)

  // region Helpers

  private class Fixture(val bus: EventBusFixture = new EventBusFixture()) {
    val ops: CustomerOps = mock(classOf[CustomerOps])
    val handler          = new CustomerHandlerImpl(bus.vertx, ops)

    lazy val consumer: Handler[Message[String]] = {
      handler.handle().succeeded() shouldBe true
      bus.registeredHandler(CUSTOMER_ADDRESS)
    }

    /** Delivers a message to the customer consumer. */
    def deliver(headers: Seq[(String, String)], body: String = null): Message[String] = {
      val msg = TestMessages.message(body, headers*)
      consumer.handle(msg)
      msg
    }
  }

  private def replyOf(msg: Message[String]): HttpResponse = {
    val captor = ArgumentCaptor.forClass(classOf[AnyRef])
    verify(msg).reply(captor.capture())
    HttpResponse.fromJsonString(captor.getValue.asInstanceOf[String])
  }

  /** Verifies that the message was failed with 500 and returns the failure message. */
  private def failedWith500(msg: Message[String]): String = {
    val captor = ArgumentCaptor.forClass(classOf[String])
    verify(msg).fail(eqTo(500), captor.capture())
    verify(msg, never()).reply(any[AnyRef]())
    captor.getValue
  }

  private def op(id: String, extra: (String, String)*): Seq[(String, String)] = (OPERATION_ID -> id) +: extra

  private def listHeaders(limit: String, cursor: Option[String] = None, status: Option[String] = None): Seq[(String, String)] =
    op(listCustomers_ID, Seq(Option(limit).map(LIMIT_QUERY_PARAM -> _), cursor.map(CURSOR_QUERY_PARAM -> _), status.map(STATUS_QUERY_PARAM -> _)).flatten*)

  private def stubListCustomers(ops: CustomerOps): Unit =
    when(ops.listCustomers(anyInt(), any[Option[String]](), any[Option[CustomerStatus]]())).thenReturn(Future.succeededFuture(ListCustomers(Nil, None)))

  /** One row per operation ID: the request, the expected call on `CustomerOps` and the expected reply. */
  private case class Operation(
      name: String,
      headers: Seq[(String, String)],
      body: String,
      stub: CustomerOps => Unit,
      expectCall: CustomerOps => Unit,
      status: Int,
      replyHeaders: Map[String, String],
      checkBody: String => Unit
  ) {
    val hasId: Boolean    = headers.exists(_._1 == ID_PATH_PARAM)
    val hasBody: Boolean  = body != null
    override def toString = name

    def withId(id: String): Seq[(String, String)] = headers.map { case (ID_PATH_PARAM, _) => ID_PATH_PARAM -> id; case h => h }
  }

  private val operations = Seq(
    Operation(
      "createCustomer",
      op(createCustomer_ID),
      """{"email":"a@b.c","company_name":"ACME"}""",
      o => when(o.createCustomer(CreateCustomer("a@b.c", Some("ACME")))).thenReturn(Future.succeededFuture(customer)),
      o => verify(o).createCustomer(CreateCustomer("a@b.c", Some("ACME"))),
      201,
      Map("Location" -> s"/customers/$uuid"),
      b => JsonObject(b).getString("email") shouldBe "a@b.c"
    ),
    Operation(
      "listCustomers",
      listHeaders("10", Some("c1"), Some("ACTIVE")),
      null,
      o => when(o.listCustomers(10, Some("c1"), Some(CustomerStatus.ACTIVE))).thenReturn(Future.succeededFuture(ListCustomers(List(customer), Some("next")))),
      o => verify(o).listCustomers(10, Some("c1"), Some(CustomerStatus.ACTIVE)),
      200,
      Map.empty,
      b => {
        JsonObject(b).getJsonArray("items").size() shouldBe 1
        JsonObject(b).getString("next_cursor") shouldBe "next"
      }
    ),
    Operation(
      "getCustomer",
      op(getCustomer_ID, ID_PATH_PARAM -> uuid.toString),
      null,
      o => when(o.getCustomer(uuid)).thenReturn(Future.succeededFuture(customer)),
      o => verify(o).getCustomer(uuid),
      200,
      Map.empty,
      b => JsonObject(b).getString("id") shouldBe uuid.toString
    ),
    Operation(
      "updateCustomer",
      op(updateCustomer_ID, ID_PATH_PARAM -> uuid.toString),
      """{"company_name":"New","status":"SUSPENDED"}""",
      o => when(o.updateCustomer(uuid, UpdateCustomer(Some("New"), Some(CustomerStatus.SUSPENDED)))).thenReturn(Future.succeededFuture(customer)),
      o => verify(o).updateCustomer(uuid, UpdateCustomer(Some("New"), Some(CustomerStatus.SUSPENDED))),
      200,
      Map.empty,
      b => JsonObject(b).getString("email") shouldBe "a@b.c"
    ),
    Operation(
      "subscribe",
      op(subscribe_ID, ID_PATH_PARAM -> uuid.toString),
      """{"plan_type":"PRO"}""",
      o =>
        when(o.createSubscription(uuid, CreateSubscription(PlanType.PRO))).thenReturn(
          Future.succeededFuture(SubscriptionWithEntitlements.fromSubscriptionAndEntitlements(subscription, List(entitlement)))
        ),
      o => verify(o).createSubscription(uuid, CreateSubscription(PlanType.PRO)),
      201,
      Map("Location"                 -> s"/customers/$uuid/subscriptions"),
      b => {
        JsonObject(b).getString("plan_type") shouldBe "PRO"
        JsonObject(b).getJsonArray("entitlements").size() shouldBe 1
      }
    ),
    Operation(
      "listPlans",
      op(listPlans_ID),
      null,
      o => when(o.listPlans()).thenReturn(Future.succeededFuture(List(Plan(PlanType.PRO, ListMap.empty)))),
      o => verify(o).listPlans(),
      200,
      Map.empty,
      b => JsonArray(b).getJsonObject(0).getString("plan_type") shouldBe "PRO"
    ),
    Operation(
      "listSubscriptions",
      op(listSubscriptions_ID, ID_PATH_PARAM -> uuid.toString),
      null,
      o => when(o.listSubscriptions(uuid)).thenReturn(Future.succeededFuture(List(subscription))),
      o => verify(o).listSubscriptions(uuid),
      200,
      Map.empty,
      b => JsonArray(b).getJsonObject(0).getString("id") shouldBe subId.toString
    ),
    Operation(
      "listEntitlements",
      op(listEntitlements_ID, ID_PATH_PARAM -> uuid.toString),
      null,
      o => when(o.listEntitlements(uuid)).thenReturn(Future.succeededFuture(List(entitlement))),
      o => verify(o).listEntitlements(uuid),
      200,
      Map.empty,
      b => JsonArray(b).getJsonObject(0).getString("feature_key") shouldBe "api_calls"
    ),
    Operation(
      "consumeEntitlement",
      op(consumeEntitlement_ID, ID_PATH_PARAM -> uuid.toString),
      """{"feature_key":"api_calls","amount":5}""",
      o => when(o.consumeEntitlement(uuid, Consumption("api_calls", 5))).thenReturn(Future.succeededFuture(ConsumeResult("api_calls", 100, 10, 90))),
      o => verify(o).consumeEntitlement(uuid, Consumption("api_calls", 5)),
      200,
      Map.empty,
      b => JsonObject(b).getLong("remaining_quota") shouldBe 90L
    )
  )

  private def table(rows: Seq[Operation]) = Table("operation", rows*)

  // endregion

  "handle" should {
    "register the health consumer, then the customer consumer" in {
      val f = new Fixture()

      f.handler.handle()

      f.bus.registeredAddresses shouldBe List(CUSTOMER_HEALTH_ADDRESS, CUSTOMER_ADDRESS)
    }

    "succeed when both registrations succeed" in {
      val f = new Fixture()

      f.handler.handle().succeeded() shouldBe true
    }

    "reply 'pong' to a ping when the database is reachable" in {
      val f   = new Fixture()
      when(f.ops.ping()).thenReturn(Future.succeededFuture(()))
      f.handler.handle()
      val msg = TestMessages.message("ping")

      f.bus.registeredHandler(CUSTOMER_HEALTH_ADDRESS).handle(msg)

      verify(f.ops).ping()
      verify(msg).reply("pong")
      verify(msg, never()).fail(anyInt(), any[String]())
    }

    "fail the ping with 503 when the database is unreachable" in {
      val f   = new Fixture()
      when(f.ops.ping()).thenReturn(Future.failedFuture[Unit](new RuntimeException("db down")))
      f.handler.handle()
      val msg = TestMessages.message("ping")

      f.bus.registeredHandler(CUSTOMER_HEALTH_ADDRESS).handle(msg)

      verify(msg).fail(503, "Postgres unreachable")
      verify(msg, never()).reply(any[AnyRef]())
    }

    "fail, and not register the customer consumer, when the health registration fails" in {
      val cause = new RuntimeException("health registration failed")
      val bus   = new EventBusFixture()
      bus.completeWith(CUSTOMER_HEALTH_ADDRESS, Future.failedFuture[Void](cause))
      val f     = new Fixture(bus)

      val result = f.handler.handle()

      result.failed() shouldBe true
      result.cause() shouldBe cause
      bus.registeredAddresses shouldBe List(CUSTOMER_HEALTH_ADDRESS)
    }

    "fail when the customer registration fails" in {
      val cause = new RuntimeException("customer registration failed")
      val bus   = new EventBusFixture()
      bus.completeWith(CUSTOMER_ADDRESS, Future.failedFuture[Void](cause))
      val f     = new Fixture(bus)

      val result = f.handler.handle()

      result.failed() shouldBe true
      result.cause() shouldBe cause
    }

    "stay pending until each registration completes" in {
      val health      = Promise.promise[Void]()
      val customerReg = Promise.promise[Void]()
      val bus         = new EventBusFixture()
      bus.completeWith(CUSTOMER_HEALTH_ADDRESS, health.future())
      bus.completeWith(CUSTOMER_ADDRESS, customerReg.future())
      val f           = new Fixture(bus)

      val result = f.handler.handle()
      result.isComplete shouldBe false
      bus.registeredAddresses shouldBe List(CUSTOMER_HEALTH_ADDRESS)

      health.complete()
      result.isComplete shouldBe false
      bus.registeredAddresses shouldBe List(CUSTOMER_HEALTH_ADDRESS, CUSTOMER_ADDRESS)

      customerReg.complete()
      result.succeeded() shouldBe true
    }
  }

  "the customer consumer" should {
    "dispatch each operation" when {
      forAll(table(operations)) { (o: Operation) =>
        s"${o.name}: decode the request, call the operation and reply the HTTP response" in {
          val f = new Fixture()
          o.stub(f.ops)

          val msg = f.deliver(o.headers, o.body)

          o.expectCall(f.ops)
          verifyNoMoreInteractions(f.ops)
          val reply = replyOf(msg)
          reply.statusCode shouldBe o.status
          reply.contentType shouldBe DEFAULT_CONTENT_TYPE
          reply.headers shouldBe o.replyHeaders
          o.checkBody(reply.body)
          verify(msg, never()).fail(anyInt(), any[String]())
        }
      }

      "updateCustomer: accept an empty JSON object" in {
        val f = new Fixture()
        when(f.ops.updateCustomer(uuid, UpdateCustomer(None, None))).thenReturn(Future.succeededFuture(customer))

        val msg = f.deliver(op(updateCustomer_ID, ID_PATH_PARAM -> uuid.toString), "{}")

        verify(f.ops).updateCustomer(uuid, UpdateCustomer(None, None))
        replyOf(msg).statusCode shouldBe 200
      }

      "createCustomer: forward a body without email (known gap: no validation here, the OpenAPI contract does it)" in {
        val f = new Fixture()
        when(f.ops.createCustomer(any[CreateCustomer]())).thenReturn(Future.succeededFuture(customer))

        f.deliver(op(createCustomer_ID), """{"company_name":"ACME"}""")

        verify(f.ops).createCustomer(CreateCustomer(null, Some("ACME")))
      }
    }

    "listCustomers" when {
      val limits = Table(("limit", "expected"), ("10", 10), ("-5", -5))

      forAll(limits) { (limit: String, expected: Int) =>
        s"parse the limit '$limit' to an Int" in {
          val f = new Fixture()
          stubListCustomers(f.ops)

          f.deliver(listHeaders(limit))

          verify(f.ops).listCustomers(expected, None, None)
        }
      }

      val optionals = Table(
        ("cursor", "status", "expectedStatus"),
        (None, None, None),
        (Some("c1"), None, None),
        (None, Some("DELETED"), Some(CustomerStatus.DELETED)),
        (Some("c1"), Some("ACTIVE"), Some(CustomerStatus.ACTIVE))
      )

      forAll(optionals) { (cursor: Option[String], status: Option[String], expectedStatus: Option[CustomerStatus]) =>
        s"treat the cursor ($cursor) and the status ($status) as optional" in {
          val f = new Fixture()
          stubListCustomers(f.ops)

          f.deliver(listHeaders("50", cursor, status))

          verify(f.ops).listCustomers(50, cursor, expectedStatus)
        }
      }

      val invalidLimits = Table("limit", "abc", "", "2147483648")

      forAll(invalidLimits) { (limit: String) =>
        s"fail with 500 on the non-numeric limit '$limit'" in {
          val f = new Fixture()

          val msg = f.deliver(listHeaders(limit))

          failedWith500(msg) should not be empty
          verifyNoInteractions(f.ops)
        }
      }

      "fail with 500 when the limit is missing" in {
        val f = new Fixture()

        val msg = f.deliver(listHeaders(null))

        failedWith500(msg) should not be empty
        verifyNoInteractions(f.ops)
      }

      "fail with 500 on an unknown status" in {
        val f = new Fixture()

        val msg = f.deliver(listHeaders("10", status = Some("BOGUS")))

        failedWith500(msg) should not be empty
        verifyNoInteractions(f.ops)
      }
    }

    "reject an invalid request with 500 and never call the operations" when {
      forAll(table(operations.filter(_.hasId))) { (o: Operation) =>
        s"${o.name}: the id is not a UUID" in {
          val f = new Fixture()

          val msg = f.deliver(o.withId("not-a-uuid"), o.body)

          failedWith500(msg) should not be empty
          verifyNoInteractions(f.ops)
        }
      }

      "getCustomer: the id is missing" in {
        val f = new Fixture()

        val msg = f.deliver(op(getCustomer_ID))

        failedWith500(msg) should not be empty
        verifyNoInteractions(f.ops)
      }

      forAll(table(operations.filter(_.hasBody))) { (o: Operation) =>
        s"${o.name}: the body is not valid JSON" in {
          val f = new Fixture()

          val msg = f.deliver(o.headers, "not json")

          failedWith500(msg) should not be empty
          verifyNoInteractions(f.ops)
        }
      }

      "createCustomer: the body is null" in {
        val f = new Fixture()

        val msg = f.deliver(op(createCustomer_ID), null)

        failedWith500(msg) should not be empty
        verifyNoInteractions(f.ops)
      }

      val unknown = Table("operation id", "doesNotExist", "live")

      forAll(unknown) { (id: String) =>
        s"the operation id '$id' is not handled" in {
          val f = new Fixture()

          val msg = f.deliver(op(id))

          failedWith500(msg) should include(s"Unknown OperationId $id")
          verifyNoInteractions(f.ops)
        }
      }

      "the operation id is missing" in {
        val f = new Fixture()

        val msg = f.deliver(Seq.empty)

        failedWith500(msg) should include("No OperationId found")
        verifyNoInteractions(f.ops)
      }
    }

    "handle the outcome of the operation" when {
      val headers = op(getCustomer_ID, ID_PATH_PARAM -> uuid.toString)

      val domainErrors = Table(
        ("error", "status", "retryAfter"),
        (DomainError.CustomerNotFound(uuid), 404, None),
        (DomainError.QuotaExceeded("api_calls", Some(30L)), 429, Some("30")),
        (DomainError.PlanNotConfigured(PlanType.PRO), 500, None)
      )

      forAll(domainErrors) { (error: DomainError, status: Int, retryAfter: Option[String]) =>
        s"reply a Problem (not a failure) on ${error.getClass.getSimpleName.stripSuffix("$")}" in {
          val f = new Fixture()
          when(f.ops.getCustomer(uuid)).thenReturn(Future.failedFuture[Customer](error))

          val msg = f.deliver(headers)

          val reply = replyOf(msg)
          reply.statusCode shouldBe status
          reply.contentType shouldBe PROBLEM_CONTENT_TYPE
          JsonObject(reply.body).getString(DETAIL_FIELD) shouldBe error.getMessage
          JsonObject(reply.body).getString(CODE_FIELD) shouldBe error.code
          reply.headers.get(RETRY_AFTER_HEADER) shouldBe retryAfter
          verify(msg, never()).fail(anyInt(), any[String]())
        }
      }

      "fail with 500 and the message on another failure" in {
        val f = new Fixture()
        when(f.ops.getCustomer(uuid)).thenReturn(Future.failedFuture[Customer](new RuntimeException("boom")))

        val msg = f.deliver(headers)

        failedWith500(msg) shouldBe "boom"
      }

      "fail with 500 and a null message when the failure has no message" in {
        val f = new Fixture()
        when(f.ops.getCustomer(uuid)).thenReturn(Future.failedFuture[Customer](new RuntimeException()))

        val msg = f.deliver(headers)

        verify(msg).fail(eqTo(500), isNull[String]())
        verify(msg, never()).reply(any[AnyRef]())
      }

      "fail with 500 and the exception text when the operation throws instead of failing the Future" in {
        val f = new Fixture()
        when(f.ops.getCustomer(uuid)).thenThrow(new IllegalStateException("db"))

        val msg = f.deliver(headers)

        failedWith500(msg) should include("IllegalStateException: db")
      }

      "fail with 500 when the reply itself throws" in {
        val f   = new Fixture()
        when(f.ops.getCustomer(uuid)).thenReturn(Future.succeededFuture(customer))
        val msg = TestMessages.message(null, headers*)
        doThrow(new IllegalStateException("closed")).when(msg).reply(any[AnyRef]())

        f.consumer.handle(msg)

        verify(msg).fail(500, "closed")
      }

      "neither reply nor fail while the Future is pending" in {
        val f       = new Fixture()
        val pending = Promise.promise[Customer]()
        when(f.ops.getCustomer(uuid)).thenReturn(pending.future())

        val msg = f.deliver(headers)

        verify(msg, never()).reply(any[AnyRef]())
        verify(msg, never()).fail(anyInt(), any[String]())

        pending.complete(customer)
        replyOf(msg).statusCode shouldBe 200
      }
    }
  }
}
