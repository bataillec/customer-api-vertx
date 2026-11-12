package me.bataillec.portfolio.customer.unit.eventbus

import io.vertx.core.eventbus.*
import io.vertx.core.{Future, Handler, Promise, Vertx}
import io.vertx.ext.healthchecks.Status
import me.bataillec.portfolio.customer.Constants.HEALTH_PING_TIMEOUT_MS
import me.bataillec.portfolio.customer.eventbus.SenderUtils
import me.bataillec.portfolio.customer.support.EventBusFixture
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.{any, eq as eqTo}
import org.mockito.Mockito.*
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks.*
import org.scalatest.wordspec.AnyWordSpec

import java.util.concurrent.TimeUnit

class SenderUtilsSpec extends AnyWordSpec with Matchers with BeforeAndAfterAll {
  private val address   = "my.address"
  private val realVertx = Vertx.vertx()

  override protected def afterAll(): Unit = {
    realVertx.close().toCompletionStage.toCompletableFuture.get(5, TimeUnit.SECONDS)
    ()
  }

  // region Helpers

  private def pingFixture(reply: Future[Message[String]]): (Vertx, EventBus) = {
    val vertx = mock(classOf[Vertx])
    val eb    = mock(classOf[EventBus])
    when(vertx.eventBus()).thenReturn(eb)
    when(eb.request[String](any[String](), any[AnyRef](), any[DeliveryOptions]())).thenReturn(reply)
    (vertx, eb)
  }

  private def succeeded: Future[Message[String]] =
    Future.succeededFuture(mock(classOf[Message[String]]))

  private def failed(t: Throwable): Future[Message[String]] =
    Future.failedFuture[Message[String]](t)

  // endregion

  "ping" should {
    "complete OK when the peer answers" in {
      val (vertx, _) = pingFixture(succeeded)
      val promise    = Promise.promise[Status]()

      SenderUtils.ping(vertx, address, promise)

      promise.future().succeeded() shouldBe true
      promise.future().result().isOk shouldBe true
    }

    "send the 'ping' message to the given address" in {
      val (vertx, eb) = pingFixture(succeeded)

      SenderUtils.ping(vertx, address, Promise.promise[Status]())

      verify(eb).request[String](eqTo(address), eqTo("ping"), any[DeliveryOptions]())
    }

    "use the health ping timeout" in {
      val (vertx, eb) = pingFixture(succeeded)
      val captor      = ArgumentCaptor.forClass(classOf[DeliveryOptions])

      SenderUtils.ping(vertx, address, Promise.promise[Status]())

      verify(eb).request[String](eqTo(address), any[AnyRef](), captor.capture())
      captor.getValue.getSendTimeout shouldBe HEALTH_PING_TIMEOUT_MS
    }

    "failures" when {
      val failures = Table(
        ("case", "failure", "expectedError"),
        ("timeout", new ReplyException(ReplyFailure.TIMEOUT, "Timed out waiting for reply"), "Timed out waiting for reply"),
        ("no handler", new ReplyException(ReplyFailure.NO_HANDLERS, "No handlers for address my.address"), "No handlers for address my.address"),
        ("recipient failure", new ReplyException(ReplyFailure.RECIPIENT_FAILURE, 503, "Postgres unreachable"), "Postgres unreachable"),
        ("arbitrary throwable", new RuntimeException("boom"), "boom")
      )

      forAll(failures) { (name: String, failure: Throwable, expectedError: String) =>
        s"complete KO (not failed) on $name" in {
          val (vertx, _) = pingFixture(failed(failure))
          val promise    = Promise.promise[Status]()

          SenderUtils.ping(vertx, address, promise)

          promise.future().succeeded() shouldBe true
          val status = promise.future().result()
          status.isOk shouldBe false
          status.getData.getString("error") shouldBe expectedError
        }
      }

      "complete KO when the failure has no message" in {
        val (vertx, _) = pingFixture(failed(new RuntimeException()))
        val promise    = Promise.promise[Status]()

        SenderUtils.ping(vertx, address, promise)

        promise.future().succeeded() shouldBe true
        promise.future().result().isOk shouldBe false
        promise.future().result().getData.containsKey("error") shouldBe true
      }
    }

    "stay pending until the request is resolved" in {
      val reply      = Promise.promise[Message[String]]()
      val (vertx, _) = pingFixture(reply.future())
      val promise    = Promise.promise[Status]()

      SenderUtils.ping(vertx, address, promise)
      promise.future().isComplete shouldBe false

      reply.complete(mock(classOf[Message[String]]))
      promise.future().isComplete shouldBe true
      promise.future().result().isOk shouldBe true
    }
  }

  "pong (with handler)" should {
    "register a consumer on the given address" in {
      val f = new EventBusFixture()

      SenderUtils.pong(f.vertx, address, _ => Future.succeededFuture(()))

      verify(f.eb).consumer[String](eqTo(address), any[Handler[Message[String]]]())
    }

    "return the consumer registration future" in {
      val f = new EventBusFixture()

      val result = SenderUtils.pong(f.vertx, address, _ => Future.succeededFuture(()))

      result.succeeded() shouldBe true
    }

    "propagate a registration failure" in {
      val cause = new RuntimeException("registration failed")
      val f     = new EventBusFixture(Future.failedFuture[Void](cause))

      val result = SenderUtils.pong(f.vertx, address, _ => Future.succeededFuture(()))

      result.failed() shouldBe true
      result.cause() shouldBe cause
    }

    "reply 'pong' when the handler succeeds" in {
      val f   = new EventBusFixture()
      val msg = mock(classOf[Message[String]])
      SenderUtils.pong(f.vertx, address, _ => Future.succeededFuture(()))

      f.registeredHandler(address).handle(msg)

      verify(msg).reply("pong")
      verify(msg, never()).fail(any[Int](), any[String]())
    }

    "fail with 503 when the handler fails" in {
      val f   = new EventBusFixture()
      val msg = mock(classOf[Message[String]])
      SenderUtils.pong(f.vertx, address, _ => Future.failedFuture[Unit](new RuntimeException("db down")))

      f.registeredHandler(address).handle(msg)

      verify(msg).fail(503, "Postgres unreachable")
      verify(msg, never()).reply(any[AnyRef]())
    }

    "pass the received message to the handler" in {
      val f        = new EventBusFixture()
      val msg      = mock(classOf[Message[String]])
      var received = Option.empty[Message[String]]
      SenderUtils.pong(
        f.vertx,
        address,
        m => {
          received = Some(m)
          Future.succeededFuture(())
        }
      )

      f.registeredHandler(address).handle(msg)

      received.get should be theSameInstanceAs msg
    }

    "always use the fixed error text, whatever the cause" in {
      val f   = new EventBusFixture()
      val msg = mock(classOf[Message[String]])
      SenderUtils.pong(f.vertx, address, _ => Future.failedFuture[Unit](new IllegalStateException("another cause")))

      f.registeredHandler(address).handle(msg)

      verify(msg).fail(503, "Postgres unreachable")
    }

    "let a synchronous handler exception escape (known gap)" in {
      val f   = new EventBusFixture()
      val msg = mock(classOf[Message[String]])
      SenderUtils.pong(f.vertx, address, _ => throw new IllegalStateException("sync failure"))

      an[IllegalStateException] should be thrownBy f.registeredHandler(address).handle(msg)

      verify(msg, never()).reply(any[AnyRef]())
      verify(msg, never()).fail(any[Int](), any[String]())
    }
  }

  "pong (plain)" should {
    "register and return the consumer created by the event bus" in {
      val f = new EventBusFixture()

      val result = SenderUtils.pong(f.vertx, address)

      result should be theSameInstanceAs f.consumer(address)
      verify(f.eb).consumer[String](eqTo(address), any[Handler[Message[String]]]())
    }

    "reply 'pong'" in {
      val f   = new EventBusFixture()
      val msg = mock(classOf[Message[String]])
      SenderUtils.pong(f.vertx, address)

      f.registeredHandler(address).handle(msg)

      verify(msg).reply("pong")
      verify(msg, never()).fail(any[Int](), any[String]())
    }

    val bodies = Table("body", "anything", "ping", "", null)

    forAll(bodies) { (body: String) =>
      s"reply 'pong' whatever the body is ('$body')" in {
        val f   = new EventBusFixture()
        val msg = mock(classOf[Message[String]])
        when(msg.body()).thenReturn(body)
        SenderUtils.pong(f.vertx, address)

        f.registeredHandler(address).handle(msg)

        verify(msg).reply("pong")
      }
    }
  }

  "ping / pong round trip (real Vert.x)" should {
    def pingResult(addr: String): Status = {
      val promise = Promise.promise[Status]()
      SenderUtils.ping(realVertx, addr, promise)
      promise.future().toCompletionStage.toCompletableFuture.get(10, TimeUnit.SECONDS)
    }

    "yield OK when a pong consumer is registered" in {
      val addr = "roundtrip.ok"
      SenderUtils.pong(realVertx, addr).completion().toCompletionStage.toCompletableFuture.get(5, TimeUnit.SECONDS)

      pingResult(addr).isOk shouldBe true
    }

    "yield KO when nobody listens on the address" in {
      val status = pingResult("roundtrip.nobody")

      status.isOk shouldBe false
      status.getData.getString("error") should include("No handlers")
    }
  }
}
