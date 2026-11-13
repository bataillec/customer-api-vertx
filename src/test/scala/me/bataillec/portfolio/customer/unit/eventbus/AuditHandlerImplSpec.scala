package me.bataillec.portfolio.customer.unit.eventbus

import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.classic.{Level, Logger as LogbackLogger}
import ch.qos.logback.core.read.ListAppender
import io.vertx.core.eventbus.Message
import io.vertx.core.{Future, Handler, Promise}
import me.bataillec.portfolio.customer.Constants.{AUDIT_ADDRESS, AUDIT_HEALTH_ADDRESS}
import me.bataillec.portfolio.customer.audit.AuditOps
import me.bataillec.portfolio.customer.eventbus.AuditHandlerImpl
import me.bataillec.portfolio.customer.support.{EventBusFixture, TestMessages}
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.*
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks.*
import org.scalatest.wordspec.AnyWordSpec
import org.slf4j.LoggerFactory

import java.io.UncheckedIOException
import scala.jdk.CollectionConverters.*

class AuditHandlerImplSpec extends AnyWordSpec with Matchers {

  // region Helpers

  private class Fixture(bus: EventBusFixture = new EventBusFixture()) {
    val ops: AuditOps             = mock(classOf[AuditOps])
    when(ops.audit(any[String]())).thenReturn(Future.succeededFuture[Void]())
    when(ops.close()).thenReturn(Future.succeededFuture[Void]())
    val handler                   = new AuditHandlerImpl(bus.vertx, ops)
    val eventBus: EventBusFixture = bus

    /** Runs `handle()` and returns the handler registered on the audit address. */
    def started(): Handler[Message[String]] = {
      handler.handle().succeeded() shouldBe true
      bus.registeredHandler(AUDIT_ADDRESS)
    }
  }

  // endregion

  "handle" should {
    "register the health consumer, then the audit consumer" in {
      val f = new Fixture()

      f.handler.handle()

      f.eventBus.registeredAddresses shouldBe List(AUDIT_HEALTH_ADDRESS, AUDIT_ADDRESS)
    }

    "succeed when both registrations succeed" in {
      val f = new Fixture()

      f.handler.handle().succeeded() shouldBe true
    }

    "reply 'pong' on the health address" in {
      val f   = new Fixture()
      f.handler.handle()
      val msg = TestMessages.message("ping")

      f.eventBus.registeredHandler(AUDIT_HEALTH_ADDRESS).handle(msg)

      verify(msg).reply("pong")
    }

    "fail, and not register the audit consumer, when the health registration fails" in {
      val cause = new RuntimeException("health registration failed")
      val bus   = new EventBusFixture()
      bus.completeWith(AUDIT_HEALTH_ADDRESS, Future.failedFuture[Void](cause))
      val f     = new Fixture(bus)

      val result = f.handler.handle()

      result.failed() shouldBe true
      result.cause() shouldBe cause
      bus.registeredAddresses shouldBe List(AUDIT_HEALTH_ADDRESS)
    }

    "fail when the audit registration fails" in {
      val cause = new RuntimeException("audit registration failed")
      val bus   = new EventBusFixture()
      bus.completeWith(AUDIT_ADDRESS, Future.failedFuture[Void](cause))
      val f     = new Fixture(bus)

      val result = f.handler.handle()

      result.failed() shouldBe true
      result.cause() shouldBe cause
    }

    "stay pending until each registration completes" in {
      val health = Promise.promise[Void]()
      val audit  = Promise.promise[Void]()
      val bus    = new EventBusFixture()
      bus.completeWith(AUDIT_HEALTH_ADDRESS, health.future())
      bus.completeWith(AUDIT_ADDRESS, audit.future())
      val f      = new Fixture(bus)

      val result = f.handler.handle()
      result.isComplete shouldBe false
      bus.registeredAddresses shouldBe List(AUDIT_HEALTH_ADDRESS)

      health.complete()
      result.isComplete shouldBe false
      bus.registeredAddresses shouldBe List(AUDIT_HEALTH_ADDRESS, AUDIT_ADDRESS)

      audit.complete()
      result.succeeded() shouldBe true
    }
  }

  "the audit consumer" should {
    "audit the body of the received message" in {
      val f = new Fixture()
      val h = f.started()

      h.handle(TestMessages.message("""{"action":"CREATE"}"""))

      verify(f.ops).audit("""{"action":"CREATE"}""")
    }

    "audit each message, in order" in {
      val f = new Fixture()
      val h = f.started()

      h.handle(TestMessages.message("first"))
      h.handle(TestMessages.message("second"))

      val inOrder = org.mockito.Mockito.inOrder(f.ops)
      inOrder.verify(f.ops).audit("first")
      inOrder.verify(f.ops).audit("second")
    }

    "pass a null body through" in {
      val f = new Fixture()
      val h = f.started()

      h.handle(TestMessages.message(null))

      verify(f.ops).audit(null)
    }

    "swallow the exceptions thrown by the audit" when {
      val failures = Table(
        ("case", "failure"),
        ("runtime exception", new RuntimeException("disk full")),
        ("unchecked IO exception", new UncheckedIOException("cannot write", new java.io.IOException("io")))
      )

      forAll(failures) { (name: String, failure: Throwable) =>
        s"on $name" in {
          val f = new Fixture()
          val h = f.started()
          doAnswer(_ => throw failure).when(f.ops).audit(any[String]())

          noException should be thrownBy h.handle(TestMessages.message("event"))

          verify(f.ops).audit("event")
        }
      }
    }

    "keep consuming after a failure" in {
      val f = new Fixture()
      val h = f.started()
      doAnswer(_ => throw new RuntimeException("disk full")).when(f.ops).audit("bad")

      h.handle(TestMessages.message("bad"))
      h.handle(TestMessages.message("good"))

      verify(f.ops).audit("good")
    }

    "log a failed write (failed Future) as an error and keep consuming" in {
      val logger   = LoggerFactory.getLogger(classOf[AuditHandlerImpl]).asInstanceOf[LogbackLogger]
      val appender = new ListAppender[ILoggingEvent]()
      appender.start()
      logger.addAppender(appender)
      try {
        val f = new Fixture()
        val h = f.started()
        when(f.ops.audit("bad")).thenReturn(Future.failedFuture[Void](new RuntimeException("broker down")))

        noException should be thrownBy h.handle(TestMessages.message("bad"))
        h.handle(TestMessages.message("good"))

        val errors = appender.list.asScala.filter(_.getLevel == Level.ERROR)
        errors should have size 1
        errors.head.getFormattedMessage should include("bad")
        errors.head.getThrowableProxy.getMessage shouldBe "broker down"
        verify(f.ops).audit("good")
      } finally logger.detachAppender(appender)
    }

    "log the swallowed failure as an error" in {
      val logger   = LoggerFactory.getLogger(classOf[AuditHandlerImpl]).asInstanceOf[LogbackLogger]
      val appender = new ListAppender[ILoggingEvent]()
      appender.start()
      logger.addAppender(appender)
      try {
        val f = new Fixture()
        val h = f.started()
        doAnswer(_ => throw new RuntimeException("disk full")).when(f.ops).audit(any[String]())

        h.handle(TestMessages.message("event-body"))

        val errors = appender.list.asScala.filter(_.getLevel == Level.ERROR)
        errors should have size 1
        errors.head.getFormattedMessage should include("event-body")
        errors.head.getThrowableProxy.getMessage shouldBe "disk full"
      } finally logger.detachAppender(appender)
    }

    "let an InterruptedException escape (known gap: it is not NonFatal)" in {
      val f = new Fixture()
      val h = f.started()
      doAnswer(_ => throw new InterruptedException("interrupted")).when(f.ops).audit(any[String]())

      an[InterruptedException] should be thrownBy h.handle(TestMessages.message("event"))
    }
  }

  "close" should {
    "delegate to operations.close() once and return its future" in {
      val f      = new Fixture()
      val closed = Future.succeededFuture[Void]()
      when(f.ops.close()).thenReturn(closed)

      f.handler.close() shouldBe theSameInstanceAs(closed)

      verify(f.ops, times(1)).close()
    }

    "propagate the exception of operations.close()" in {
      val f = new Fixture()
      doThrow(new IllegalStateException("already closed")).when(f.ops).close()

      an[IllegalStateException] should be thrownBy f.handler.close()
    }
  }
}
