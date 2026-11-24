package me.bataillec.portfolio.customer.unit.audit

import io.vertx.core.{Future, Vertx}
import me.bataillec.portfolio.customer.audit.*
import me.bataillec.portfolio.customer.support.FutureUtils.{await, failureOf}
import org.mockito.Mockito.*
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.time.{Clock, Duration, Instant, ZoneOffset}

class AuditOperationSpec extends AnyWordSpec with Matchers with BeforeAndAfterAll {
  private val vertx = Vertx.vertx()
  private val now   = Instant.parse("2026-11-24T12:00:00Z")
  private val clock = Clock.fixed(now, ZoneOffset.UTC)

  override protected def afterAll(): Unit = {
    await(vertx.close())
    super.afterAll()
  }

  private def operation(logger: AuditLogger, store: OutboxStore) =
    new AuditOperation(logger, new OutboxRelay(vertx, store, logger, 200, 100, Duration.ofDays(7)), clock)

  "health" should {
    "report the outbox backlog and how old it is" in {
      val logger = mock(classOf[AuditLogger])
      when(logger.health()).thenReturn(Future.succeededFuture(()))
      val store = mock(classOf[OutboxStore])
      when(store.backlog()).thenReturn(Future.succeededFuture(Backlog(3, Some(now.minusSeconds(42)))))

      val details = await(operation(logger, store).health())

      details.getLong("outbox_backlog") shouldBe 3L
      details.getLong("outbox_oldest_age_seconds") shouldBe 42L
    }

    "leave the age out when the outbox is empty" in {
      val logger = mock(classOf[AuditLogger])
      when(logger.health()).thenReturn(Future.succeededFuture(()))
      val store = mock(classOf[OutboxStore])
      when(store.backlog()).thenReturn(Future.succeededFuture(Backlog(0, None)))

      val details = await(operation(logger, store).health())

      details.getLong("outbox_backlog") shouldBe 0L
      details.containsKey("outbox_oldest_age_seconds") shouldBe false
    }

    "fail when Kafka is unreachable" in {
      val logger = mock(classOf[AuditLogger])
      val error  = new RuntimeException("no broker")
      when(logger.health()).thenReturn(Future.failedFuture[Unit](error))

      failureOf(operation(logger, mock(classOf[OutboxStore])).health()) shouldBe error
    }

    "stay up, with an unknown backlog, when only the database cannot be read" in {
      val logger = mock(classOf[AuditLogger])
      when(logger.health()).thenReturn(Future.succeededFuture(()))
      val store = mock(classOf[OutboxStore])
      when(store.backlog()).thenReturn(Future.failedFuture[Backlog](new RuntimeException("db down")))

      await(operation(logger, store).health()).getString("outbox_backlog") shouldBe "unknown"
    }
  }

  "close" should {
    "stop the relay, close the store, then the logger" in {
      val logger = mock(classOf[AuditLogger])
      when(logger.close()).thenReturn(Future.succeededFuture[Void]())
      val store = mock(classOf[OutboxStore])
      when(store.close()).thenReturn(Future.succeededFuture(()))

      await(operation(logger, store).close())

      val calls = org.mockito.Mockito.inOrder(store, logger)
      calls.verify(store).close()
      calls.verify(logger).close()
    }

    "close the logger even when the store fails to close" in {
      val logger = mock(classOf[AuditLogger])
      when(logger.close()).thenReturn(Future.succeededFuture[Void]())
      val store = mock(classOf[OutboxStore])
      when(store.close()).thenReturn(Future.failedFuture[Unit](new RuntimeException("boom")))

      await(operation(logger, store).close())

      verify(logger).close()
    }
  }

  "the default health" should {
    "succeed for an AuditLogger that does not override it" in {
      val logger = new AuditLogger {
        override def close(): Future[Void]            = Future.succeededFuture[Void]()
        override def audit(msg: String): Future[Void] = Future.succeededFuture[Void]()
      }

      await(logger.health()) shouldBe (())
    }
  }
}
