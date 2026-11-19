package me.bataillec.portfolio.customer.unit.audit

import io.vertx.core.Future
import me.bataillec.portfolio.customer.audit.{AuditLogger, AuditOperation, AuditOps}
import me.bataillec.portfolio.customer.support.FutureUtils.await
import org.mockito.Mockito.*
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class AuditOperationSpec extends AnyWordSpec with Matchers {
  "audit" should {
    "delegate to the logger and return its future" in {
      val logger  = mock(classOf[AuditLogger])
      val written = Future.succeededFuture[Void]()
      when(logger.audit("event")).thenReturn(written)

      new AuditOperation(logger).audit("event") shouldBe theSameInstanceAs(written)

      verify(logger).audit("event")
    }
  }

  "close" should {
    "delegate to the logger and return its future" in {
      val logger = mock(classOf[AuditLogger])
      val closed = Future.succeededFuture[Void]()
      when(logger.close()).thenReturn(closed)

      new AuditOperation(logger).close() shouldBe theSameInstanceAs(closed)

      verify(logger).close()
    }
  }

  "health" should {
    "delegate to the logger and return its future" in {
      val logger  = mock(classOf[AuditLogger])
      val checked = Future.succeededFuture[Unit](())
      when(logger.health()).thenReturn(checked)

      new AuditOperation(logger).health() shouldBe theSameInstanceAs(checked)

      verify(logger).health()
    }
  }

  "the default health" should {
    "succeed for an AuditLogger that does not override it" in {
      val logger = new AuditLogger {
        override def close(): Future[Void]          = Future.succeededFuture[Void]()
        override def audit(msg: String): Future[Void] = Future.succeededFuture[Void]()
      }

      await(logger.health()) shouldBe (())
    }

    "succeed for an AuditOps that does not override it" in {
      val ops = new AuditOps {
        override def audit(msg: String): Future[Void] = Future.succeededFuture[Void]()
        override def close(): Future[Void]            = Future.succeededFuture[Void]()
      }

      await(ops.health()) shouldBe (())
    }
  }
}
