package me.bataillec.portfolio.customer.unit.audit

import io.vertx.core.Future
import me.bataillec.portfolio.customer.audit.{AuditLogger, AuditOperation}
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
}
