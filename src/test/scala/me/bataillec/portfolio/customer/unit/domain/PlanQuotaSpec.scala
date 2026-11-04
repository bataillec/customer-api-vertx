package me.bataillec.portfolio.customer.unit.domain

import io.vertx.sqlclient.Row
import me.bataillec.portfolio.customer.domain.PlanQuota
import org.mockito.Mockito.*
import org.scalatest.Inside.inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.time.{Clock, Instant}

class PlanQuotaSpec extends AnyWordSpec with Matchers {
  "PlanQuota" should {
    val planQuota = PlanQuota(1L, true)

    "be built" in {
      inside(planQuota) { case PlanQuota(quota, resetsMonthly) =>
        quota shouldBe 1L
        resetsMonthly shouldBe true
      }
    }

    "nextDate" in {
      val fixedInstantString = "2026-11-03T10:00:00Z"
      val fixedInstant       = Instant.parse(fixedInstantString)
      val clock              = mock(classOf[Clock])
      when(clock.instant()).thenReturn(fixedInstant)

      val nextResetDate = PlanQuota.nextResetDate(Instant.now(clock))

      nextResetDate.toString shouldBe "2026-12-01T00:00:00Z"
    }

    "be built from a Row" in {
      val row = mock(classOf[Row])

      when(row.getLong("quota")).thenReturn(1L)
      when(row.getBoolean("resets_monthly")).thenReturn(true)

      val pq = PlanQuota.fromRow(row)

      pq shouldBe planQuota
    }
  }
}
