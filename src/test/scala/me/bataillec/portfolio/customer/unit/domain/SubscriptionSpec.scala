package me.bataillec.portfolio.customer.unit.domain

import io.vertx.sqlclient.Row
import me.bataillec.portfolio.customer.domain.PlanType.*
import me.bataillec.portfolio.customer.domain.Subscription
import me.bataillec.portfolio.customer.domain.SubscriptionStatus.*
import org.mockito.Mockito.*
import org.scalatest.Inside.inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.time.{Clock, Instant, OffsetDateTime}
import java.util.UUID

class SubscriptionSpec extends AnyWordSpec with Matchers {
  "Subscription" should {
    val fixedUUIDString1 = "ed7b2379-c08e-44d1-823c-90b5c4f028e8"
    val fixedUUID1       = UUID.fromString(fixedUUIDString1)

    val fixedUUIDString2 = "ed7b2379-c08e-44d1-823c-90b5c4f028e9"
    val fixedUUID2       = UUID.fromString(fixedUUIDString2)

    val fixedInstantString = "2026-11-03T10:00:00Z"
    val fixedInstant       = Instant.parse(fixedInstantString)
    val clock              = mock(classOf[Clock])
    when(clock.instant()).thenReturn(fixedInstant)

    val subscription = Subscription(
      id = fixedUUID1,
      customerId = fixedUUID2,
      planType = ENTERPRISE,
      status = ACTIVE,
      startDate = Instant.now(clock),
      endDate = None
    )

    "be built" in {
      inside(subscription) { case Subscription(id, customerId, planType, status, startDate, endDate) =>
        id.toString shouldBe fixedUUIDString1
        customerId.toString shouldBe fixedUUIDString2
        planType shouldBe ENTERPRISE
        status shouldBe ACTIVE
        startDate.toString shouldBe fixedInstantString
        endDate shouldBe empty
      }
    }

    "be built from a Row" in {
      val row = mock(classOf[Row])

      when(row.getUUID("id")).thenReturn(fixedUUID1)
      when(row.getUUID("customer_id")).thenReturn(fixedUUID2)
      when(row.getString("plan_type")).thenReturn("ENTERPRISE")
      when(row.getString("status")).thenReturn("ACTIVE")
      when(row.getOffsetDateTime("start_date")).thenReturn(OffsetDateTime.parse(fixedInstantString))

      Subscription.fromRow(row) shouldBe subscription
    }
  }
}
