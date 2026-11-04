package me.bataillec.portfolio.customer.unit.domain

import me.bataillec.portfolio.customer.domain.PlanType.ENTERPRISE
import me.bataillec.portfolio.customer.domain.SubscriptionStatus.ACTIVE
import me.bataillec.portfolio.customer.domain.{Entitlement, Subscription, SubscriptionWithEntitlements}
import me.bataillec.portfolio.customer.http.HttpResponse
import org.mockito.Mockito.{mock, when}
import org.scalatest.Inside.inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.time.{Clock, Instant}
import java.util.UUID

class SubscriptionWithEntitlementsSpec extends AnyWordSpec with Matchers {
  "SubscriptionWithEntitlements" should {
    val fixedInstantString = "2026-11-03T10:00:00Z"
    val fixedInstant       = Instant.parse(fixedInstantString)
    val clock              = mock(classOf[Clock])
    when(clock.instant()).thenReturn(fixedInstant)

    val fixedUUIDString1 = "ed7b2379-c08e-44d1-823c-90b5c4f028e8"
    val fixedUUID1       = UUID.fromString(fixedUUIDString1)

    val fixedUUIDString2 = "ed7b2379-c08e-44d1-823c-90b5c4f028e9"
    val fixedUUID2       = UUID.fromString(fixedUUIDString2)

    val entitlement1 = Entitlement("featureKey1", 3L, 2L, None)
    val entitlement2 = Entitlement("featureKey2", 5L, 4L, None)

    val subscriptionWithEntitlements = SubscriptionWithEntitlements(
      id = fixedUUID1,
      customerId = fixedUUID2,
      planType = ENTERPRISE,
      status = ACTIVE,
      startDate = Instant.now(clock),
      endDate = None,
      entitlements = List(entitlement1, entitlement2)
    )

    "be built" in {
      inside(subscriptionWithEntitlements) { case SubscriptionWithEntitlements(id, customerId, planType, status, startDate, endDate, entitlements) =>
        id.toString shouldBe fixedUUIDString1
        customerId.toString shouldBe fixedUUIDString2
        planType shouldBe ENTERPRISE
        status shouldBe ACTIVE
        startDate.toString shouldBe fixedInstantString
        endDate shouldBe empty
        entitlements should have size 2
        entitlements should contain allOf (entitlement1, entitlement2)
      }
    }

    "be built froma subscription and a list of entitlements" in {
      val subscription = Subscription(
        id = fixedUUID1,
        customerId = fixedUUID2,
        planType = ENTERPRISE,
        status = ACTIVE,
        startDate = Instant.now(clock),
        endDate = None
      )

      val res = SubscriptionWithEntitlements.fromSubscriptionAndEntitlements(subscription, List(entitlement1, entitlement2))

      res shouldBe subscriptionWithEntitlements
    }

    "become a HTTPResponse" in {
      val response = subscriptionWithEntitlements.toHttpResponse()

      inside(response) { case HttpResponse(statusCode, body, contentType, headers) =>
        statusCode shouldBe 200
        contentType shouldBe "application/json"
        headers shouldBe Map.empty
        body shouldBe """{"id":"ed7b2379-c08e-44d1-823c-90b5c4f028e8","customer_id":"ed7b2379-c08e-44d1-823c-90b5c4f028e9","plan_type":"ENTERPRISE","status":"ACTIVE","start_date":"2026-11-03T10:00:00Z","entitlements":[{"feature_key":"featureKey1","allocated_quota":3,"consumed_quota":2},{"feature_key":"featureKey2","allocated_quota":5,"consumed_quota":4}]}"""
      }
    }
  }
}
