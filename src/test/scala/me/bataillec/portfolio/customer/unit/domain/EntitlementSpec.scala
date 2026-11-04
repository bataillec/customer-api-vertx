package me.bataillec.portfolio.customer.unit.domain

import io.vertx.sqlclient.Row
import me.bataillec.portfolio.customer.domain.Entitlement
import me.bataillec.portfolio.customer.http.HttpResponse
import org.mockito.Mockito.*
import org.scalatest.Inside.inside
import org.scalatest.OptionValues.*
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.time.{Clock, Instant, OffsetDateTime}

class EntitlementSpec extends AnyWordSpec with Matchers {
  "Entitlement" should {
    val fixedInstantString = "2026-11-03T10:00:00Z"
    val fixedInstant       = Instant.parse(fixedInstantString)
    val clock              = mock(classOf[Clock])
    when(clock.instant()).thenReturn(fixedInstant)

    val entitlement = Entitlement("featureKey", 3L, 2L, Some(Instant.now(clock)))

    "be built" when {
      "full" in {
        inside(entitlement) { case Entitlement(featureKey, allocatedQuota, consumedQuota, resetDate) =>
          featureKey shouldBe "featureKey"
          allocatedQuota shouldBe 3L
          consumedQuota shouldBe 2L
          resetDate.value.toString shouldBe fixedInstantString
        }
      }

      "minimal" in {
        val entitlement = Entitlement("featureKey", 3L, 2L, None)

        inside(entitlement) { case Entitlement(featureKey, allocatedQuota, consumedQuota, resetDate) =>
          featureKey shouldBe "featureKey"
          allocatedQuota shouldBe 3L
          consumedQuota shouldBe 2L
          resetDate shouldBe empty
        }
      }
    }

    "become a HTTPResponse" in {
      val response = entitlement.toHttpResponse()

      inside(response) { case HttpResponse(statusCode, body, contentType, headers) =>
        statusCode shouldBe 200
        contentType shouldBe "application/json"
        headers shouldBe Map.empty
        body shouldBe """{"feature_key":"featureKey","allocated_quota":3,"consumed_quota":2,"reset_date":"2026-11-03T10:00:00Z"}"""
      }
    }

    "be built froma Row" in {
      val row = mock(classOf[Row])

      when(row.getString("feature_key")).thenReturn("featureKey")
      when(row.getLong("allocated_quota")).thenReturn(3L)
      when(row.getLong("consumed_quota")).thenReturn(2L)
      when(row.getOffsetDateTime("reset_date")).thenReturn(OffsetDateTime.parse(fixedInstantString))

      Entitlement.fromRow(row) shouldBe entitlement
    }

    "effective method" when {
      val clockBefore = mock(classOf[Clock])
      when(clockBefore.instant()).thenReturn(Instant.parse("2026-11-01T10:00:00Z"))

      "no reset date" in {
        val e = Entitlement("featureKey", 3L, 2L, None)

        inside(e.effective(Instant.now(clockBefore))) { case Entitlement(featureKey, allocatedQuota, consumedQuota, resetDate) =>
          featureKey shouldBe "featureKey"
          allocatedQuota shouldBe 3L
          consumedQuota shouldBe 2L
          resetDate shouldBe empty
        }
      }

      "reset date before now" in {
        val e = Entitlement("featureKey", 3L, 2L, Some(Instant.now(clock)))

        inside(e.effective(Instant.now(clockBefore))) { case Entitlement(featureKey, allocatedQuota, consumedQuota, resetDate) =>
          featureKey shouldBe "featureKey"
          allocatedQuota shouldBe 3L
          consumedQuota shouldBe 2L
          resetDate.value.toString shouldBe fixedInstantString
        }
      }

      "reset date after now" in {
        val clockAfter = mock(classOf[Clock])
        when(clockAfter.instant()).thenReturn(Instant.parse("2026-11-06T10:00:00Z"))

        val e = Entitlement("featureKey", 3L, 2L, Some(Instant.now(clock)))

        inside(e.effective(Instant.now(clockAfter))) { case Entitlement(featureKey, allocatedQuota, consumedQuota, resetDate) =>
          featureKey shouldBe "featureKey"
          allocatedQuota shouldBe 3L
          consumedQuota shouldBe 0L
          resetDate.value.toString shouldBe "2026-12-01T00:00:00Z"
        }
      }
    }
  }
}
