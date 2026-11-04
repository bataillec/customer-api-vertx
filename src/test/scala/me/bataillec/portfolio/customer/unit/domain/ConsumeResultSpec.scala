package me.bataillec.portfolio.customer.unit.domain

import io.vertx.sqlclient.Row
import me.bataillec.portfolio.customer.domain.ConsumeResult
import me.bataillec.portfolio.customer.http.HttpResponse
import org.mockito.Mockito.*
import org.scalatest.Inside.inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class ConsumeResultSpec extends AnyWordSpec with Matchers {
  "ConsumeResult" should {
    val consumeResult = ConsumeResult("featureKey", 3L, 2L, 1L)

    "be built" in {

      inside(consumeResult) { case ConsumeResult(featureKey, allocatedQuota, consumedQuota, remainingQuota) =>
        featureKey shouldBe "featureKey"
        allocatedQuota shouldBe 3L
        consumedQuota shouldBe 2L
        remainingQuota shouldBe 1L
      }
    }

    "become a HTTPResponse" in {
      val response = consumeResult.toHttpResponse()

      inside(response) { case HttpResponse(statusCode, body, contentType, headers) =>
        statusCode shouldBe 200
        contentType shouldBe "application/json"
        headers shouldBe Map.empty
        body shouldBe """{"feature_key":"featureKey","allocated_quota":3,"consumed_quota":2,"remaining_quota":1}"""
      }
    }

    "be built from a Row" in {
      val row = mock(classOf[Row])

      when(row.getString("feature_key")).thenReturn("featureKey")
      when(row.getLong("allocated_quota")).thenReturn(3L)
      when(row.getLong("consumed_quota")).thenReturn(2L)

      val cr = ConsumeResult.fromRow(row)

      cr shouldBe consumeResult
    }
  }
}
