package me.bataillec.portfolio.customer.http

import io.vertx.core.json.JsonObject
import me.bataillec.portfolio.customer.domain.{CustomerStatus, DomainError, PlanType}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.UUID

class ProblemSpec extends AnyFlatSpec with Matchers {
  private val id = UUID.fromString("11111111-1111-1111-1111-111111111111")

  "fromDomainError" should "follow the Problem contract" in {
    val response = Problem.fromDomainError(DomainError.CustomerNotFound(id))
    val body     = JsonObject(response.body)

    response.statusCode shouldBe 404
    response.contentType shouldBe "application/problem+json"
    body.getString("title") shouldBe "Customer not found"
    body.getInteger("status") shouldBe 404
    body.getString("code") shouldBe "CUSTOMER_NOT_FOUND"
    body.getString("detail") shouldBe s"Customer '$id' does not exist"
    body.containsKey("statusCode") shouldBe false
  }

  it should "keep the identity of every domain error" in {
    val errors = List(
      DomainError.InvalidCursor("abc")                               -> ("INVALID_CURSOR", 400),
      DomainError.EmailAlreadyUsed("a@b.com")                        -> ("EMAIL_ALREADY_USED", 409),
      DomainError.QuotaExceeded("CALLS", None)                       -> ("QUOTA_EXCEEDED", 429),
      DomainError.EntitlementNotFound(id, "CALLS")                   -> ("ENTITLEMENT_NOT_FOUND", 404),
      DomainError.PlanNotConfigured(PlanType.PRO)                    -> ("PLAN_NOT_CONFIGURED", 500),
      DomainError.CustomerNotActive(id, CustomerStatus.DELETED)      -> ("CUSTOMER_NOT_ACTIVE", 409),
      DomainError.PlanAlreadyActive(id, PlanType.PRO)                -> ("PLAN_ALREADY_ACTIVE", 409),
      DomainError.NoActiveSubscription(id)                           -> ("NO_ACTIVE_SUBSCRIPTION", 409)
    )

    errors.foreach { case (error, (code, status)) =>
      val response = Problem.fromDomainError(error)
      response.statusCode shouldBe status
      JsonObject(response.body).getString("code") shouldBe code
    }
  }

  it should "only send Retry-After when waiting helps" in {
    Problem.fromDomainError(DomainError.QuotaExceeded("CALLS", Some(60))).headers shouldBe Map("Retry-After" -> "60")
    Problem.fromDomainError(DomainError.QuotaExceeded("STORAGE_GB", None)).headers shouldBe Map.empty
  }

  "HttpResponse" should "keep the body as it is across the event bus" in {
    val response = Problem.fromDomainError(DomainError.QuotaExceeded("CALLS", Some(60)))

    HttpResponse.fromJsonString(response.toJson.encode()) shouldBe response
  }

  it should "keep an array body" in {
    val response = HttpResponse(200, """[{"plan_type":"PRO"}]""")

    HttpResponse.fromJsonString(response.toJson.encode()) shouldBe response
  }
}
