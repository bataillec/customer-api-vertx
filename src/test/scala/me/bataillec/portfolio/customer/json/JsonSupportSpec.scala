package me.bataillec.portfolio.customer.json

import me.bataillec.portfolio.customer.domain.*
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.Instant
import java.util.UUID
import scala.collection.immutable.ListMap

class JsonSupportSpec extends AnyFlatSpec with Matchers {

  private val customer = Customer(
    id = UUID.fromString("11111111-1111-1111-1111-111111111111"),
    email = "a@b.com",
    companyName = Some("Acme"),
    status = CustomerStatus.ACTIVE,
    createdAt = Instant.parse("2026-10-26T14:00:00Z"),
    updatedAt = Instant.parse("2026-10-26T14:00:00Z")
  )

  "JsonSupport" should "decode snake_case JSON into a case class with an Option" in {
    // language=JSON
    val json =
      """{
        |   "email": "a@b.com",
        |   "company_name": "Acme"
        | }
        |""".stripMargin
    JsonSupport.decode(json, classOf[CreateCustomer]) shouldBe CreateCustomer("a@b.com", Some("Acme"))
  }

  it should "decode a missing optional field as None" in {
    // language=JSON
    val json =
      """{
        |   "email": "a@b.com"
        |}""".stripMargin
    JsonSupport.decode(json, classOf[CreateCustomer]) shouldBe CreateCustomer("a@b.com", None)
  }

  it should "encode a customer with snake_case names, an enum name and ISO dates" in {
    val json = JsonSupport.encode(customer)
    json should include(""""company_name":"Acme"""")
    json should include(""""status":"ACTIVE"""")
    json should include(""""created_at":"2026-10-26T14:00:00Z"""")
    json should include(""""id":"11111111-1111-1111-1111-111111111111"""")
  }

  it should "round-trip a customer" in {
    JsonSupport.decode(JsonSupport.encode(customer), classOf[Customer]) shouldBe customer
  }

  it should "encode a plan with the reset policy of each quota" in {
    val plan = Plan(PlanType.PRO, ListMap("STORAGE_GB" -> PlanQuota(50, resetsMonthly = false)))
    JsonSupport.encode(plan) shouldBe """{"plan_type":"PRO","quotas":{"STORAGE_GB":{"quota":50,"resets_monthly":false}}}"""
  }

  it should "encode the reset date of an entitlement and omit it when there is none" in {
    JsonSupport.encode(Entitlement("CALLS", 10, 1, Some(Instant.parse("2026-11-01T00:00:00Z")))) should include(""""reset_date":"2026-11-01T00:00:00Z"""")
    JsonSupport.encode(Entitlement("STORAGE_GB", 10, 1, None)) should not include "reset_date"
  }

  "ListCustomers" should "build an HttpResponse with the items and next_cursor" in {
    val response = ListCustomers(List(customer), Some("abc")).toHttpResponse()
    response.statusCode shouldBe 200
    response.body should include(""""items":[{""")
    response.body should include(""""next_cursor":"abc"""")
  }
}
