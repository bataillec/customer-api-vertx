package me.bataillec.portfolio.customer.e2e

import io.cucumber.scala.{EN, ScalaDsl}
import io.vertx.core.json.JsonObject
import org.scalatest.matchers.should.Matchers

import java.util.UUID
import scala.jdk.CollectionConverters.*

/** The steps about customers, their subscription and their entitlements. */
class CustomerSteps extends ScalaDsl with EN with Matchers {
  import World.*

  private def register(): Unit = {
    val newEmail = s"${UUID.randomUUID()}@example.com"
    val created  = api.createCustomer(newEmail)
    withClue(s"Registering $newEmail answered ${created.status} ${created.body}: ")(created.status shouldBe 201)
    customerId = Some(created.string("id"))
    email = Some(newEmail)
  }

  private def subscribe(plan: String): Unit = {
    last = Some(api.subscribe(customer, plan))
  }

  private def consume(amount: Int, feature: String): Unit = {
    last = Some(api.consume(customer, feature, amount.toLong))
  }

  private def entitlement(feature: String): JsonObject = {
    val all = api.get(s"/customers/$customer/entitlements").jsonArray.asScala.map(_.asInstanceOf[JsonObject]).toList
    all.find(_.getString("feature_key") == feature).getOrElse(fail(s"The customer has no entitlement to $feature, only ${all.map(_.getString("feature_key"))}"))
  }

  private def refused(status: Int, code: String): Unit = {
    withClue(s"The API answered ${response.status} ${response.body}: ") {
      response.status shouldBe status
      response.code shouldBe code
    }
  }

  Given("a registered customer") { () =>
    register()
  }

  Given("a registered customer subscribed to the {string} plan") { (plan: String) =>
    register()
    subscribe(plan)
    response.status shouldBe 201
  }

  Given("the customer has already consumed {int} of {string}") { (amount: Int, feature: String) =>
    consume(amount, feature)
    response.status shouldBe 200
  }

  When("a new customer registers") { () =>
    val newEmail = s"${UUID.randomUUID()}@example.com"
    last = Some(api.createCustomer(newEmail))
    last.filter(_.status == 201).foreach { created =>
      customerId = Some(created.string("id"))
      email = Some(newEmail)
    }
  }

  When("another customer registers with the same email") { () =>
    last = Some(api.createCustomer(email.getOrElse(fail("No customer has registered yet"))))
  }

  When("the customer subscribes to the {string} plan") { (plan: String) =>
    subscribe(plan)
  }

  When("the customer consumes {int} of {string}") { (amount: Int, feature: String) =>
    consume(amount, feature)
  }

  When("the customer is suspended") { () =>
    last = Some(api.setStatus(customer, "SUSPENDED"))
    response.status shouldBe 200
  }

  When("the customer is deleted") { () =>
    last = Some(api.setStatus(customer, "DELETED"))
    response.status shouldBe 200
  }

  When("the customer is reactivated") { () =>
    last = Some(api.setStatus(customer, "ACTIVE"))
    response.status shouldBe 200
  }

  When("the company name of the customer is changed to {string}") { (name: String) =>
    last = Some(api.patch(s"/customers/$customer", JsonObject().put("company_name", name)))
    response.status shouldBe 200
  }

  When("someone lists the plans") { () =>
    last = Some(api.get("/plans"))
  }

  Then("the customer is registered") { () =>
    response.status shouldBe 201
    api.get(s"/customers/$customer").status shouldBe 200
  }

  Then("the subscription is active") { () =>
    response.status shouldBe 201
    response.string("status") shouldBe "ACTIVE"
  }

  Then("the customer status is {string}") { (status: String) =>
    val fetched = api.get(s"/customers/$customer")
    fetched.status shouldBe 200
    fetched.string("status") shouldBe status
  }

  Then("the customer is allocated {int} of {string}") { (quota: Int, feature: String) =>
    entitlement(feature).getLong("allocated_quota") shouldBe quota.toLong
  }

  Then("the customer has used {int} of {string}") { (used: Int, feature: String) =>
    entitlement(feature).getLong("consumed_quota") shouldBe used.toLong
  }

  Then("the customer has {int} of {string} left") { (left: Int, feature: String) =>
    val e = entitlement(feature)
    (e.getLong("allocated_quota") - e.getLong("consumed_quota")) shouldBe left.toLong
  }

  Then("the consumption is accepted") { () =>
    withClue(s"The API answered ${response.status} ${response.body}: ")(response.status shouldBe 200)
  }

  Then("the request is refused because the quota is exceeded") { () =>
    refused(429, "QUOTA_EXCEEDED")
  }

  Then("the request is refused because the customer is not active") { () =>
    refused(409, "CUSTOMER_NOT_ACTIVE")
  }

  Then("the request is refused because the email is already used") { () =>
    refused(409, "EMAIL_ALREADY_USED")
  }

  Then("the plans on offer are {string}, {string} and {string}") { (first: String, second: String, third: String) =>
    response.status shouldBe 200
    response.jsonArray.asScala.map(_.asInstanceOf[JsonObject].getString("plan_type")).toSet shouldBe Set(first, second, third)
  }
}
