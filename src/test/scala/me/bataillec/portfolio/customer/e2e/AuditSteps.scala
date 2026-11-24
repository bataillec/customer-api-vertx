package me.bataillec.portfolio.customer.e2e

import io.cucumber.datatable.DataTable
import io.cucumber.scala.{EN, ScalaDsl}
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters.*

/** The steps about the audit trail, which the platform writes to Kafka. */
class AuditSteps extends ScalaDsl with EN with Matchers {
  import World.*

  // The relay publishes after the commit: the probe polls the topic until the events have arrived
  Then("the audit trail of the customer shows, in this order:") { (table: DataTable) =>
    val expected = table.asList().asScala.toList
    probe.awaitEvents(customer, expected.size).map(_.eventType) shouldBe expected
  }

  Then("the last audit event of the customer shows the company {string} and the status change from {string} to {string}") {
    (company: String, from: String, to: String) =>
      val update = probe.awaitEvents(customer, 3).last
      update.eventType shouldBe "CUSTOMER_UPDATED"
      update.field("company_name") shouldBe Some(company)
      update.field("old_status") shouldBe Some(from)
      update.field("new_status") shouldBe Some(to)
      update.json.fieldNames().asScala.map(update.json.getValue).foreach(_ should not be "-")
  }

  Then("every audit event of the customer is keyed by the customer") { () =>
    val events = probe.awaitEvents(customer, 1)
    events.foreach(_.key shouldBe customer)
  }
}
