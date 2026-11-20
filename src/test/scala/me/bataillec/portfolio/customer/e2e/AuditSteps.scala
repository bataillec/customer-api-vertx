package me.bataillec.portfolio.customer.e2e

import io.cucumber.datatable.DataTable
import io.cucumber.scala.{EN, ScalaDsl}
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters.*

/** The steps about the audit trail, which the platform writes to Kafka. */
class AuditSteps extends ScalaDsl with EN with Matchers {
  import World.*

  // The audit is fire-and-forget: the probe polls the topic until the events have arrived
  Then("the audit trail of the customer shows, in this order:") { (table: DataTable) =>
    val expected = table.asList().asScala.toList
    probe.awaitEvents(customer, expected.size).map(_.eventType) shouldBe expected
  }

  Then("every audit event of the customer is keyed by the customer") { () =>
    val events = probe.awaitEvents(customer, 1)
    events.foreach(_.key shouldBe customer)
  }
}
