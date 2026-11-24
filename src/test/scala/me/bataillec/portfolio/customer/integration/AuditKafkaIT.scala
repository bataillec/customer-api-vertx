package me.bataillec.portfolio.customer.integration

import io.vertx.core.json.JsonObject
import me.bataillec.portfolio.customer.support.{AppFixture, AuditEvent, AuditProbe}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.time.{Duration, Instant}
import java.util.UUID
import java.util.concurrent.{Executors, TimeUnit}
import scala.compiletime.uninitialized
import scala.jdk.CollectionConverters.*

/** The audit flow end to end: an HTTP call, the outbox table, the audit verticle's relay and the Kafka topic.
  * The relay publishes after the commit, so the assertions poll the topic instead of expecting the event at once.
  */
class AuditKafkaIT extends AnyWordSpec with Matchers with AppFixture {
  private var probe: AuditProbe = uninitialized

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    probe = new AuditProbe(containers.kafkaBootstrap, auditTopic)
  }

  override protected def afterAll(): Unit = {
    try if (probe != null) probe.close()
    finally super.afterAll()
  }

  private def newCustomerId(): String = {
    val created = api.createCustomer()
    created.status shouldBe 201
    created.string("id")
  }

  "an audit event" should {
    "be a valid v1 envelope keyed by the customer id" in {
      val before = Instant.now().minusSeconds(5)
      val id     = newCustomerId()

      val event = probe.awaitEvents(id, 1).head

      event.key shouldBe id
      event.eventType shouldBe "CUSTOMER_CREATED"
      event.customerId shouldBe id
      event.json.getString("producer") shouldBe "customer-api"
      event.json.getInteger("schema_version") shouldBe 1
      UUID.fromString(event.json.getString("event_id")) should not be null
      Instant.parse(event.json.getString("occurred_at")).isAfter(before) shouldBe true
      Instant.parse(event.json.getString("occurred_at")).isBefore(Instant.now().plusSeconds(5)) shouldBe true
    }

    "have a unique event id" in {
      val ids = (1 to 3).map(_ => newCustomerId())

      val eventIds = ids.flatMap(probe.awaitEvents(_, 1)).map(_.json.getString("event_id"))

      eventIds.distinct should have size 3
    }
  }

  "the operations" should {
    "publish CUSTOMER_UPDATED with the company name and the status change" in {
      val id = newCustomerId()
      api.patch(s"/customers/$id", JsonObject().put("company_name", "Acme")).status shouldBe 200

      val update = probe.awaitEvents(id, 2).last

      update.eventType shouldBe "CUSTOMER_UPDATED"
      update.field("company_name") shouldBe Some("Acme")
      update.field("old_status") shouldBe Some("ACTIVE")
      update.field("new_status") shouldBe Some("ACTIVE")
    }

    "publish the stored company name when only the status changes, and no sentinel" in {
      val id = newCustomerId()
      api.patch(s"/customers/$id", JsonObject().put("company_name", "Acme")).status shouldBe 200
      api.setStatus(id, "SUSPENDED").status shouldBe 200

      val update = probe.awaitEvents(id, 3).last

      update.field("company_name") shouldBe Some("Acme")
      update.field("old_status") shouldBe Some("ACTIVE")
      update.field("new_status") shouldBe Some("SUSPENDED")
      update.json.fieldNames().asScala.map(update.json.getValue) should not contain "-"
    }

    "leave the company name out when the customer has none" in {
      val id = newCustomerId()
      api.setStatus(id, "SUSPENDED").status shouldBe 200

      val update = probe.awaitEvents(id, 2).last

      update.json.containsKey("company_name") shouldBe false
      update.field("new_status") shouldBe Some("SUSPENDED")
    }

    "publish a status change on delete" in {
      val id = newCustomerId()
      api.setStatus(id, "DELETED").status shouldBe 200

      val update = probe.awaitEvents(id, 2).last

      update.eventType shouldBe "CUSTOMER_UPDATED"
      update.field("old_status") shouldBe Some("ACTIVE")
      update.field("new_status") shouldBe Some("DELETED")
    }

    "publish SUBSCRIPTION_CHANGED, with the previous plan on a plan change" in {
      val id = newCustomerId()
      api.subscribe(id, "FREE_TIER").status shouldBe 201
      api.subscribe(id, "PRO").status shouldBe 201

      val events = probe.awaitEvents(id, 3).filter(_.eventType == "SUBSCRIPTION_CHANGED")

      events.map(e => (e.field("plan_type"), e.field("previous_plan_type"))) shouldBe
        List((Some("FREE_TIER"), None), (Some("PRO"), Some("FREE_TIER")))
    }

    "publish QUOTA_EXCEEDED with the feature key, and no QUOTA_CONSUMED for the refusal" in {
      val id = newCustomerId()
      api.subscribe(id, "FREE_TIER").status shouldBe 201
      api.consume(id, "STORAGE_GB", 1).status shouldBe 200
      api.consume(id, "STORAGE_GB", 1).status shouldBe 429

      val events = probe.awaitEvents(id, 4)

      events.map(_.eventType) shouldBe List("CUSTOMER_CREATED", "SUBSCRIPTION_CHANGED", "QUOTA_CONSUMED", "QUOTA_EXCEEDED")
      events.last.field("feature_key") shouldBe Some("STORAGE_GB")
    }

    "publish QUOTA_CONSUMED with the amount and the quotas, in order with the other events of the customer" in {
      val id = newCustomerId()
      api.subscribe(id, "FREE_TIER").status shouldBe 201
      api.consume(id, "API_CALLS_MONTHLY", 10).status shouldBe 200
      api.consume(id, "API_CALLS_MONTHLY", 5).status shouldBe 200

      val events = probe.awaitEvents(id, 4)

      events.map(_.eventType) shouldBe List("CUSTOMER_CREATED", "SUBSCRIPTION_CHANGED", "QUOTA_CONSUMED", "QUOTA_CONSUMED")
      val consumed = events.filter(_.eventType == "QUOTA_CONSUMED")
      consumed.foreach(_.key shouldBe id)
      consumed.map(e => (e.json.getString("feature_key"), e.json.getLong("amount"), e.json.getLong("consumed_quota"), e.json.getLong("allocated_quota"))) shouldBe
        List(("API_CALLS_MONTHLY", 10L, 10L, 1000L), ("API_CALLS_MONTHLY", 5L, 15L, 1000L))
      consumed.map(_.partition).distinct should have size 1
      consumed.map(_.offset) shouldBe consumed.map(_.offset).sorted
    }
  }

  "a failed request" should {
    "produce no event" in {
      val id = newCustomerId()
      api.subscribe(id, "PRO").status shouldBe 201

      api.subscribe(id, "PRO").status shouldBe 409                                 // PLAN_ALREADY_ACTIVE
      api.subscribe(id, "GOLD").status shouldBe 400                                // validation
      api.patch(s"/customers/$id", JsonObject().put("status", "NOPE")).status shouldBe 400
      api.consume(id, "NOPE", 1).status shouldBe 404                               // ENTITLEMENT_NOT_FOUND
      api.consume(id, "API_CALLS_MONTHLY", 0).status shouldBe 400
      // The events of a customer share a partition, so this one arrives after any event the failures could have produced
      api.patch(s"/customers/$id", JsonObject().put("company_name", "marker")).status shouldBe 200

      val events = probe.awaitEvents(id, 3)

      events.map(_.eventType) shouldBe List("CUSTOMER_CREATED", "SUBSCRIPTION_CHANGED", "CUSTOMER_UPDATED")
      probe.eventsAfter(id, Duration.ofSeconds(2)) should have size 3
    }

    "produce no event for a customer that does not exist or is not active" in {
      val id = newCustomerId()
      api.setStatus(id, "SUSPENDED").status shouldBe 200

      api.subscribe(id, "PRO").status shouldBe 409
      api.consume(id, "API_CALLS_MONTHLY", 1).status shouldBe 409
      api.get(s"/customers/${UUID.randomUUID()}").status shouldBe 404
      api.setStatus(id, "ACTIVE").status shouldBe 200

      probe.awaitEvents(id, 3).map(_.eventType) shouldBe List("CUSTOMER_CREATED", "CUSTOMER_UPDATED", "CUSTOMER_UPDATED")
      probe.eventsAfter(id, Duration.ofSeconds(2)) should have size 3
    }
  }

  "the ordering" should {
    "be kept per customer, on one partition, whatever the other customers do" in {
      val pool = Executors.newFixedThreadPool(8)
      val ids =
        try {
          val flows = (1 to 8).map { _ =>
            pool.submit(() => {
              val id = newCustomerId()
              api.subscribe(id, "FREE_TIER").status shouldBe 201
              api.subscribe(id, "PRO").status shouldBe 201
              api.setStatus(id, "DELETED").status shouldBe 200
              id
            })
          }
          flows.map(_.get(120, TimeUnit.SECONDS)).toList
        } finally pool.shutdown()

      val perCustomer = ids.map(id => id -> probe.awaitEvents(id, 4))

      perCustomer.foreach { (id, events: List[AuditEvent]) =>
        events.map(_.eventType) shouldBe List("CUSTOMER_CREATED", "SUBSCRIPTION_CHANGED", "SUBSCRIPTION_CHANGED", "CUSTOMER_UPDATED")
        events.map(_.partition).distinct should have size 1
        events.map(_.offset) shouldBe events.map(_.offset).sorted
        events.map(_.key).distinct shouldBe List(id)
      }
      // 8 customers over 3 partitions: the key really spreads the load
      perCustomer.map(_._2.head.partition).distinct.size should be > 1
    }
  }
}
