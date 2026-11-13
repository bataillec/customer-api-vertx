package me.bataillec.portfolio.customer.unit.eventbus

import io.vertx.core.eventbus.{DeliveryOptions, EventBus}
import io.vertx.core.json.JsonObject
import me.bataillec.portfolio.customer.Constants.AUDIT_ADDRESS
import me.bataillec.portfolio.customer.eventbus.AuditSenderImpl
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.{any, eq as eqTo}
import org.mockito.Mockito.*
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks.*
import org.scalatest.wordspec.AnyWordSpec

import java.time.{Clock, Instant, ZoneOffset}
import java.util.UUID
import scala.jdk.CollectionConverters.*

class AuditSenderImplSpec extends AnyWordSpec with Matchers {
  private val customerId = UUID.fromString("11111111-2222-3333-4444-555555555555")
  private val eventId    = UUID.fromString("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
  private val now        = Instant.parse("2026-11-13T10:15:30Z")
  private val coreFields = Set("event_id", "event_type", "occurred_at", "producer", "schema_version", "customer_id")

  // region Helpers

  private class Fixture {
    val eb: EventBus   = mock(classOf[EventBus])
    val sender         = new AuditSenderImpl(eb, Clock.fixed(now, ZoneOffset.UTC), () => eventId)
    private val captor = ArgumentCaptor.forClass(classOf[AnyRef])

    /** The raw object published on the audit address. */
    def published: AnyRef = {
      verify(eb).publish(eqTo(AUDIT_ADDRESS), captor.capture())
      captor.getValue
    }

    def publishedJson: JsonObject = JsonObject(published.asInstanceOf[String])
  }

  // endregion

  "send" should {
    "publish (not send) once on the audit address" in {
      val f = new Fixture

      f.sender.send("CUSTOMER_CREATED", customerId)

      verify(f.eb, times(1)).publish(eqTo(AUDIT_ADDRESS), any[AnyRef]())
      verify(f.eb, never()).send(any[String](), any[AnyRef]())
      verify(f.eb, never()).send(any[String](), any[AnyRef](), any[DeliveryOptions]())
      verify(f.eb, never()).request[String](any[String](), any[AnyRef]())
    }

    "publish a String body, not a JsonObject" in {
      val f = new Fixture

      f.sender.send("CUSTOMER_CREATED", customerId)

      f.published shouldBe a[String]
    }

    "write the platform envelope v1 and the customer id (as a string)" in {
      val f = new Fixture

      f.sender.send("CUSTOMER_CREATED", customerId)

      val json = f.publishedJson
      json.getString("event_id") shouldBe "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
      json.getString("event_type") shouldBe "CUSTOMER_CREATED"
      json.getString("occurred_at") shouldBe "2026-11-13T10:15:30Z"
      json.getString("producer") shouldBe "customer-api"
      json.getInteger("schema_version") shouldBe 1
      json.getValue("customer_id") shouldBe "11111111-2222-3333-4444-555555555555"
    }

    "write only the envelope fields when there is no extra" in {
      val f = new Fixture

      f.sender.send("CUSTOMER_CREATED", customerId)

      f.publishedJson.fieldNames().asScala.toSet shouldBe coreFields
    }

    "generate a random event id and use the system clock by default" in {
      val eb = mock(classOf[EventBus])
      val captor = ArgumentCaptor.forClass(classOf[AnyRef])

      new AuditSenderImpl(eb).send("CUSTOMER_CREATED", customerId)

      verify(eb).publish(eqTo(AUDIT_ADDRESS), captor.capture())
      val json = JsonObject(captor.getValue.asInstanceOf[String])
      noException should be thrownBy UUID.fromString(json.getString("event_id"))
      noException should be thrownBy Instant.parse(json.getString("occurred_at"))
    }

    "extras" when {
      val table = Table(
        ("case", "extras"),
        ("one extra", Seq("email" -> "a@b.c")),
        ("two extras", Seq("email" -> "a@b.c", "status" -> "ACTIVE")),
        ("three extras", Seq("email" -> "a@b.c", "status" -> "ACTIVE", "company_name" -> "ACME"))
      )

      forAll(table) { (name: String, extras: Seq[(String, String)]) =>
        s"add $name as string fields" in {
          val f = new Fixture

          f.sender.send("CUSTOMER_UPDATED", customerId, extras*)

          val json = f.publishedJson
          json.size() shouldBe coreFields.size + extras.size
          json.getString("event_type") shouldBe "CUSTOMER_UPDATED"
          json.getString("customer_id") shouldBe customerId.toString
          extras.foreach((k, v) => json.getValue(k) shouldBe v)
        }
      }

      "keep the last value of a duplicated key" in {
        val f = new Fixture

        f.sender.send("CUSTOMER_UPDATED", customerId, "k" -> "first", "k" -> "last")

        f.publishedJson.getString("k") shouldBe "last"
      }

      "never let an extra overwrite an envelope field" in {
        val f = new Fixture

        f.sender.send(
          "CUSTOMER_UPDATED",
          customerId,
          "event_type"     -> "FORGED",
          "customer_id"    -> "forged",
          "event_id"       -> "forged",
          "producer"       -> "forged",
          "occurred_at"    -> "forged",
          "schema_version" -> "forged"
        )

        val json = f.publishedJson
        json.getString("event_type") shouldBe "CUSTOMER_UPDATED"
        json.getString("customer_id") shouldBe customerId.toString
        json.getString("event_id") shouldBe eventId.toString
        json.getString("producer") shouldBe "customer-api"
        json.getString("occurred_at") shouldBe "2026-11-13T10:15:30Z"
        json.getInteger("schema_version") shouldBe 1
      }

      "write a null value as a JSON null" in {
        val f = new Fixture

        f.sender.send("CUSTOMER_UPDATED", customerId, "k" -> null)

        val json = f.publishedJson
        json.containsKey("k") shouldBe true
        json.getValue("k") shouldBe null
      }

      "survive an encode / decode round trip of special characters" in {
        val f       = new Fixture
        val special = "quote \" backslash \\ newline \n unicode é ✓"

        f.sender.send("CUSTOMER_UPDATED", customerId, "k" -> special)

        f.publishedJson.getString("k") shouldBe special
      }
    }
  }
}
