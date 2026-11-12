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

import java.util.UUID
import scala.jdk.CollectionConverters.*

class AuditSenderImplSpec extends AnyWordSpec with Matchers {
  private val customerId = UUID.fromString("11111111-2222-3333-4444-555555555555")

  // region Helpers

  private class Fixture {
    val eb: EventBus   = mock(classOf[EventBus])
    val sender         = new AuditSenderImpl(eb)
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

      f.sender.send("CREATE", customerId)

      verify(f.eb, times(1)).publish(eqTo(AUDIT_ADDRESS), any[AnyRef]())
      verify(f.eb, never()).send(any[String](), any[AnyRef]())
      verify(f.eb, never()).send(any[String](), any[AnyRef](), any[DeliveryOptions]())
      verify(f.eb, never()).request[String](any[String](), any[AnyRef]())
    }

    "publish a String body, not a JsonObject" in {
      val f = new Fixture

      f.sender.send("CREATE", customerId)

      f.published shouldBe a[String]
    }

    "write the action and the customer id (as a string)" in {
      val f = new Fixture

      f.sender.send("CREATE", customerId)

      val json = f.publishedJson
      json.getString("action") shouldBe "CREATE"
      json.getValue("customer_id") shouldBe "11111111-2222-3333-4444-555555555555"
    }

    "write only the 2 core fields when there is no extra" in {
      val f = new Fixture

      f.sender.send("CREATE", customerId)

      f.publishedJson.fieldNames().asScala.toSet shouldBe Set("action", "customer_id")
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

          f.sender.send("UPDATE", customerId, extras*)

          val json = f.publishedJson
          json.size() shouldBe 2 + extras.size
          json.getString("action") shouldBe "UPDATE"
          json.getString("customer_id") shouldBe customerId.toString
          extras.foreach((k, v) => json.getValue(k) shouldBe v)
        }
      }

      "keep the last value of a duplicated key" in {
        val f = new Fixture

        f.sender.send("UPDATE", customerId, "k" -> "first", "k" -> "last")

        f.publishedJson.getString("k") shouldBe "last"
      }

      "write a null value as a JSON null" in {
        val f = new Fixture

        f.sender.send("UPDATE", customerId, "k" -> null)

        val json = f.publishedJson
        json.containsKey("k") shouldBe true
        json.getValue("k") shouldBe null
      }

      "survive an encode / decode round trip of special characters" in {
        val f       = new Fixture
        val special = "quote \" backslash \\ newline \n unicode é ✓"

        f.sender.send("UPDATE", customerId, "k" -> special)

        f.publishedJson.getString("k") shouldBe special
      }
    }
  }
}
