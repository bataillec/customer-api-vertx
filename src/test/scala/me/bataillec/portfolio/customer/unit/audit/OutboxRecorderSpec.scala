package me.bataillec.portfolio.customer.unit.audit

import io.vertx.core.Future
import io.vertx.core.json.JsonObject
import io.vertx.sqlclient.SqlClient
import me.bataillec.portfolio.customer.audit.OutboxRecorder
import me.bataillec.portfolio.customer.customer.Storage
import me.bataillec.portfolio.customer.support.FutureUtils.{await, failureOf}
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.{any, eq as eqTo}
import org.mockito.Mockito.*
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.time.{Clock, Instant, ZoneOffset}
import java.util.UUID

class OutboxRecorderSpec extends AnyWordSpec with Matchers {
  private val customerId = UUID.fromString("11111111-2222-3333-4444-555555555555")
  private val eventId    = UUID.fromString("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
  private val now        = Instant.parse("2026-11-13T10:15:30Z")

  private class Fixture {
    val storage: Storage = mock(classOf[Storage])
    val tx: SqlClient    = mock(classOf[SqlClient])
    val recorder         = new OutboxRecorder(storage, Clock.fixed(now, ZoneOffset.UTC), () => eventId)
    when(storage.insertOutbox(any[SqlClient](), any[UUID](), any[JsonObject]())).thenReturn(Future.succeededFuture(()))

    def written: JsonObject = {
      val captor = ArgumentCaptor.forClass(classOf[JsonObject])
      verify(storage).insertOutbox(eqTo(tx), eqTo(customerId), captor.capture())
      captor.getValue
    }
  }

  "prepare" should {
    "build the envelope with the extras, and write nothing" in {
      val f = new Fixture

      val json = f.recorder.prepare("QUOTA_CONSUMED", customerId, "feature_key" -> "api_calls", "amount" -> 5L)

      json.getString("event_type") shouldBe "QUOTA_CONSUMED"
      json.getString("event_id") shouldBe "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
      json.getString("feature_key") shouldBe "api_calls"
      json.getLong("amount") shouldBe 5L
      verifyNoInteractions(f.storage)
    }
  }

  "record" should {
    "write the platform envelope v1 on the given transaction" in {
      val f = new Fixture

      await(f.recorder.record(f.tx, "CUSTOMER_CREATED", customerId))

      val json = f.written
      json.getString("event_id") shouldBe "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
      json.getString("event_type") shouldBe "CUSTOMER_CREATED"
      json.getString("occurred_at") shouldBe "2026-11-13T10:15:30Z"
      json.getString("producer") shouldBe "customer-api"
      json.getInteger("schema_version") shouldBe 1
      json.getValue("customer_id") shouldBe "11111111-2222-3333-4444-555555555555"
      json.fieldNames() should have size 6
    }

    "add the extras" in {
      val f = new Fixture

      await(f.recorder.record(f.tx, "SUBSCRIPTION_CHANGED", customerId, "plan_type" -> "PRO", "previous_plan_type" -> "FREE_TIER"))

      f.written.getString("plan_type") shouldBe "PRO"
      f.written.getString("previous_plan_type") shouldBe "FREE_TIER"
    }

    "write an integer extra as a JSON number, not as a string" in {
      val f = new Fixture

      await(f.recorder.record(f.tx, "QUOTA_CONSUMED", customerId, "amount" -> 3L))

      f.written.getValue("amount") shouldBe a[java.lang.Number]
      f.written.getLong("amount") shouldBe 3L
    }

    "leave out an extra that is null" in {
      val f = new Fixture

      await(f.recorder.record(f.tx, "CUSTOMER_UPDATED", customerId, "company_name" -> (null: String)))

      f.written.containsKey("company_name") shouldBe false
    }

    "not let an extra overwrite a core field" in {
      val f = new Fixture

      await(f.recorder.record(f.tx, "CUSTOMER_CREATED", customerId, "event_type" -> "FORGED", "customer_id" -> "nobody", "schema_version" -> 9L))

      val json = f.written
      json.getString("event_type") shouldBe "CUSTOMER_CREATED"
      json.getString("customer_id") shouldBe customerId.toString
      json.getInteger("schema_version") shouldBe 1
    }

    "fail when the row cannot be written, so that the transaction rolls back" in {
      val f     = new Fixture
      val error = new RuntimeException("boom")
      when(f.storage.insertOutbox(any[SqlClient](), any[UUID](), any[JsonObject]())).thenReturn(Future.failedFuture[Unit](error))

      failureOf(f.recorder.record(f.tx, "CUSTOMER_CREATED", customerId)) shouldBe error
    }

    "give each event its own id by default" in {
      val storage  = mock(classOf[Storage])
      val tx       = mock(classOf[SqlClient])
      val captor   = ArgumentCaptor.forClass(classOf[JsonObject])
      when(storage.insertOutbox(any[SqlClient](), any[UUID](), any[JsonObject]())).thenReturn(Future.succeededFuture(()))
      val recorder = new OutboxRecorder(storage)

      await(recorder.record(tx, "CUSTOMER_CREATED", customerId))
      await(recorder.record(tx, "CUSTOMER_CREATED", customerId))

      verify(storage, times(2)).insertOutbox(eqTo(tx), eqTo(customerId), captor.capture())
      captor.getAllValues.stream().map(_.getString("event_id")).distinct().count() shouldBe 2
    }
  }
}
