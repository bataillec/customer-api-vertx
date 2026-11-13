package me.bataillec.portfolio.customer.unit.audit

import io.vertx.core.{Future, Vertx}
import io.vertx.kafka.client.producer.{KafkaProducer, KafkaProducerRecord, RecordMetadata}
import me.bataillec.portfolio.customer.audit.KafkaAuditWriter
import me.bataillec.portfolio.customer.config.AuditConfig
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.*
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class KafkaAuditWriterSpec extends AnyWordSpec with Matchers {
  private val message =
    """{"event_id":"e","event_type":"CUSTOMER_CREATED","customer_id":"11111111-2222-3333-4444-555555555555"}"""

  private def fixture() = {
    val producer = mock(classOf[KafkaProducer[String, String]])
    when(producer.send(any[KafkaProducerRecord[String, String]]()))
      .thenReturn(Future.succeededFuture[RecordMetadata](null))
    (producer, new KafkaAuditWriter(producer, "audit.topic"))
  }

  "audit" should {
    "send the message to the topic, keyed by customer_id" in {
      val (producer, writer) = fixture()
      val captor             = ArgumentCaptor.forClass(classOf[KafkaProducerRecord[String, String]])

      writer.audit(message).succeeded() shouldBe true

      verify(producer).send(captor.capture())
      val record = captor.getValue
      record.topic() shouldBe "audit.topic"
      record.key() shouldBe "11111111-2222-3333-4444-555555555555"
      record.value() shouldBe message
    }

    "fail with the cause when the send fails" in {
      val (producer, writer) = fixture()
      val cause              = new RuntimeException("broker down")
      when(producer.send(any[KafkaProducerRecord[String, String]]()))
        .thenReturn(Future.failedFuture[RecordMetadata](cause))

      val result = writer.audit(message)

      result.failed() shouldBe true
      result.cause() shouldBe cause
    }

    "throw and not send when the message is not JSON" in {
      val (producer, writer) = fixture()

      an[Exception] should be thrownBy writer.audit("not json")

      verify(producer, never()).send(any[KafkaProducerRecord[String, String]]())
    }
  }

  "close" should {
    "close the producer and return its future" in {
      val (producer, writer) = fixture()
      val closed             = Future.succeededFuture[Void]()
      when(producer.close()).thenReturn(closed)

      writer.close() shouldBe theSameInstanceAs(closed)
    }
  }

  "apply" should {
    "build a writer on a real producer without connecting at creation" in {
      val vertx = Vertx.vertx()
      try {
        val writer = KafkaAuditWriter(vertx, AuditConfig("localhost:1", "audit.topic"))
        writer should not be null
        writer.close().await()
      } finally vertx.close().await()
    }
  }
}
