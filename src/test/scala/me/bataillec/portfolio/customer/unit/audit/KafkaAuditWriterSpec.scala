package me.bataillec.portfolio.customer.unit.audit

import io.vertx.core.{Future, Vertx}
import io.vertx.kafka.admin.{ClusterDescription, KafkaAdminClient}
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
    val admin    = mock(classOf[KafkaAdminClient])
    (producer, admin, new KafkaAuditWriter(producer, admin, "audit.topic"))
  }

  "audit" should {
    "send the message to the topic, keyed by customer_id" in {
      val (producer, _, writer) = fixture()
      val captor             = ArgumentCaptor.forClass(classOf[KafkaProducerRecord[String, String]])

      writer.audit(message).succeeded() shouldBe true

      verify(producer).send(captor.capture())
      val record = captor.getValue
      record.topic() shouldBe "audit.topic"
      record.key() shouldBe "11111111-2222-3333-4444-555555555555"
      record.value() shouldBe message
    }

    "fail with the cause when the send fails" in {
      val (producer, _, writer) = fixture()
      val cause                = new RuntimeException("broker down")
      when(producer.send(any[KafkaProducerRecord[String, String]]()))
        .thenReturn(Future.failedFuture[RecordMetadata](cause))

      val result = writer.audit(message)

      result.failed() shouldBe true
      result.cause() shouldBe cause
    }

    "throw and not send when the message is not JSON" in {
      val (producer, _, writer) = fixture()

      an[Exception] should be thrownBy writer.audit("not json")

      verify(producer, never()).send(any[KafkaProducerRecord[String, String]]())
    }
  }

  "health" should {
    "succeed when a broker describes the cluster" in {
      val (_, admin, writer) = fixture()
      when(admin.describeCluster()).thenReturn(Future.succeededFuture(new ClusterDescription()))

      writer.health().succeeded() shouldBe true
    }

    "fail when the brokers cannot be reached" in {
      val (_, admin, writer) = fixture()
      val cause              = new RuntimeException("no broker")
      when(admin.describeCluster()).thenReturn(Future.failedFuture(cause))

      val result = writer.health()

      result.failed() shouldBe true
      result.cause() shouldBe cause
    }

    "not rely on the producer metadata cache" in {
      val (producer, admin, writer) = fixture()
      when(admin.describeCluster()).thenReturn(Future.succeededFuture(new ClusterDescription()))

      writer.health()

      verify(producer, never()).partitionsFor(any[String]())
    }
  }

  "close" should {
    "close the producer and the admin client" in {
      val (producer, admin, writer) = fixture()
      when(producer.close()).thenReturn(Future.succeededFuture[Void]())
      when(admin.close()).thenReturn(Future.succeededFuture[Void]())

      writer.close().succeeded() shouldBe true

      verify(producer).close()
      verify(admin).close()
    }

    "fail when one of them fails to close" in {
      val (producer, admin, writer) = fixture()
      val cause                     = new RuntimeException("stuck")
      when(producer.close()).thenReturn(Future.succeededFuture[Void]())
      when(admin.close()).thenReturn(Future.failedFuture[Void](cause))

      writer.close().failed() shouldBe true
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
