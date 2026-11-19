package me.bataillec.portfolio.customer.audit

import io.vertx.core.json.JsonObject
import io.vertx.core.{Future, Vertx}
import io.vertx.kafka.client.producer.{KafkaProducer, KafkaProducerRecord}
import me.bataillec.portfolio.customer.config.AuditConfig

import scala.jdk.CollectionConverters.*

/** Writes each audit event, a JSON envelope, to a Kafka topic. The record key is the `customer_id` of the event. */
class KafkaAuditWriter(producer: KafkaProducer[String, String], topic: String) extends AuditLogger {

  override def audit(msg: String): Future[Void] = {
    val key = JsonObject(msg).getString(KafkaAuditWriter.KEY_FIELD)
    producer.send(KafkaProducerRecord.create(topic, key, msg)).mapEmpty[Void]()
  }

  // A metadata request: it fails when the brokers cannot be reached, unlike a mere "pong"
  override def health(): Future[Unit] = producer.partitionsFor(topic).mapEmpty[Unit]()

  override def close(): Future[Void] = producer.close()
}

object KafkaAuditWriter {
  private val KEY_FIELD = "customer_id"

  def apply(vertx: Vertx, config: AuditConfig): KafkaAuditWriter = {
    val producerConfig = Map(
      "bootstrap.servers"  -> config.kafkaBootstrapServers,
      "key.serializer"     -> "org.apache.kafka.common.serialization.StringSerializer",
      "value.serializer"   -> "org.apache.kafka.common.serialization.StringSerializer",
      "acks"               -> "all",
      "enable.idempotence" -> "true"
    )
    new KafkaAuditWriter(KafkaProducer.create[String, String](vertx, producerConfig.asJava), config.auditTopic)
  }
}
