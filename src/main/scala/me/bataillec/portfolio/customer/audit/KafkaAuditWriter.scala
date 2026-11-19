package me.bataillec.portfolio.customer.audit

import io.vertx.core.json.JsonObject
import io.vertx.core.{Future, Vertx}
import io.vertx.kafka.admin.KafkaAdminClient
import io.vertx.kafka.client.producer.{KafkaProducer, KafkaProducerRecord}
import me.bataillec.portfolio.customer.config.AuditConfig

import scala.jdk.CollectionConverters.*

/** Writes each audit event, a JSON envelope, to a Kafka topic. The record key is the `customer_id` of the event. */
class KafkaAuditWriter(producer: KafkaProducer[String, String], admin: KafkaAdminClient, topic: String) extends AuditLogger {

  override def audit(msg: String): Future[Void] = {
    val key = JsonObject(msg).getString(KafkaAuditWriter.KEY_FIELD)
    producer.send(KafkaProducerRecord.create(topic, key, msg)).mapEmpty[Void]()
  }

  // The admin client asks a broker for the cluster description on every call and fails after HEALTH_TIMEOUT_MS when none
  // answers. producer.partitionsFor was not enough: the producer serves it from its cached metadata, so it kept
  // succeeding while the brokers were down.
  override def health(): Future[Unit] = admin.describeCluster().mapEmpty[Unit]()

  override def close(): Future[Void] = Future.join(producer.close(), admin.close()).mapEmpty[Void]()
}

object KafkaAuditWriter {
  private val KEY_FIELD = "customer_id"

  // Under HEALTH_PING_TIMEOUT_MS, so that the check fails by itself before the ping gives up
  private[audit] val HEALTH_TIMEOUT_MS = "3000"

  def apply(vertx: Vertx, config: AuditConfig): KafkaAuditWriter = {
    val producerConfig = Map(
      "bootstrap.servers"  -> config.kafkaBootstrapServers,
      "key.serializer"     -> "org.apache.kafka.common.serialization.StringSerializer",
      "value.serializer"   -> "org.apache.kafka.common.serialization.StringSerializer",
      "acks"               -> "all",
      "enable.idempotence" -> "true"
    )
    val adminConfig = Map(
      "bootstrap.servers"       -> config.kafkaBootstrapServers,
      "default.api.timeout.ms"  -> HEALTH_TIMEOUT_MS,
      "request.timeout.ms"      -> HEALTH_TIMEOUT_MS
    )
    new KafkaAuditWriter(
      KafkaProducer.create[String, String](vertx, producerConfig.asJava),
      KafkaAdminClient.create(vertx, adminConfig.asJava),
      config.auditTopic
    )
  }
}
