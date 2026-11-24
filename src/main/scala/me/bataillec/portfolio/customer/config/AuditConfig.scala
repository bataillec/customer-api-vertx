package me.bataillec.portfolio.customer.config

import me.bataillec.portfolio.customer.Constants.*
import me.bataillec.portfolio.customer.config.ConfigUtils.MyMap

/** The audit verticle relays the outbox table to Kafka, so it needs the database as well as the broker.
  * `pg` is the same `PG_*` settings as the customer verticles.
  */
final case class AuditConfig(
    kafkaBootstrapServers: String,
    auditTopic: String,
    pg: CustomerConfig,
    outboxPollIntervalMs: Int,
    outboxBatchSize: Int,
    outboxRetentionDays: Int
)

object AuditConfig {
  def fromEnv(env: Map[String, String] = sys.env): AuditConfig = {
    AuditConfig(
      kafkaBootstrapServers = env.str(KAFKA_BOOTSTRAP_SERVERS, KAFKA_BOOTSTRAP_SERVERS_DEFAULT_VALUE),
      auditTopic = env.str(AUDIT_TOPIC, AUDIT_TOPIC_DEFAULT_VALUE),
      pg = CustomerConfig.fromEnv(env),
      outboxPollIntervalMs = positive(env, OUTBOX_POLL_INTERVAL_MS, OUTBOX_POLL_INTERVAL_MS_DEFAULT_VALUE),
      outboxBatchSize = positive(env, OUTBOX_BATCH_SIZE, OUTBOX_BATCH_SIZE_DEFAULT_VALUE),
      outboxRetentionDays = positive(env, OUTBOX_RETENTION_DAYS, OUTBOX_RETENTION_DAYS_DEFAULT_VALUE)
    )
  }

  private def positive(env: Map[String, String], key: String, default: Int): Int = {
    val value = env.int(key, default)
    if (value < 1) throw new IllegalArgumentException(s"$key must be at least 1, got $value")
    value
  }
}
