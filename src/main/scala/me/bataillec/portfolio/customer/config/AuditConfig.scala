package me.bataillec.portfolio.customer.config

import me.bataillec.portfolio.customer.Constants.*
import me.bataillec.portfolio.customer.config.ConfigUtils.MyMap

final case class AuditConfig(
    kafkaBootstrapServers: String,
    auditTopic: String
)

object AuditConfig {
  def fromEnv(env: Map[String, String] = sys.env): AuditConfig = {
    AuditConfig(
      kafkaBootstrapServers = env.str(KAFKA_BOOTSTRAP_SERVERS, KAFKA_BOOTSTRAP_SERVERS_DEFAULT_VALUE),
      auditTopic = env.str(AUDIT_TOPIC, AUDIT_TOPIC_DEFAULT_VALUE)
    )
  }
}
