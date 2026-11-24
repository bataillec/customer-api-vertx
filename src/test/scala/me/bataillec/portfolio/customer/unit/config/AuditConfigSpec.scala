package me.bataillec.portfolio.customer.unit.config

import me.bataillec.portfolio.customer.config.AuditConfig
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class AuditConfigSpec extends AnyWordSpec with Matchers {
  "AuditConfig.fromEnv" should {
    "read Kafka, the topic and the outbox settings" in {
      val config = AuditConfig.fromEnv(
        Map(
          "KAFKA_BOOTSTRAP_SERVERS" -> "a:1,b:2",
          "AUDIT_TOPIC"             -> "my.topic",
          "OUTBOX_POLL_INTERVAL_MS" -> "50",
          "OUTBOX_BATCH_SIZE"       -> "10",
          "OUTBOX_RETENTION_DAYS"   -> "3"
        )
      )

      config.kafkaBootstrapServers shouldBe "a:1,b:2"
      config.auditTopic shouldBe "my.topic"
      config.outboxPollIntervalMs shouldBe 50
      config.outboxBatchSize shouldBe 10
      config.outboxRetentionDays shouldBe 3
    }

    "read the database like the customer verticles do" in {
      val config = AuditConfig.fromEnv(Map("PG_HOST" -> "db", "PG_PORT" -> "6543", "PG_DATABASE" -> "d", "PG_USER" -> "u", "PG_PASSWORD" -> "p"))

      config.pg.pgHost shouldBe "db"
      config.pg.pgPort shouldBe 6543
      config.pg.pgDatabase shouldBe "d"
      config.pg.pgUser shouldBe "u"
      config.pg.pgPassword shouldBe "p"
    }

    "use the defaults" in {
      val config = AuditConfig.fromEnv(Map.empty)

      config.kafkaBootstrapServers shouldBe "localhost:9092"
      config.auditTopic shouldBe "customer.events.v1"
      config.pg.pgHost shouldBe "localhost"
      config.outboxPollIntervalMs shouldBe 200
      config.outboxBatchSize shouldBe 100
      config.outboxRetentionDays shouldBe 7
    }

    "ignore an unrelated variable" in {
      AuditConfig.fromEnv(Map("TOTO" -> "tata")).auditTopic shouldBe "customer.events.v1"
    }

    "reject an outbox setting below 1" in {
      an[IllegalArgumentException] should be thrownBy AuditConfig.fromEnv(Map("OUTBOX_BATCH_SIZE" -> "0"))
      an[IllegalArgumentException] should be thrownBy AuditConfig.fromEnv(Map("OUTBOX_POLL_INTERVAL_MS" -> "-5"))
    }

    "reject a setting that is not a number" in {
      an[IllegalArgumentException] should be thrownBy AuditConfig.fromEnv(Map("OUTBOX_RETENTION_DAYS" -> "soon"))
    }
  }
}
