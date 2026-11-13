package me.bataillec.portfolio.customer.unit.config

import me.bataillec.portfolio.customer.config.AuditConfig
import me.bataillec.portfolio.customer.support.EnvironmentUtils
import org.scalatest.Inside.inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class AuditConfigSpec extends AnyWordSpec with Matchers {
  "AuditConfig" should {
    "be built" in {
      val config = AuditConfig("kafka:9092", "my.topic")

      inside(config) { case AuditConfig(servers, topic) =>
        servers shouldBe "kafka:9092"
        topic shouldBe "my.topic"
      }
    }

    "build fromEnv" when {
      "Use env" in {
        EnvironmentUtils.setEnv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
        EnvironmentUtils.setEnv("AUDIT_TOPIC", "my.topic")

        val config = AuditConfig.fromEnv()

        inside(config) { case AuditConfig(servers, topic) =>
          servers shouldBe "kafka:9092"
          topic shouldBe "my.topic"
        }
      }

      "KAFKA_BOOTSTRAP_SERVERS and AUDIT_TOPIC are set" in {
        val map = Map("KAFKA_BOOTSTRAP_SERVERS" -> "a:1,b:2", "AUDIT_TOPIC" -> "my.topic")

        val config = AuditConfig.fromEnv(map)

        inside(config) { case AuditConfig(servers, topic) =>
          servers shouldBe "a:1,b:2"
          topic shouldBe "my.topic"
        }
      }

      "use default" in {
        val config = AuditConfig.fromEnv(Map.empty)

        inside(config) { case AuditConfig(servers, topic) =>
          servers shouldBe "localhost:9092"
          topic shouldBe "customer.events.v1"
        }
      }

      "ignore unrelated var" in {
        val config = AuditConfig.fromEnv(Map("TOTO" -> "tata"))

        inside(config) { case AuditConfig(servers, topic) =>
          servers shouldBe "localhost:9092"
          topic shouldBe "customer.events.v1"
        }
      }
    }
  }
}
