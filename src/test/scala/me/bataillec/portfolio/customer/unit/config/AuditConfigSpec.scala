package me.bataillec.portfolio.customer.unit.config

import me.bataillec.portfolio.customer.config.AuditConfig
import me.bataillec.portfolio.customer.support.EnvironmentUtils
import org.scalatest.Inside.inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class AuditConfigSpec extends AnyWordSpec with Matchers {
  "AuditConfig" should {
    "be built" in {
      val config = AuditConfig("/my/path/to/custom.log")

      inside(config) { case AuditConfig(auditFilePath) =>
        auditFilePath shouldBe "/my/path/to/custom.log"
      }
    }

    "build fromEnv" when {
      "Use env" in {
        EnvironmentUtils.setEnv("AUDIT_FILE_PATH", "/my/path/to/custom.log")

        val config = AuditConfig.fromEnv()

        inside(config) { case AuditConfig(auditFilePath) =>
          auditFilePath shouldBe "/my/path/to/custom.log"
        }
      }

      "AUDIT_FILE_PATH is set" in {
        val map = Map("AUDIT_FILE_PATH" -> "/my/path/to/custom.log")

        val config = AuditConfig.fromEnv(map)

        inside(config) { case AuditConfig(auditFilePath) =>
          auditFilePath shouldBe "/my/path/to/custom.log"
        }
      }

      "use default" in {
        val config = AuditConfig.fromEnv(Map.empty)

        inside(config) { case AuditConfig(auditFilePath) =>
          auditFilePath shouldBe "./audit.log"
        }
      }

      "ignore unrelated var" in {
        val map = Map("TOTO" -> "tata")

        val config = AuditConfig.fromEnv(map)

        inside(config) { case AuditConfig(auditFilePath) =>
          auditFilePath shouldBe "./audit.log"
        }
      }
    }
  }
}
