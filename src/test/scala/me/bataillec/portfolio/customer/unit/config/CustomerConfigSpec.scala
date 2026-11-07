package me.bataillec.portfolio.customer.unit.config

import me.bataillec.portfolio.customer.config.CustomerConfig
import me.bataillec.portfolio.customer.support.EnvironmentUtils
import org.scalatest.Inside.inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class CustomerConfigSpec extends AnyWordSpec with Matchers {
  "CustomerConfig" should {
    "be built" in {
      val config = CustomerConfig(
        pgHost = "pgHost",
        pgPort = 42,
        pgDatabase = "pgDatabase",
        pgUser = "pgUser",
        pgPassword = "pgPassword",
        pgPoolSize = 10,
        jdbcUrl = "jdbcUrl"
      )

      inside(config) { case CustomerConfig(pgHost, pgPort, pgDatabase, pgUser, pgPassword, pgPoolSize, jdbcUrl) =>
        pgHost shouldBe "pgHost"
        pgPort shouldBe 42
        pgDatabase shouldBe "pgDatabase"
        pgUser shouldBe "pgUser"
        pgPassword shouldBe "pgPassword"
        pgPoolSize shouldBe 10
        jdbcUrl shouldBe "jdbcUrl"
      }
    }

    "build fromEnv" when {
      "Use env" in {
        EnvironmentUtils.setEnv("PG_HOST", "pgHost")
        EnvironmentUtils.setEnv("PG_PORT", "42")
        EnvironmentUtils.setEnv("PG_DATABASE", "pgDatabase")
        EnvironmentUtils.setEnv("PG_USER", "pgUser")
        EnvironmentUtils.setEnv("PG_PASSWORD", "pgPassword")
        EnvironmentUtils.setEnv("PG_POOL_SIZE", "10")

        val config = CustomerConfig.fromEnv()

        inside(config) { case CustomerConfig(pgHost, pgPort, pgDatabase, pgUser, pgPassword, pgPoolSize, jdbcUrl) =>
          pgHost shouldBe "pgHost"
          pgPort shouldBe 42
          pgDatabase shouldBe "pgDatabase"
          pgUser shouldBe "pgUser"
          pgPassword shouldBe "pgPassword"
          pgPoolSize shouldBe 10
          jdbcUrl shouldBe "jdbc:postgresql://pgHost:42/pgDatabase"
        }
      }

      "all env varaibles are set" in {
        val map = Map(
          "PG_HOST"      -> "pgHost",
          "PG_PORT"      -> "42",
          "PG_DATABASE"  -> "pgDatabase",
          "PG_USER"      -> "pgUser",
          "PG_PASSWORD"  -> "pgPassword",
          "PG_POOL_SIZE" -> "10"
        )

        val config = CustomerConfig.fromEnv(map)

        inside(config) { case CustomerConfig(pgHost, pgPort, pgDatabase, pgUser, pgPassword, pgPoolSize, jdbcUrl) =>
          pgHost shouldBe "pgHost"
          pgPort shouldBe 42
          pgDatabase shouldBe "pgDatabase"
          pgUser shouldBe "pgUser"
          pgPassword shouldBe "pgPassword"
          pgPoolSize shouldBe 10
          jdbcUrl shouldBe "jdbc:postgresql://pgHost:42/pgDatabase"
        }
      }

      "use default" in {
        val config = CustomerConfig.fromEnv(Map.empty)

        inside(config) { case CustomerConfig(pgHost, pgPort, pgDatabase, pgUser, pgPassword, pgPoolSize, jdbcUrl) =>
          pgHost shouldBe "localhost"
          pgPort shouldBe 5432
          pgDatabase shouldBe "customers"
          pgUser shouldBe "customers"
          pgPassword shouldBe "customers"
          pgPoolSize shouldBe 16
          jdbcUrl shouldBe "jdbc:postgresql://localhost:5432/customers"
        }
      }
    }
  }
}
