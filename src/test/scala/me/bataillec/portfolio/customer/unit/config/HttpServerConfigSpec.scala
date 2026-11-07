package me.bataillec.portfolio.customer.unit.config

import me.bataillec.portfolio.customer.config.HttpServerConfig
import me.bataillec.portfolio.customer.support.EnvironmentUtils
import org.scalatest.Inside.inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class HttpServerConfigSpec extends AnyWordSpec with Matchers {
  "HttpServerConfig" should {
    "be built" in {
      val config = HttpServerConfig(42)

      inside(config) { case HttpServerConfig(httpPort) =>
        httpPort shouldBe 42
      }
    }

    "build fromEnv" when {
      "Use env" in {
        EnvironmentUtils.setEnv("HTTP_PORT", "42")

        val config = HttpServerConfig.fromEnv()

        inside(config) { case HttpServerConfig(httpPort) =>
          httpPort shouldBe 42
        }
      }

      "HTTP_PORT is set" in {
        val map = Map(
          "HTTP_PORT" -> "42"
        )

        val config = HttpServerConfig.fromEnv(map)

        inside(config) { case HttpServerConfig(httpPort) =>
          httpPort shouldBe 42
        }
      }

      "use default" in {
        val config = HttpServerConfig.fromEnv(Map.empty)

        inside(config) { case HttpServerConfig(httpPort) =>
          httpPort shouldBe 8080
        }
      }
    }
  }
}
