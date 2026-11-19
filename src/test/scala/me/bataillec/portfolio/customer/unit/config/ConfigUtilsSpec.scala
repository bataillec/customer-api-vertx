package me.bataillec.portfolio.customer.unit.config

import me.bataillec.portfolio.customer.config.ConfigUtils.MyMap
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class ConfigUtilsSpec extends AnyWordSpec with Matchers {
  "MyMap extension" should {
    "get a string" when {
      "value is set" in {
        val map = Map(
          "key"       -> "value",
          "other_key" -> "other value",
          "last_key"  -> "42"
        )

        map.str("key", "default value") shouldBe "value"
      }

      "use default" in {
        val map = Map(
          "other_key" -> "other value",
          "last_key"  -> "42"
        )

        map.str("key", "default value") shouldBe "default value"
      }
    }

    "get a int" when {
      "value is set" in {
        val map = Map(
          "key"       -> "value",
          "other_key" -> "other value",
          "last_key"  -> "42"
        )

        map.int("last_key", 1) shouldBe 42
      }

      "use default" in {
        val map = Map(
          "key"       -> "value",
          "other_key" -> "other value"
        )

        map.int("last_key", 10) shouldBe 10
      }

      "value is not an integer" in {
        val e = the[IllegalArgumentException] thrownBy Map("port" -> "abc").int("port", 1)

        e.getMessage shouldBe "Invalid integer for port: 'abc'"
      }
    }
  }
}
