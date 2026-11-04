package me.bataillec.portfolio.customer.unit.domain

import me.bataillec.portfolio.customer.domain.SubscriptionStatus
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks.*
import org.scalatest.prop.TableFor2
import org.scalatest.wordspec.AnyWordSpec

class SubscriptionStatusSpec extends AnyWordSpec with Matchers {
  "SubscriptionStatus" should {
    val list: TableFor2[String, SubscriptionStatus] = Table(
      ("String", "Status"),
      ("ACTIVE", SubscriptionStatus.ACTIVE),
      ("CANCELED", SubscriptionStatus.CANCELED),
      ("EXPIRED", SubscriptionStatus.EXPIRED)
    )

    "toString" when {
      forAll(list) { case (str, status) =>
        s"$str toString" in {
          status.toString shouldBe str
        }
      }
    }

    "fromString" when {
      forAll(list) { case (str, status) =>
        s"$str fromString" in {
          SubscriptionStatus.valueOf(str) shouldBe status
        }
      }
    }
  }
}
