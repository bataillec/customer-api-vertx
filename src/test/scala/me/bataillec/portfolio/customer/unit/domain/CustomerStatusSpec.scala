package me.bataillec.portfolio.customer.unit.domain

import me.bataillec.portfolio.customer.domain.CustomerStatus
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks.*
import org.scalatest.prop.TableFor2
import org.scalatest.wordspec.AnyWordSpec

class CustomerStatusSpec extends AnyWordSpec with Matchers {
  "CustomerStatus" should {
    val list: TableFor2[String, CustomerStatus] = Table(
      ("String", "Status"),
      ("ACTIVE", CustomerStatus.ACTIVE),
      ("SUSPENDED", CustomerStatus.SUSPENDED),
      ("DELETED", CustomerStatus.DELETED)
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
          CustomerStatus.valueOf(str) shouldBe status
        }
      }
    }
  }
}
