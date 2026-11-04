package me.bataillec.portfolio.customer.unit.domain

import me.bataillec.portfolio.customer.domain.PlanType
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks.*
import org.scalatest.prop.TableFor2
import org.scalatest.wordspec.AnyWordSpec

class PlanTypeSpec extends AnyWordSpec with Matchers {
  "PlanType" should {
    val list: TableFor2[String, PlanType] = Table(
      ("String", "PlanType"),
      ("FREE_TIER", PlanType.FREE_TIER),
      ("PRO", PlanType.PRO),
      ("ENTERPRISE", PlanType.ENTERPRISE)
    )

    "toString" when {
      forAll(list) { case (str, planType) =>
        s"$str toString" in {
          planType.toString shouldBe str
        }
      }
    }

    "fromString" when {
      forAll(list) { case (str, planType) =>
        s"$str fromString" in {
          PlanType.valueOf(str) shouldBe planType
        }
      }
    }
  }
}
