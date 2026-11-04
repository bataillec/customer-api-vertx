package me.bataillec.portfolio.customer.unit.domain

import me.bataillec.portfolio.customer.domain.Consumption
import org.scalatest.Inside.inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class ConsumptionSpec extends AnyWordSpec with Matchers {
  "Consuption" should {
    "be built" in {
      val consumption = Consumption("featureKey", 1L)

      inside(consumption) { case Consumption(featureKey, amount) =>
        featureKey shouldBe "featureKey"
        amount shouldBe 1L
      }
    }
  }
}
