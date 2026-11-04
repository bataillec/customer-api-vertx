package me.bataillec.portfolio.customer.unit.domain

import me.bataillec.portfolio.customer.domain.PlanType.ENTERPRISE
import me.bataillec.portfolio.customer.domain.{CreateSubscription, PlanType}
import org.scalatest.Inside.inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class CreateSubscriptionSpec extends AnyWordSpec with Matchers {
  "CreateSubscription" should {
    "be built" in {
      val createSubscription = CreateSubscription(ENTERPRISE)

      inside(createSubscription) { case CreateSubscription(planType) =>
        planType shouldBe ENTERPRISE
      }
    }
  }
}
