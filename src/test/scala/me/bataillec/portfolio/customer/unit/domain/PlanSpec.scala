package me.bataillec.portfolio.customer.unit.domain

import me.bataillec.portfolio.customer.domain.PlanType.ENTERPRISE
import me.bataillec.portfolio.customer.domain.{Plan, PlanQuota}
import org.scalatest.Inside.inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.collection.immutable.ListMap

class PlanSpec extends AnyWordSpec with Matchers {
  "Plan" should {
    "be built" in {
      val plan = Plan(ENTERPRISE, ListMap("feature1" -> PlanQuota(1L, true), "feature2" -> PlanQuota(10L, false)))

      inside(plan) { case Plan(planType, quotas) =>
        planType shouldBe ENTERPRISE
        quotas should have size 2
        quotas should contain allOf ("feature2" -> PlanQuota(10L, false), "feature1" -> PlanQuota(1L, true))
      }
    }
  }
}
