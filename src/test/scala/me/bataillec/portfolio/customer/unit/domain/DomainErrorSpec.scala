package me.bataillec.portfolio.customer.unit.domain

import me.bataillec.portfolio.customer.domain.CustomerStatus.SUSPENDED
import me.bataillec.portfolio.customer.domain.DomainError
import me.bataillec.portfolio.customer.domain.DomainError.*
import me.bataillec.portfolio.customer.domain.PlanType.ENTERPRISE
import org.scalatest.Inside.inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.util.UUID

class DomainErrorSpec extends AnyWordSpec with Matchers {
  private val fixedUUIDString = "ed7b2379-c08e-44d1-823c-90b5c4f028e8"
  private val fixedUUID       = UUID.fromString(fixedUUIDString)

  "InvalidCursor" in {
    val e = InvalidCursor("bad cursor")

    inside(e) { case e: DomainError =>
      e.getMessage shouldBe "Cursor 'bad cursor' is invalid"
      e.status shouldBe 400
      e.code shouldBe "INVALID_CURSOR"
      e.title shouldBe "Invalid cursor"
    }
  }

  "CustomerNotFound" in {
    val e = CustomerNotFound(fixedUUID)

    inside(e) { case e: DomainError =>
      e.getMessage shouldBe "Customer 'ed7b2379-c08e-44d1-823c-90b5c4f028e8' does not exist"
      e.status shouldBe 404
      e.code shouldBe "CUSTOMER_NOT_FOUND"
      e.title shouldBe "Customer not found"
    }
  }

  "EmailAlreadyUsed" in {
    val e = EmailAlreadyUsed("email@domain.com")

    inside(e) { case e: DomainError =>
      e.getMessage shouldBe "A customer with email 'email@domain.com' already exists"
      e.status shouldBe 409
      e.code shouldBe "EMAIL_ALREADY_USED"
      e.title shouldBe "Email already used"
    }
  }

  "QuotaExceeded" when {
    "retry" in {
      val e = QuotaExceeded("featureKey", Some(42L))

      inside(e) { case e: DomainError =>
        e.getMessage shouldBe "Quota for 'featureKey' is exhausted. Retry in 42 seconds"
        e.status shouldBe 429
        e.code shouldBe "QUOTA_EXCEEDED"
        e.title shouldBe "Quota exceeded"
      }
    }

    "no retry" in {
      val e = QuotaExceeded("featureKey", None)

      inside(e) { case e: DomainError =>
        e.getMessage shouldBe "Quota for 'featureKey' is exhausted"
        e.status shouldBe 429
        e.code shouldBe "QUOTA_EXCEEDED"
        e.title shouldBe "Quota exceeded"
      }
    }
  }

  "EntitlementNotFound" in {
    val e = EntitlementNotFound(fixedUUID, "featureKey")

    inside(e) { case e: DomainError =>
      e.getMessage shouldBe "Customer 'ed7b2379-c08e-44d1-823c-90b5c4f028e8' has no entitlement 'featureKey'"
      e.status shouldBe 404
      e.code shouldBe "ENTITLEMENT_NOT_FOUND"
      e.title shouldBe "Entitlement not found"
    }
  }

  "PlanNotConfigured" in {
    val e = PlanNotConfigured(ENTERPRISE)

    inside(e) { case e: DomainError =>
      e.getMessage shouldBe "No quotas are configured for plan 'ENTERPRISE'"
      e.status shouldBe 500
      e.code shouldBe "PLAN_NOT_CONFIGURED"
      e.title shouldBe "Plan not configured"
    }
  }

  "CustomerNotActive" in {
    val e = CustomerNotActive(fixedUUID, SUSPENDED)

    inside(e) { case e: DomainError =>
      e.getMessage shouldBe "Customer 'ed7b2379-c08e-44d1-823c-90b5c4f028e8' is SUSPENDED"
      e.status shouldBe 409
      e.code shouldBe "CUSTOMER_NOT_ACTIVE"
      e.title shouldBe "Customer not active"
    }
  }

  "PlanAlreadyActive" in {
    val e = PlanAlreadyActive(fixedUUID, ENTERPRISE)

    inside(e) { case e: DomainError =>
      e.getMessage shouldBe "Customer 'ed7b2379-c08e-44d1-823c-90b5c4f028e8' is already subscribed to plan 'ENTERPRISE'"
      e.status shouldBe 409
      e.code shouldBe "PLAN_ALREADY_ACTIVE"
      e.title shouldBe "Plan already active"
    }
  }

  "NoActiveSubscription" in {
    val e = NoActiveSubscription(fixedUUID)

    inside(e) { case e: DomainError =>
      e.getMessage shouldBe "Customer 'ed7b2379-c08e-44d1-823c-90b5c4f028e8' has no active subscription"
      e.status shouldBe 409
      e.code shouldBe "NO_ACTIVE_SUBSCRIPTION"
      e.title shouldBe "No active subscription"
    }
  }
}
