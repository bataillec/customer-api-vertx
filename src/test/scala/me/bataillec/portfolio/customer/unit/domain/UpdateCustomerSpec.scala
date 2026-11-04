package me.bataillec.portfolio.customer.unit.domain

import me.bataillec.portfolio.customer.domain.CustomerStatus.ACTIVE
import me.bataillec.portfolio.customer.domain.UpdateCustomer
import org.scalatest.Inside.inside
import org.scalatest.OptionValues.*
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class UpdateCustomerSpec extends AnyWordSpec with Matchers {
  "UpdateCustomer" should {
    "be built" when {
      "full" in {
        val updateCustomer = UpdateCustomer(Some("new company"), Some(ACTIVE))

        inside(updateCustomer) { case UpdateCustomer(companyName, status) =>
          companyName.value shouldBe "new company"
          status.value shouldBe ACTIVE
        }
      }

      "minimal" in {
        val updateCustomer = UpdateCustomer(None, None)

        inside(updateCustomer) { case UpdateCustomer(companyName, status) =>
          companyName shouldBe empty
          status shouldBe empty
        }
      }
    }
  }
}
