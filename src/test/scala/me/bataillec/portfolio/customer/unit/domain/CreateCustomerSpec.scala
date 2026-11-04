package me.bataillec.portfolio.customer.unit.domain

import me.bataillec.portfolio.customer.domain.CreateCustomer
import org.scalatest.Inside.inside
import org.scalatest.OptionValues.*
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class CreateCustomerSpec extends AnyWordSpec with Matchers {
  "CreateCustomer" should {
    "be built (full)" in {
      val createCustomer = CreateCustomer("email@domain.com", Some("company"))

      inside(createCustomer) { case CreateCustomer(email, companyName) =>
        email shouldBe "email@domain.com"
        companyName.value shouldBe "company"
      }
    }

    "be built (minimal)" in {
      val createCustomer = CreateCustomer("email@domain.com", None)

      inside(createCustomer) { case CreateCustomer(email, companyName) =>
        email shouldBe "email@domain.com"
        companyName shouldBe empty
      }
    }
  }
}
