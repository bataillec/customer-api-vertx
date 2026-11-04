package me.bataillec.portfolio.customer.unit.domain

import io.vertx.sqlclient.Row
import me.bataillec.portfolio.customer.domain.Customer
import me.bataillec.portfolio.customer.domain.CustomerStatus.ACTIVE
import me.bataillec.portfolio.customer.http.HttpResponse
import org.mockito.Mockito.*
import org.scalatest.Inside.inside
import org.scalatest.OptionValues.*
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.time.{Clock, Instant, OffsetDateTime}
import java.util.UUID

class CustomerSpec extends AnyWordSpec with Matchers {
  "Customer" should {
    val fixedInstantString = "2026-11-03T10:00:00Z"
    val fixedInstant       = Instant.parse(fixedInstantString)
    val clock              = mock(classOf[Clock])
    when(clock.instant()).thenReturn(fixedInstant)

    val fixedUUIDString = "ed7b2379-c08e-44d1-823c-90b5c4f028e8"
    val fixedUUID       = UUID.fromString(fixedUUIDString)

    val customer = Customer(
      fixedUUID,
      "email@domain.com",
      Some("company"),
      ACTIVE,
      Instant.now(clock),
      Instant.now(clock)
    )

    "be built" in {

      inside(customer) { case Customer(id, email, companyName, status, createdAt, updatedAt) =>
        id.toString shouldBe "ed7b2379-c08e-44d1-823c-90b5c4f028e8"
        email shouldBe "email@domain.com"
        companyName.value shouldBe "company"
        status shouldBe ACTIVE
        createdAt.toString shouldBe "2026-11-03T10:00:00Z"
        updatedAt.toString shouldBe "2026-11-03T10:00:00Z"
      }
    }

    "become a HTTPResponse" in {
      val response = customer.toHttpResponse()

      inside(response) { case HttpResponse(statusCode, body, contentType, headers) =>
        statusCode shouldBe 200
        contentType shouldBe "application/json"
        headers shouldBe Map.empty
        body shouldBe """{"id":"ed7b2379-c08e-44d1-823c-90b5c4f028e8","email":"email@domain.com","company_name":"company","status":"ACTIVE","created_at":"2026-11-03T10:00:00Z","updated_at":"2026-11-03T10:00:00Z"}"""
      }
    }

    "be build from a Row" in {
      val row = mock(classOf[Row])

      when(row.getUUID("id")).thenReturn(fixedUUID)
      when(row.getString("email")).thenReturn("email@domain.com")
      when(row.getString("company_name")).thenReturn("company")
      when(row.getString("status")).thenReturn("ACTIVE")
      when(row.getOffsetDateTime("created_at")).thenReturn(OffsetDateTime.parse(fixedInstantString))
      when(row.getOffsetDateTime("updated_at")).thenReturn(OffsetDateTime.parse(fixedInstantString))

      val c = Customer.fromRow(row)

      c shouldBe customer
    }
  }
}
