package me.bataillec.portfolio.customer.unit.domain

import me.bataillec.portfolio.customer.domain.CustomerStatus.ACTIVE
import me.bataillec.portfolio.customer.domain.{Customer, ListCustomers}
import me.bataillec.portfolio.customer.http.HttpResponse
import org.mockito.Mockito.*
import org.scalatest.Inside.inside
import org.scalatest.OptionValues.*
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.time.{Clock, Instant}
import java.util.UUID

class ListCustomersSpec extends AnyWordSpec with Matchers {
  "ListCustomers" should {
    val fixedInstantString = "2026-11-03T10:00:00Z"
    val fixedInstant       = Instant.parse(fixedInstantString)
    val clock              = mock(classOf[Clock])
    when(clock.instant()).thenReturn(fixedInstant)

    val fixedUUIDString1 = "ed7b2379-c08e-44d1-823c-90b5c4f028e8"
    val fixedUUID1       = UUID.fromString(fixedUUIDString1)

    val fixedUUIDString2 = "ed7b2379-c08e-44d1-823c-90b5c4f028e9"
    val fixedUUID2       = UUID.fromString(fixedUUIDString2)

    val c1 = Customer(
      fixedUUID1,
      "email1@domain.com",
      Some("company 1"),
      ACTIVE,
      Instant.now(clock),
      Instant.now(clock)
    )

    val c2 = Customer(
      fixedUUID2,
      "email2@domain.com",
      Some("company 2"),
      ACTIVE,
      Instant.now(clock),
      Instant.now(clock)
    )

    "be built" when {
      "be built (full)" in {
        val listCustomers = ListCustomers(
          items = List(c1, c2),
          nextCursor = Some("cursor")
        )

        inside(listCustomers) { case ListCustomers(items, nextCursor) =>
          items should have size 2
          items should contain allOf (c1, c2)
          nextCursor.value shouldBe "cursor"
        }
      }

      "be build (no cursor)" in {
        val listCustomers = ListCustomers(
          items = List(c1, c2),
          nextCursor = None
        )

        inside(listCustomers) { case ListCustomers(items, nextCursor) =>
          items should have size 2
          items should contain allOf (c1, c2)
          nextCursor shouldBe empty
        }
      }

      "be built (no cursor, no customer" in {
        val listCustomers = ListCustomers(
          items = List.empty,
          nextCursor = None
        )

        inside(listCustomers) { case ListCustomers(items, nextCursor) =>
          items should have size 0
          nextCursor shouldBe empty
        }
      }
    }

    "become a HTTPResponse" in {
      val listCustomers = ListCustomers(
        items = List(c1, c2),
        nextCursor = Some("cursor")
      )

      val response = listCustomers.toHttpResponse()

      inside(response) { case HttpResponse(statusCode, body, contentType, headers) =>
        statusCode shouldBe 200
        contentType shouldBe "application/json"
        headers shouldBe Map.empty
        body shouldBe """{"items":[{"id":"ed7b2379-c08e-44d1-823c-90b5c4f028e8","email":"email1@domain.com","company_name":"company 1","status":"ACTIVE","created_at":"2026-11-03T10:00:00Z","updated_at":"2026-11-03T10:00:00Z"},{"id":"ed7b2379-c08e-44d1-823c-90b5c4f028e9","email":"email2@domain.com","company_name":"company 2","status":"ACTIVE","created_at":"2026-11-03T10:00:00Z","updated_at":"2026-11-03T10:00:00Z"}],"next_cursor":"cursor"}"""
      }
    }
  }
}
