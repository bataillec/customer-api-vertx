package me.bataillec.portfolio.customer.unit.customer

import me.bataillec.portfolio.customer.customer.CustomerCursor.*
import me.bataillec.portfolio.customer.domain.Customer
import me.bataillec.portfolio.customer.domain.CustomerStatus.{ACTIVE, SUSPENDED}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.nio.charset.StandardCharsets.UTF_8
import java.time.Instant
import java.time.format.DateTimeParseException
import java.time.temporal.ChronoUnit.DAYS
import java.util.{Base64, UUID}

class CustomerCursorSpec extends AnyWordSpec with Matchers {
  "CustomerCursor" should {
    val fixedInstantString = "2026-11-03T10:00:00Z"
    val fixedInstant       = Instant.parse(fixedInstantString)
    val fixedUuidString    = "ed7b2379-c08e-44d1-823c-90b5c4f028e8"
    val fixedUuid          = UUID.fromString(fixedUuidString)
    val customer           = Customer(fixedUuid, "email@domain.com", Some("company"), ACTIVE, fixedInstant, fixedInstant)

    "encodeCursor" in {
      val cursor = encodeCursor(customer)

      cursor shouldBe Base64.getUrlEncoder.withoutPadding.encodeToString("2026-11-03T10:00:00Z|ed7b2379-c08e-44d1-823c-90b5c4f028e8".getBytes(UTF_8))
      new String(Base64.getUrlDecoder.decode(cursor), UTF_8) shouldBe "2026-11-03T10:00:00Z|ed7b2379-c08e-44d1-823c-90b5c4f028e8"
      cursor should fullyMatch regex "[A-Za-z0-9_-]+" // URL-safe
      cursor should not endWith "="                   // no padding

      val anotherCursor = encodeCursor(
        customer.copy(
          email = "another_email@domain.com",
          companyName = Some("Another Company"),
          status = SUSPENDED,
          updatedAt = fixedInstant.plus(1, DAYS)
        )
      )

      cursor shouldBe anotherCursor // only id & createdAt impact the cursor

      new String(
        Base64.getUrlDecoder.decode(encodeCursor(customer.copy(createdAt = fixedInstant.plusNanos(123456789)))),
        UTF_8
      ) shouldBe "2026-11-03T10:00:00.123456789Z|ed7b2379-c08e-44d1-823c-90b5c4f028e8"
    }

    "decodeCursor" in {
      val fixedCursor = "MjAyNi0xMS0wM1QxMDowMDowMFp8ZWQ3YjIzNzktYzA4ZS00NGQxLTgyM2MtOTBiNWM0ZjAyOGU4"

      decodeCursor(fixedCursor) shouldBe (fixedInstant, fixedUuid)

      an[IllegalArgumentException] shouldBe thrownBy(decodeCursor("!!!"))                                                        // not valid base64url
      a[MatchError] shouldBe thrownBy(decodeCursor(""))                                                                          // empty string
      a[MatchError] shouldBe thrownBy(decodeCursor(Base64.getUrlEncoder.withoutPadding.encodeToString("abc".getBytes(UTF_8))))   // No separator
      a[MatchError] shouldBe thrownBy(decodeCursor(Base64.getUrlEncoder.withoutPadding.encodeToString("a|b|c".getBytes(UTF_8)))) // too many parts
      a[DateTimeParseException] shouldBe thrownBy(
        decodeCursor(Base64.getUrlEncoder.withoutPadding.encodeToString("not-a-date|ed7b2379-c08e-44d1-823c-90b5c4f028e8".getBytes(UTF_8)))
      )                                                                                                                          // not a date
      a[IllegalArgumentException] shouldBe thrownBy(
        decodeCursor(Base64.getUrlEncoder.withoutPadding.encodeToString("2026-11-03T10:00:00Z|not-a-uuid".getBytes(UTF_8)))
      )                                                                                                                          // not an uuid
    }

    "round-trip" in {
      decodeCursor(encodeCursor(customer)) shouldBe (fixedInstant, fixedUuid)
      decodeCursor(encodeCursor(customer.copy(createdAt = fixedInstant.plusNanos(123456789)))) shouldBe (fixedInstant.plusNanos(123456789), fixedUuid)
    }
  }
}
