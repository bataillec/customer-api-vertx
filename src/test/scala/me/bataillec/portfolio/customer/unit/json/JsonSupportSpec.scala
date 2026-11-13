package me.bataillec.portfolio.customer.unit.json

import me.bataillec.portfolio.customer.domain.*
import me.bataillec.portfolio.customer.json.JsonSupport
import me.bataillec.portfolio.customer.unit.json.JsonSupportSpec.*
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import tools.jackson.core.JacksonException

import java.time.Instant
import java.util.UUID
import scala.compiletime.uninitialized
import scala.reflect.ClassTag

class JsonSupportSpec extends AnyWordSpec with Matchers {
  "JsonSupport" should {
    val instant          = Instant.parse("2026-11-03T10:00:00Z")
    val detailledInstant = Instant.parse("2026-11-03T10:00:00.123Z")
    val customerId       = UUID.fromString("ed7b2379-c08e-44d1-823c-90b5c4f028e8")
    val customer         = Customer(customerId, "email@domain.com", Some("company"), CustomerStatus.ACTIVE, instant, instant)

    // language=JSON
    val customerJson =
      """{"id":"ed7b2379-c08e-44d1-823c-90b5c4f028e8","email":"email@domain.com","company_name":"company","status":"ACTIVE","created_at":"2026-11-03T10:00:00Z","updated_at":"2026-11-03T10:00:00Z"}"""

    "encode" in {
      JsonSupport.encode(customer) shouldBe customerJson
      JsonSupport.encode(OptionalObject(Some("company"))) shouldBe """{"company_name":"company"}"""
      JsonSupport.encode(OptionalObject(None)) shouldBe """{}"""
      JsonSupport.encode(OptionalObject(null)) shouldBe """{}"""
      JsonSupport.encode(InstantObject(instant)) shouldBe """{"instant":"2026-11-03T10:00:00Z"}"""
      JsonSupport.encode(InstantObject(detailledInstant)) shouldBe """{"instant":"2026-11-03T10:00:00.123Z"}"""
      JsonSupport.encode(EnumObject(CustomerStatus.ACTIVE)) shouldBe """{"customer_status":"ACTIVE"}"""
      JsonSupport.encode(
        ListObject(List(OptionalObject(Some("company1")), OptionalObject(Some("company2"))))
      ) shouldBe """{"list":[{"company_name":"company1"},{"company_name":"company2"}]}"""
      JsonSupport.encode(ListObject(List.empty)) shouldBe """{"list":[]}"""
      JsonSupport.encode(UUIDObject(customerId)) shouldBe """{"id":"ed7b2379-c08e-44d1-823c-90b5c4f028e8"}"""
      JsonSupport.encode(LongAndBooleanObject(5000000000L, true)) shouldBe """{"long":5000000000,"bool":true}"""
      JsonSupport.encode(LongAndBooleanObject(-5000000000L, false)) shouldBe """{"long":-5000000000,"bool":false}"""
    }

    "decode" in {
      JsonSupport.decode(customerJson, classOf[Customer]) shouldBe customer
      JsonSupport.decode("""{"company_name":"company"}""", classOf[OptionalObject]) shouldBe OptionalObject(Some("company"))
      JsonSupport.decode("""{}""", classOf[OptionalObject]) shouldBe OptionalObject(None)
      JsonSupport.decode("""{"instant":"2026-11-03T10:00:00Z"}""", classOf[InstantObject]) shouldBe InstantObject(instant)
      JsonSupport.decode("""{"instant":"2026-11-03T10:00:00.123Z"}""", classOf[InstantObject]) shouldBe InstantObject(detailledInstant)
      JsonSupport.decode("""{"customer_status":"ACTIVE"}""", classOf[EnumObject]) shouldBe EnumObject(CustomerStatus.ACTIVE)
      JsonSupport.decode(
        """{"list":[{"company_name":"company1"},{"company_name":"company2"}]}""",
        classOf[ListObject]
      ) shouldBe ListObject(List(OptionalObject(Some("company1")), OptionalObject(Some("company2"))))
      JsonSupport.decode("""{"list":[]}""", classOf[ListObject]) shouldBe ListObject(List.empty)
      JsonSupport.decode("""{"id":"ed7b2379-c08e-44d1-823c-90b5c4f028e8"}""", classOf[UUIDObject]) shouldBe UUIDObject(customerId)
      JsonSupport.decode("""{"long":5000000000,"bool":true}""", classOf[LongAndBooleanObject]) shouldBe LongAndBooleanObject(5000000000L, true)
      JsonSupport.decode("""{"long":-5000000000,"bool":false}""", classOf[LongAndBooleanObject]) shouldBe LongAndBooleanObject(-5000000000L, false)
    }

    "decode error paths" in {
      def failing(json: String, clazz: Class[?]) = a[JacksonException] should be thrownBy JsonSupport.decode(json, clazz)

      failing("""{"email":""", classOf[CreateCustomer])                                      // truncated JSON
      failing("""{email: a@b.c}""", classOf[CreateCustomer])                                 // invalid syntax
      failing("", classOf[CreateCustomer])                                                   // empty body
      failing("   ", classOf[CreateCustomer])                                                // blank body
      failing("[]", classOf[CreateCustomer])                                                 // array instead of object
      failing("42", classOf[CreateCustomer])                                                 // number instead of object
      failing("""{"email":{"a":1}}""", classOf[CreateCustomer])                              // object instead of string
      failing("""{"plan_type":"GOLD"}""", classOf[CreateSubscription])                       // unknown enum value
      failing("""{"plan_type":"pro"}""", classOf[CreateSubscription])                        // lower-case enum value
      failing("""{"feature_key":"A","amount":"abc"}""", classOf[Consumption])                // non-numeric instead of number
      failing("""{"feature_key":"A","amount":true}""", classOf[Consumption])                 // boolean instead of number
      failing("""{"feature_key":"A","amount":99999999999999999999}""", classOf[Consumption]) // long overflow
      failing("""{"feature_key":"A"}""", classOf[Consumption])                               // primitives default to 0
      failing("""{"email":"a@b.c"} xyz""", classOf[CreateCustomer])                          // trailing tokens

      JsonSupport.decode("{}", classOf[CreateCustomer]) shouldBe CreateCustomer(null, None)               // missing String becomes null
      JsonSupport.decode("""{"feature_key":"A","amount":"5"}""", classOf[Consumption]).amount shouldBe 5L // scalar coercion is allowed
      JsonSupport.decode("""{"email":123}""", classOf[CreateCustomer]).email shouldBe "123"               // coerce a number into a String
      JsonSupport.decode("""{"feature_key":"A","amount":5.7}""", classOf[Consumption]).amount shouldBe 5L // truncate a float given for a Long

      JsonSupport
        .decode("""{"email":"a@b.c","email":"b@b.c"}""", classOf[CreateCustomer])
        .email shouldBe "b@b.c"                                         // duplicate keys are not detected for beans, so the last one wins
      JsonSupport.decode("null", classOf[CreateCustomer]) shouldBe null // readValue("null") returns null

    }

    "encode error paths" in {
      JsonSupport.encode(null) shouldBe "null" // write null as the JSON literal null

      //  a direct cycle fails instead of overflowing the stack
      val node = new Node
      node.next = node
      a[JacksonException] should be thrownBy JsonSupport.encode(node)
    }

    "round-trip (decode(encode(x)) == x)" in {
      def roundTrip[T <: AnyRef: ClassTag](value: T): T =
        JsonSupport.decode(JsonSupport.encode(value), summon[ClassTag[T]].runtimeClass.asInstanceOf[Class[T]])

      roundTrip(customer) shouldBe customer
      roundTrip(OptionalObject(Some("company"))) shouldBe OptionalObject(Some("company"))
      roundTrip(OptionalObject(None)) shouldBe OptionalObject(None)
      roundTrip(OptionalObject(null)) shouldBe OptionalObject(None) // beware
      roundTrip(InstantObject(instant)) shouldBe InstantObject(instant)
      roundTrip(InstantObject(detailledInstant)) shouldBe InstantObject(detailledInstant)
      roundTrip(EnumObject(CustomerStatus.ACTIVE)) shouldBe EnumObject(CustomerStatus.ACTIVE)
      roundTrip(ListObject(List(OptionalObject(Some("company1")), OptionalObject(Some("company2"))))) shouldBe ListObject(
        List(OptionalObject(Some("company1")), OptionalObject(Some("company2")))
      )
      roundTrip(ListObject(List.empty)) shouldBe ListObject(List.empty)
      roundTrip(UUIDObject(customerId)) shouldBe UUIDObject(customerId)
      roundTrip(LongAndBooleanObject(5000000000L, true)) shouldBe LongAndBooleanObject(5000000000L, true)
      roundTrip(LongAndBooleanObject(-5000000000L, false)) shouldBe LongAndBooleanObject(-5000000000L, false)
    }
  }
}

object JsonSupportSpec {
  final case class OptionalObject(companyName: Option[String])
  final case class InstantObject(instant: Instant)
  final case class EnumObject(customerStatus: CustomerStatus)
  final case class ListObject(list: List[OptionalObject])
  final case class UUIDObject(id: UUID)
  final case class LongAndBooleanObject(long: Long, bool: Boolean)

  // Only used by the "self-reference" encode test
  final class Node {
    var next: Node = uninitialized
  }
}
