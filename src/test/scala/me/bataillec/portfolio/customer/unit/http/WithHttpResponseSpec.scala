package me.bataillec.portfolio.customer.unit.http

import me.bataillec.portfolio.customer.http.{HttpResponse, WithHttpResponse}
import me.bataillec.portfolio.customer.unit.http.WithHttpResponseSpec.SimpleClass
import org.scalatest.Inside.inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class WithHttpResponseSpec extends AnyWordSpec with Matchers {
  "WithHttpResponse" should {
    val c = SimpleClass("param")

    "build a HTTPResponse with default values" in {
      inside(c.toHttpResponse()) { case HttpResponse(statusCode, body, contentType, headers) =>
        statusCode shouldBe 200
        body shouldBe """{"param":"param"}"""
        contentType shouldBe "application/json"
        headers shouldBe empty
      }
    }

    "build a HTTPResponse with the given values" in {
      inside(c.toHttpResponse(400, "text/plain", Map("header1" -> "value1"))) { case HttpResponse(statusCode, body, contentType, headers) =>
        statusCode shouldBe 400
        body shouldBe """{"param":"param"}"""
        contentType shouldBe "text/plain"
        headers should have size 1
        headers should contain("header1" -> "value1")
      }
    }
  }
}

object WithHttpResponseSpec {
  private final case class SimpleClass(param: String) extends WithHttpResponse
}
