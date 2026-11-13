package me.bataillec.portfolio.customer.unit.http

import io.vertx.core.buffer.Buffer
import io.vertx.core.json.JsonObject
import io.vertx.openapi.validation.{ValidationContext, ValidatorException}
import me.bataillec.portfolio.customer.Constants.PROBLEM_CONTENT_TYPE
import me.bataillec.portfolio.customer.http.ApiRoutes
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.jdk.CollectionConverters.*

class ProblemMediaTypeSpec extends AnyWordSpec with Matchers {
  private val problem = """{"title":"Customer not found","status":404,"code":"CUSTOMER_NOT_FOUND"}"""

  private def analyser(body: String, context: ValidationContext = ValidationContext.RESPONSE) =
    ApiRoutes.problemMediaType.createContentAnalyser(PROBLEM_CONTENT_TYPE, Buffer.buffer(body), context)

  "problemMediaType" should {
    "handle only application/problem+json" in {
      ApiRoutes.problemMediaType.canHandle(PROBLEM_CONTENT_TYPE) shouldBe true
      ApiRoutes.problemMediaType.canHandle("application/json") shouldBe false
      ApiRoutes.problemMediaType.canHandle("text/plain") shouldBe false
    }

    "declare application/problem+json as its only supported type" in {
      ApiRoutes.problemMediaType.supportedTypes().asScala should contain only PROBLEM_CONTENT_TYPE
    }

    "parse a valid body like application/json" in {
      val a = analyser(problem)

      noException should be thrownBy a.checkSyntacticalCorrectness()
      a.transform() shouldBe JsonObject(problem)
    }

    "parse a valid body in a request context too" in {
      val a = analyser(problem, ValidationContext.REQUEST)

      noException should be thrownBy a.checkSyntacticalCorrectness()
      a.transform() shouldBe JsonObject(problem)
    }

    "reject a malformed body" in {
      a[ValidatorException] should be thrownBy analyser("""{"title":""").checkSyntacticalCorrectness()
    }
  }
}
