package me.bataillec.portfolio.customer.e2e

import io.cucumber.scala.{EN, ScalaDsl}
import io.vertx.core.json.JsonObject
import me.bataillec.portfolio.customer.support.ApiResponse
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters.*

/** The steps about the platform itself: its health, and what happens when a part of it stops or hangs. */
class PlatformSteps extends ScalaDsl with EN with Matchers {
  import World.*

  // The names of the checks of /health/ready
  private val CheckOf = Map("customer" -> "customer-verticle", "audit" -> "audit-verticle")

  private def checks(ready: ApiResponse): Map[String, String] =
    ready.json.getJsonArray("checks").asScala.map(_.asInstanceOf[JsonObject]).map(c => c.getString("id") -> c.getString("status")).toMap

  private def stop(name: String): Unit = {
    stack.stop(name)
    stopped += name
  }

  Given("the platform is running with two customer services") { () =>
    stack.CustomerNodes.foreach(name => withClue(s"$name: ")(stack.isRunning(name) shouldBe true))
    api.get("/health/ready").status shouldBe 200
  }

  When("someone checks whether the platform is ready") { () =>
    last = Some(api.get("/health/ready"))
  }

  When("the customer service {string} is stopped") { (name: String) =>
    stop(name)
  }

  When("both customer services are stopped") { () =>
    stack.CustomerNodes.foreach(stop)
  }

  When("both customer services are started again") { () =>
    stack.CustomerNodes.foreach(stack.startAgain)
    stopped.clear()
  }

  When("the database stops responding") { () =>
    stack.pause("postgres")
    paused += "postgres"
  }

  When("the database responds again") { () =>
    stack.unpause("postgres")
    paused -= "postgres"
  }

  When("the audit broker stops responding") { () =>
    stack.pause("kafka")
    paused += "kafka"
  }

  When("the audit broker responds again") { () =>
    stack.unpause("kafka")
    paused -= "kafka"
  }

  Then("the platform is ready") { () =>
    withClue(s"/health/ready answered ${response.status} ${response.body}: ") {
      response.status shouldBe 200
      response.json.getString("status") shouldBe "UP"
    }
  }

  Then("the {word} service is reported as {word}") { (service: String, status: String) =>
    checks(response)(CheckOf(service)) shouldBe status
  }

  Then("the platform reports that it is alive") { () =>
    api.get("/health/live").status shouldBe 200
  }

  Then("the platform publishes its API description") { () =>
    val spec = api.get("/openapi.json")
    spec.status shouldBe 200
    spec.json.getString("openapi") should startWith("3.")
  }

  Then("plans can still be listed") { () =>
    // The cluster takes a moment to notice that a node left
    Waiting.until("the plans to be listed", seconds = 60)(api.get("/plans").status == 200)
  }

  Then("a new customer can still be registered") { () =>
    Waiting.until("a customer to be registered", seconds = 60)(api.createCustomer().status == 201)
  }

  Then("listing the plans is refused because the service is unavailable") { () =>
    Waiting.until("the plans to be refused as unavailable", seconds = 60) {
      val answer = api.get("/plans")
      answer.status == 503 && answer.code == "NO_HANDLERS"
    }
  }

  Then("the platform reports that it is not ready") { () =>
    Waiting.until("the platform to report it is not ready", seconds = 60)(api.get("/health/ready").status == 503)
    api.get("/health/live").status shouldBe 200
  }

  Then("the platform reports that it is not ready because the {word} service is down") { (service: String) =>
    Waiting.until(s"the $service service to be reported DOWN", seconds = 60) {
      val ready = api.get("/health/ready")
      ready.status == 503 && checks(ready)(CheckOf(service)) == "DOWN"
    }
    api.get("/health/live").status shouldBe 200
  }

  Then("the platform becomes ready again") { () =>
    Waiting.until("the platform to be ready", seconds = 180)(api.get("/health/ready").status == 200)
  }
}
