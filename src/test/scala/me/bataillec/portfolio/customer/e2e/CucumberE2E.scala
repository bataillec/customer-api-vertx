package me.bataillec.portfolio.customer.e2e

import org.junit.platform.suite.api.{IncludeEngines, SelectClasspathResource, Suite}

/** Runs the Gherkin scenarios of `src/test/resources/features` against the packaged platform.
  * Only `mvn verify -Pe2e` runs it. The glue package and the reports are set in `junit-platform.properties`.
  */
@Suite
@IncludeEngines(Array("cucumber"))
@SelectClasspathResource("features")
class CucumberE2E
