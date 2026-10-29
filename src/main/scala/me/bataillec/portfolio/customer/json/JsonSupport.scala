package me.bataillec.portfolio.customer.json

import com.fasterxml.jackson.annotation.JsonInclude
import tools.jackson.databind.PropertyNamingStrategies
import tools.jackson.databind.cfg.DateTimeFeature
import tools.jackson.databind.json.JsonMapper
import tools.jackson.module.scala.DefaultScalaModule

// Vert.x's own mapper (DatabindCodec.mapper()) is immutable with Jackson 3, so the Scala module
// can't be registered on it. The domain types go through this mapper instead.
object JsonSupport {
  private val mapper: JsonMapper = JsonMapper
    .builder()
    .addModule(DefaultScalaModule)
    .propertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE)
    .disable(DateTimeFeature.WRITE_DATES_AS_TIMESTAMPS)
    // NON_ABSENT omits both null and None (NON_NULL would still write "field": null for None)
    .changeDefaultPropertyInclusion(_.withValueInclusion(JsonInclude.Include.NON_ABSENT))
    .build()

  def decode[T](json: String, clazz: Class[T]): T = mapper.readValue(json, clazz)

  def encode(value: AnyRef): String = mapper.writeValueAsString(value)
}
