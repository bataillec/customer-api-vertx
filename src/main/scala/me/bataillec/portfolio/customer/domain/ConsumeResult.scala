package me.bataillec.portfolio.customer.domain

import io.vertx.sqlclient.Row
import me.bataillec.portfolio.customer.http.WithHttpResponse

final case class ConsumeResult(
    featureKey: String,
    allocatedQuota: Long,
    consumedQuota: Long,
    remainingQuota: Long
) extends WithHttpResponse

object ConsumeResult {
  def fromRow(r: Row): ConsumeResult = {
    val allocatedQuota = r.getLong("allocated_quota")
    val consumedQuota  = r.getLong("consumed_quota")
    ConsumeResult(
      featureKey = r.getString("feature_key"),
      allocatedQuota = allocatedQuota,
      consumedQuota = consumedQuota,
      remainingQuota = allocatedQuota - consumedQuota
    )
  }
}
