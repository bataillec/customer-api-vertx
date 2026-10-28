package me.bataillec.portfolio.customer.domain

import io.vertx.sqlclient.Row
import me.bataillec.portfolio.customer.http.WithHttpResponse

import java.time.Instant

final case class Entitlement(
    featureKey: String,
    allocatedQuota: Long,
    consumedQuota: Long,
    resetDate: Instant
) extends WithHttpResponse

object Entitlement {
  def fromRow(r: Row): Entitlement = {
    Entitlement(
      featureKey = r.getString("feature_key"),
      allocatedQuota = r.getLong("allocated_quota"),
      consumedQuota = r.getLong("consumed_quota"),
      resetDate = r.getOffsetDateTime("reset_date").toInstant
    )
  }
}
