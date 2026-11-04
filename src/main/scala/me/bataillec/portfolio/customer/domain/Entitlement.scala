package me.bataillec.portfolio.customer.domain

import io.vertx.sqlclient.Row
import me.bataillec.portfolio.customer.http.WithHttpResponse

import java.time.Instant

final case class Entitlement(
    featureKey: String,
    allocatedQuota: Long,
    consumedQuota: Long,
    resetDate: Option[Instant] // None: the usage never resets
) extends WithHttpResponse {

  /** The values as of `now`: once `resetDate` has passed, a new period starts even if nothing has been consumed yet. */
  def effective(now: Instant): Entitlement =
    resetDate match {
      case Some(date) if !date.isAfter(now) => copy(consumedQuota = 0, resetDate = Some(PlanQuota.nextResetDate(now)))
      case _                                => this
    }
}

object Entitlement {
  def fromRow(r: Row): Entitlement = {
    Entitlement(
      featureKey = r.getString("feature_key"),
      allocatedQuota = r.getLong("allocated_quota"),
      consumedQuota = r.getLong("consumed_quota"),
      resetDate = Option(r.getOffsetDateTime("reset_date")).map(_.toInstant)
    )
  }
}
