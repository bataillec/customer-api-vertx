package me.bataillec.portfolio.customer.domain

import io.vertx.sqlclient.Row

import java.time.{Instant, ZoneOffset}

final case class PlanQuota(
    quota: Long,
    resetsMonthly: Boolean
)

object PlanQuota {
  def nextResetDate(now: Instant): Instant = {
    now
      .atZone(ZoneOffset.UTC)
      .toLocalDate
      .withDayOfMonth(1)
      .plusMonths(1)
      .atStartOfDay(ZoneOffset.UTC)
      .toInstant
  }

  def fromRow(r: Row): PlanQuota =
    PlanQuota(
      quota = r.getLong("quota"),
      resetsMonthly = r.getBoolean("resets_monthly")
    )
}
