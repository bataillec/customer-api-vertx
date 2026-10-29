package me.bataillec.portfolio.customer.domain

import me.bataillec.portfolio.customer.domain

import java.time.{Instant, ZoneOffset}

object PlanQuotas {
  val apiCalls    = "API_CALLS_MONTHLY"
  val storageSize = "STORAGE_GB"

  def nextResetDate(now: Instant): Instant = {
    now
      .atZone(ZoneOffset.UTC)
      .toLocalDate
      .withDayOfMonth(1)
      .plusMonths(1)
      .atStartOfDay(ZoneOffset.UTC)
      .toInstant
  }
}
