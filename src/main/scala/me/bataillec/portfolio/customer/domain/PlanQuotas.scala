package me.bataillec.portfolio.customer.domain

import me.bataillec.portfolio.customer.domain

import java.time.{Instant, ZoneOffset}

object PlanQuotas {
  val apiCalls    = "API_CALLS_MONTHLY"
  val storageSize = "STORAGE_GB"

  def forPlan(plan: PlanType): Map[String, Long] = {
    plan match {
      case PlanType.FREE_TIER  => Map(apiCalls -> 1_000L, storageSize -> 1L)
      case PlanType.PRO        => Map(apiCalls -> 100_000L, storageSize -> 50L)
      case PlanType.ENTERPRISE => Map(apiCalls -> 10_000_000L, storageSize -> 1_000L)
    }
  }

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
