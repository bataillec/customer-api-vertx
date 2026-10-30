package me.bataillec.portfolio.customer.domain

import scala.collection.immutable.ListMap

final case class PlanQuota(
    quota: Long,
    resetsMonthly: Boolean
)

final case class Plan(
    planType: PlanType,
    quotas: ListMap[String, PlanQuota]
)
