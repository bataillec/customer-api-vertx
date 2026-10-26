package me.bataillec.portfolio.customer.domain

final case class ConsumeResult(
    featureKey: String,
    allocatedQuota: Long,
    consumedQuota: Long,
    remainingQuota: Long
)
