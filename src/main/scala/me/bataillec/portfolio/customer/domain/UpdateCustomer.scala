package me.bataillec.portfolio.customer.domain

final case class UpdateCustomer(
    companyName: Option[String],
    status: Option[CustomerStatus]
)
