package me.bataillec.portfolio.customer.domain

final case class CreateCustomer(
    email: String,
    companyName: Option[String]
)
