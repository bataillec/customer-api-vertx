package me.bataillec.portfolio.customer.domain

import me.bataillec.portfolio.customer.http.WithHttpResponse

case class ListCustomers(
    items: List[Customer],
    nextCursor: Option[String]
) extends WithHttpResponse
