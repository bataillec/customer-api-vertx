package me.bataillec.portfolio.customer.eventbus

import io.vertx.core.Future

trait CustomerHandler {
  def handle(): Future[Unit]
}
