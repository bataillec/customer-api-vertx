package me.bataillec.portfolio.customer.config

object ConfigUtils {

  implicit class MyMap(map: Map[String, String]) {
    def str(key: String, default: String): String = {
      map.getOrElse(key, default)
    }

    def int(key: String, default: Int): Int = {
      map.get(key).map(_.toInt).getOrElse(default)
    }
  }
}
