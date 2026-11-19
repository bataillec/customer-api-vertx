package me.bataillec.portfolio.customer.config

object ConfigUtils {

  implicit class MyMap(map: Map[String, String]) {
    def str(key: String, default: String): String = {
      map.getOrElse(key, default)
    }

    def int(key: String, default: Int): Int = {
      map.get(key).map { v =>
        v.trim.toIntOption.getOrElse(throw new IllegalArgumentException(s"Invalid integer for $key: '$v'"))
      }.getOrElse(default)
    }
  }
}
