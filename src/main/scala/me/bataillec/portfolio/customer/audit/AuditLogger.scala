package me.bataillec.portfolio.customer.audit

import ch.qos.logback.classic.encoder.PatternLayoutEncoder
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.classic.{Level, LoggerContext}
import ch.qos.logback.core.rolling.{RollingFileAppender, TimeBasedRollingPolicy}
import org.slf4j.{Logger, LoggerFactory}

import java.nio.file.Paths

object AuditLogger {
  private val pattern        = "%d{yyyy-MM-dd HH:mm:ss.SSS} - %msg%n"
  private var logger: Logger = _

  def init(filepath: String): Unit = {
    if (logger == null) {
      val configuredPath = Paths.get(filepath).toAbsolutePath
      val dir            = configuredPath.getParent.toString
      val filename       = configuredPath.getFileName.toString

      val ctx = LoggerFactory.getILoggerFactory.asInstanceOf[LoggerContext]

      val encoder = new PatternLayoutEncoder()
      encoder.setContext(ctx)
      encoder.setPattern(pattern)
      encoder.start()

      val appender = new RollingFileAppender[ILoggingEvent]()
      appender.setContext(ctx)
      appender.setName("FILE")
      appender.setEncoder(encoder)
      appender.setFile(filepath) // /a/b/audit.log

      val policy = new TimeBasedRollingPolicy[ILoggingEvent]
      policy.setContext(ctx)
      policy.setParent(appender)
      policy.setFileNamePattern(dir.replace('\\', '/').replace("%", "\\%") + "/%d{yyyy-MM-dd}." + filename.replace("%", "\\%")) // /a/b/2026-12-31.audit.log
      policy.setMaxHistory(90)
      policy.start()

      appender.setRollingPolicy(policy)
      appender.start()

      val root = ctx.getLogger(getClass)
      root.setLevel(Level.INFO)
      root.setAdditive(false)
      root.addAppender(appender)

      logger = root
    }
  }

  def audit(msg: String): Unit = {
    if (logger != null) {
      logger.info(msg)
    } else {
      throw new Exception("Logger is not initialized")
    }
  }
}
