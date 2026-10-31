package me.bataillec.portfolio.customer.audit

import ch.qos.logback.classic.encoder.PatternLayoutEncoder
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.classic.util.LogbackMDCAdapter
import ch.qos.logback.classic.{Level, Logger, LoggerContext}
import ch.qos.logback.core.rolling.{RollingFileAppender, TimeBasedRollingPolicy}

import java.nio.file.Paths

class FileAuditWriter(filepath: String) extends AuditLogger {
  private val pattern = "%d{yyyy-MM-dd HH:mm:ss.SSS} - %msg%n"

  private val ctx = new LoggerContext()
  // A context built by hand has no MDC adapter, and every append then fails with a NullPointerException
  ctx.setMDCAdapter(new LogbackMDCAdapter())
  ctx.start()

  private val appender: RollingFileAppender[ILoggingEvent] = {
    val configuredPath = Paths.get(filepath).toAbsolutePath
    val dir            = configuredPath.getParent.toString
    val filename       = configuredPath.getFileName.toString
    val encoder        = new PatternLayoutEncoder()
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
    appender
  }

  private val logger: Logger = {
    val auditLogger = ctx.getLogger("audit")
    auditLogger.setLevel(Level.INFO)
    auditLogger.setAdditive(false)
    auditLogger.addAppender(appender)
    auditLogger
  }

  override def audit(msg: String): Unit = logger.info(msg)

  override def close(): Unit = ctx.stop()
}
