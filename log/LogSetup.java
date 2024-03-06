package com.systemfive.archive.log;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.PatternLayout;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.ConsoleAppender;
import ch.qos.logback.core.encoder.LayoutWrappingEncoder;
import org.rapidoid.log.LogOptions;
import org.slf4j.LoggerFactory;

public class LogSetup {

  public static void init() {
    LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
    JoranConfigurator configurator = new JoranConfigurator();
    configurator.setContext(context);
    context.reset();
    ConsoleAppender<ILoggingEvent> ca = new ConsoleAppender<>();
    ca.setContext( context );
    ca.setName( "console" );
    LayoutWrappingEncoder<ILoggingEvent> encoder = new LayoutWrappingEncoder<>();
    encoder.setContext( context );
    PatternLayout layout = new PatternLayout();
    layout.setPattern( "%d{YYYY-MM-dd HH:mm:ss.SSS} %-5level [%thread] %logger: %msg%n" );
    layout.setContext( context );
    layout.start();
    encoder.setLayout( layout );
    ca.setEncoder( encoder );
    Filter filter = new Filter();
    filter.start();
    ca.addFilter( filter );
    ca.start();
    Logger rootLogger = context.getLogger( Logger.ROOT_LOGGER_NAME );
    rootLogger.addAppender( ca );
    LogOptions options = org.rapidoid.log.Log.options();
    options.fancy( false );
  }

}