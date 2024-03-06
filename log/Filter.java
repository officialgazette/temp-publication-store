package com.systemfive.archive.log;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.spi.FilterReply;

public class Filter extends ch.qos.logback.core.filter.Filter<ILoggingEvent> {

  @Override
  public FilterReply decide( ILoggingEvent event ) {
    if (event.getLoggerName().equals( "org.rapidoid.config.RapidoidInitializer" ))
      return FilterReply.DENY;
    if (event.getLoggerName().equals( "com.systemfive.archive.rest.HttpServer" ) && event.getMessage().equals( "Got request: GET /api/v1/admin/ping" ))
      return FilterReply.DENY;
    return FilterReply.ACCEPT;
  }

}