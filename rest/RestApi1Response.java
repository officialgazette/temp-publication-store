package com.systemfive.archive.rest;

import com.systemfive.archive.entity.ContentType;
import org.rapidoid.http.HttpStatus;
import org.rapidoid.http.MediaType;
import org.rapidoid.net.abstracts.Channel;

public class RestApi1Response {

  private final HttpServer http;
  private final Channel ctx;
  private final boolean isKeepAlive;

  public RestApi1Response( HttpServer http, Channel ctx, boolean isKeepAlive ) {
    this.http = http;
    this.ctx = ctx;
    this.isKeepAlive = isKeepAlive;
  }

  public HttpStatus error( int code, String message ) {
    return http.response( ctx, code, isKeepAlive, ( "{\"error\":\"" + message + "\"}\n" ).getBytes(), MediaType.JSON );
  }

  public HttpStatus message(String message) {
    return http.response( ctx, 200, isKeepAlive, ( "{\"message\":\"" + message + "\"}\n" ).getBytes(), MediaType.JSON );
  }

  public HttpStatus json( byte[] json ) {
    return http.response( ctx, 200, isKeepAlive, json, MediaType.JSON );
  }

  public HttpStatus content( byte[] data, ContentType contentType ) {
    return content( data, contentType, false );
  }

  public HttpStatus content( byte[] data, ContentType contentType, boolean isGzip ) {
    return http.response( ctx, 200, isKeepAlive, data, MediaType.of( contentType.type ), isGzip );
  }
  
}