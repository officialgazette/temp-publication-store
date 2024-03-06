package com.systemfive.archive.rest;

import org.rapidoid.buffer.Buf;
import org.rapidoid.http.HttpStatus;
import org.rapidoid.net.abstracts.Channel;
import org.rapidoid.net.impl.RapidoidHelper;

public interface RestApi {

  public HttpStatus handle( HttpServer http, Channel ctx, Buf buf, RapidoidHelper req, String[] path, int index );

}