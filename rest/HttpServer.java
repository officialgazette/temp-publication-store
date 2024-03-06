package com.systemfive.archive.rest;

import com.systemfive.archive.config.Config;
import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.rapidoid.buffer.Buf;
import org.rapidoid.env.Env;
import org.rapidoid.http.AbstractHttpServer;
import org.rapidoid.http.HttpStatus;
import org.rapidoid.http.MediaType;
import org.rapidoid.net.Server;
import org.rapidoid.net.ServerBuilder;
import org.rapidoid.net.TCP;
import org.rapidoid.net.abstracts.Channel;
import org.rapidoid.net.impl.RapidoidHelper;

public class HttpServer extends AbstractHttpServer {

  static final Logger LOG = LoggerFactory.getLogger( HttpServer.class.getName() );
  private final Routes routes = new Routes();
  protected final byte[] CONTENT_ENCODING_GZIP = "Content-Encoding: gzip".getBytes();
  protected final byte[] TRANSFER_ENCODING_CHUNKED = "Transfer-Encoding: chunked".getBytes();

  public HttpServer() {
    super( "Archive", "Not found!\n", "Error!\n", true );
  }

  public void mount( String path, RestApi api ) {
    LOG.info( "Mounting " + api.getClass().getCanonicalName() + " to " + path );
    routes.add( pathToArray( path ), api );
  }

  public HttpStatus response( Channel ctx, int status, boolean isKeepAlive, byte[] body, MediaType contentType ) {
    return response( ctx, status, isKeepAlive, body, contentType, false );
  }

  public HttpStatus response( Channel ctx, int status, boolean isKeepAlive, byte[] body, MediaType contentType, boolean isGzip ) {
    startResponse( ctx, status, isKeepAlive );
    if( isGzip ) {
      ctx.write( CONTENT_ENCODING_GZIP );
      ctx.write( CR_LF );
    }
    writeBody( ctx, body, contentType );
    return HttpStatus.DONE;
  }
 
  @Override
  protected HttpStatus handle( Channel ctx, Buf buf, RapidoidHelper req ) {
    LOG.info( "Got request: " + buf.get( req.verb ) + " " + buf.get( req.uri ));
    String[] path = pathToArray( buf.get( req.path ));
    return routes.handle( this, ctx, buf, req, path );
  }

  public Server listen() {
    return listen(Config.listenAddress(), Config.listenPort());
  }
  
  @Override
  public Server listen(String address, int port) {
    Env.setProfiles(Config.profile());
    ServerBuilder server = TCP.server()
      .protocol( this )
      .address( address )
      .port( port )
      .syncBufs( true )
      .workers( Config.worker() );
    if( Config.listenSSL() ) {
      server.tls( true )
        .keystore( Config.keystore() )
        .keyManagerPassword( Config.keystorePassword().toCharArray() )
        .keystorePassword( Config.keystorePassword().toCharArray() );
      if( Config.truststore() != null ) {
        server
          .truststore( Config.truststore() )
          .truststorePassword( Config.truststorePassword().toCharArray() );
      }
    }
    return server.build().start();
  }

  private String[] pathToArray( String path ) {
    return ( "/" + path ).split( "/+" );
  }

  private class Routes {

    private String[][] routes = new String[0][0];
    private RestApi[] handlers = new RestApi[0];
    private int count = 0;

    public void add( String[] path, RestApi api ) {
      handlers = Arrays.copyOf( handlers, count + 1 );
      handlers[count] = api;
      routes = Arrays.copyOf( routes, count + 1 );
      routes[count] = path;
      count++;
    }

    public HttpStatus handle( HttpServer http, Channel ctx, Buf buf, RapidoidHelper req, String[] path ) {
      for( int i = 0; i < routes.length; i++ ) {
        if( path.length >= routes[i].length ) {
          for( int j = 1; j < routes[i].length; j++ ) {
            if( path[j].equals( routes[i][j] )) {
              if( j == routes[i].length - 1 ) {
                handlers[i].handle( http, ctx, buf, req, path, routes[i].length );
                return HttpStatus.DONE;
              }
              continue;
            }
            break;
          }
        }
      }
      return HttpStatus.NOT_FOUND;
    }

  }

}