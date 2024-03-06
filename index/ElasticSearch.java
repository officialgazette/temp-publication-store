package com.systemfive.archive.index;

import com.systemfive.archive.config.Config;
import java.util.List;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.client.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ElasticSearch {

  static final Logger LOG = LoggerFactory.getLogger( ElasticSearch.class.getName() );
  private static RestClient REST_CLIENT;

  public static void connect() {
    LOG.info( "Connecting to Elasticsearch" );
    List<String> nodes = Config.elasticSearchNodes();
    if( nodes.size() >= 3 ) {
      REST_CLIENT = RestClient.builder( node( nodes.get( 0 )), node( nodes.get( 1 )), node( nodes.get( 2 ))).build();
    } else if( nodes.size() == 2 ) {
      REST_CLIENT = RestClient.builder( node( nodes.get( 0 )), node( nodes.get( 1 ))).build();
    } else if( nodes.size() == 1 ) {
      REST_CLIENT = RestClient.builder(node( nodes.get( 0 ))).build();
    } else {
      LOG.error( "No Elasticsearch nodes configured" );
      System.exit( 1 );
    }
  }

  public static RestClient client() {
    return REST_CLIENT;
  }
  
  public static HttpEntity httpJsonEntity( String query ) {
    return new NStringEntity( query, ContentType.APPLICATION_JSON );
  }

  private static HttpHost node( String string ) {
    String host;
    int port;
    if( string.matches( ".*:.*" )) {
      String[] node = string.split( ":" );
      host = node[0];
      port = Integer.parseInt( node[1] );
    } else {
      host = string;
      port = 9200;
    }
    return new HttpHost( host, port, "http" );
  }

}