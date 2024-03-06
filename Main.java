package com.systemfive.archive;

import com.systemfive.archive.db.Oracle;
import com.systemfive.archive.entity.ContentType;
import com.systemfive.archive.entity.Language;
import com.systemfive.archive.entity.Tenant;
import com.systemfive.archive.index.ElasticSearch;
import com.systemfive.archive.log.LogSetup;
import com.systemfive.archive.rest.HttpServer;
import com.systemfive.archive.rest.RestApi1;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

  static Logger LOG = LoggerFactory.getLogger( Main.class );

  public static void main( String[] args ) throws Exception {

    Runtime.getRuntime().addShutdownHook(
      new Thread() {
        @Override
        public void run() {
          LOG.info( "Archive shutdown" );
        }
      }
    );

    LogSetup.init();

    Oracle.connect();
    ElasticSearch.connect();

    ContentType.load();
    Language.load();
    Tenant.load();

    HttpServer http = new HttpServer();
    http.mount( "/api/v1", new RestApi1() );
    http.listen();

  }

}