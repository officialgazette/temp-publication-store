package com.systemfive.archive;

import com.systemfive.archive.db.Oracle;
import com.systemfive.archive.entity.Language;
import com.systemfive.archive.entity.Tenant;
import com.systemfive.archive.index.ElasticSearch;
import com.systemfive.archive.log.LogSetup;
import com.systemfive.archive.index.Reindex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReindexMain {

  static Logger LOG = LoggerFactory.getLogger( ReindexMain.class );

  public static void main( String[] args ) throws Exception {

    Runtime.getRuntime().addShutdownHook(
      new Thread() {
        @Override
        public void run() {
          System.out.println( "Reindex shutdown" );
        }
      }
    );

    LogSetup.init();

    Oracle.connect();
    ElasticSearch.connect();

    Language.load();
    Tenant.load();

    Reindex.execute();

  }
}