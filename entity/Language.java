package com.systemfive.archive.entity;

import com.systemfive.archive.db.OracleQuery;
import java.sql.ResultSet;
import java.util.HashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Language implements Entity {

  static final Logger LOG = LoggerFactory.getLogger( Language.class.getName() );
  private static final HashMap<String, Language> CODES = new HashMap<>();

  public final String code;

  static {
    loadLanguages();
  }

  public Language( String code ) {
    this.code = code;
  }

  public static boolean contains( String code ) {
    return CODES.containsKey( code );
  }

  public static Language get( String code ) {
    return CODES.get( code );
  }

  private static void loadLanguages() {
    LOG.info( "Loading languages" );
    try( OracleQuery query = new OracleQuery() ) {
      try( ResultSet rs = query.get( "SELECT code FROM language ORDER BY code" )) {
        while( rs.next() ) {
          String code = rs.getString( 1 );
          Language language = new Language( code );
          CODES.put( code, language );
        }
      }
    } catch( Exception ex ) {
      LOG.error( "Failed to load languages", ex );
      System.exit( 1 );
    }
  }

  public static void load() {}

}