package com.systemfive.archive.entity;

import com.systemfive.archive.db.OracleQuery;
import java.sql.ResultSet;
import java.util.HashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ContentType implements Entity {

  static final Logger LOG = LoggerFactory.getLogger( ContentType.class.getName() );
  private static final HashMap<String, ContentType> EXTENSIONS = new HashMap<>();
  private static ContentType pdf = null;

  public final String extension;
  public final String type;
  public final int id;

  static {
    loadContentTypes();
  }

  public ContentType( String extension, String type, int id ) {
    this.extension = extension;
    this.type = type;
    this.id = id;
  }

  public static boolean contains( String extension ) {
    return EXTENSIONS.containsKey( extension );
  }

  public static ContentType get( String extension ) {
    return EXTENSIONS.get( extension );
  }
  
  public static ContentType getByFilename( String filename ) {
    if( !filename.matches( ".+\\..+" ))
      return null;
    String extension = filename.replaceAll( ".*\\.([^.]+)$", "$1" );
    return EXTENSIONS.get( extension );
  }
  
  public static int getPdfId() {
    if( pdf == null )
      return 0;
    return pdf.id;
  }

  private static void loadContentTypes() {
    LOG.info( "Loading content types" );
    try( OracleQuery query = new OracleQuery() ) {
      try( ResultSet rs = query.get( "SELECT id, file_extension, type FROM content_type ORDER BY id" )) {
        while( rs.next() ) {
          int id = rs.getInt( 1 );
          String extension = rs.getString( 2 );
          String type = rs.getString( 3 );
          ContentType contentType = new ContentType( extension, type, id );
          EXTENSIONS.put( extension, contentType );
        }
      }
    } catch( Exception ex ) {
      LOG.error( "Failed to load content types", ex );
      System.exit( 1 );
    }
    pdf = get( "pdf" );
    if( pdf == null ) {
      LOG.error( "PDF content type not configured" );
      System.exit( 1 );
    }
  }

  public static void load() {}

}