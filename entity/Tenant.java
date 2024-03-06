package com.systemfive.archive.entity;

import com.systemfive.archive.db.OracleQuery;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Tenant implements Entity {

  static final Logger LOG = LoggerFactory.getLogger( Tenant.class.getName() );
  private static final HashMap<String, Tenant> NAMES = new HashMap<>();
  private static final HashMap<Integer, Tenant> IDS = new HashMap<>();

  public final int id;
  public final String name;
  public String subtenant;

  static {
    loadTenants();
  }

  public Tenant( int id, String name ) {
    this.id = id;
    this.name = name;
    this.subtenant = null;
  }
  
  public Tenant( int id, String name, String subtenant ) {
    this.id = id;
    this.name = name;
    this.subtenant = subtenant;
  }

  public static boolean contains( String name ) {
    return NAMES.containsKey( name );
  }

  public static boolean contains( int id ) {
    return IDS.containsKey( id );
  }

  public static Tenant get( String name ) {
    return NAMES.get( name );
  }

  public static Tenant get( int id ) {
    return IDS.get( id );
  }

  public static Tenant create( String name ) throws Exception {
    try( OracleQuery query = new OracleQuery() ) {
      LOG.info( "Creating new tenant " + name );
      try( PreparedStatement psql = query.prepare( "INSERT INTO tenant ( id, tenant ) VALUES ( sq_tenant_id.nextval, ? )" )) {
        psql.setString( 1, name );
        query.executeQuery(psql);
      }
    } catch( SQLException ex ) {
      if( ex.getErrorCode() != 00001 ) // ignore ORA-00001: unique constraint violation
        LOG.error( "Could not create new tenant", ex );
    }
    // reload entities
    loadTenants();
    return get( name );
  }

  private static void loadTenants() {
    LOG.info( "Loading tenants" );
    try( OracleQuery query = new OracleQuery() ) {
      NAMES.clear();
      IDS.clear();
      try( ResultSet rs = query.get( "SELECT id, tenant FROM tenant ORDER BY id" )) {
        while( rs.next() ) {
          int id = rs.getInt( 1 );
          String name = rs.getString( 2 );
          Tenant tenant = new Tenant( id, name );
          NAMES.put( name, tenant );
          IDS.put( id, tenant );
        }
      }
    } catch( Exception ex ) {
      LOG.error( "Failed to load tenants", ex );
      System.exit( 1 );
    }
  }

  public static void load() {}

}