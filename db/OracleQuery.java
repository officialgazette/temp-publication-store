package com.systemfive.archive.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OracleQuery implements AutoCloseable {

  static final Logger LOG = LoggerFactory.getLogger( OracleQuery.class.getName() );
  private Connection Connection = null;
  private Statement Statement = null;
  private PreparedStatement PreparedStatement = null;
  private ResultSet ResultSet = null;

  public OracleQuery() {
    try {
      Connection = Oracle.getConnection();
    } catch( SQLException ex ) {
      LOG.error( "Could not get connection to database", ex );
    }
  }

  public ResultSet get( String query ) throws SQLException {
    Statement = Connection.createStatement( ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE );
    Statement.closeOnCompletion();
    ResultSet = Statement.executeQuery( query );
    return ResultSet;
  }

  public void filterLanguage( ResultSet rs, String[] languages, int language_column ) throws SQLException {
    if( !rs.first( )) {
      LOG.debug( "filterLanguage: no rows found" );
      return;
    }
    if( rs.isLast() ) {
      LOG.debug( "filterLanguage: only one row, nothing to filter" );
      return;
    }
    for( String language : languages ) {
      LOG.debug( "filterLanguage: looking for language: " + language );
      rs.first();
      while( !rs.isAfterLast() ) {
        String lang = rs.getString( language_column );
        if( lang != null && lang.equals( language )) {
          LOG.debug( "filterLanguage: found" );
          return;
        }
        rs.next();
      }
    }
    LOG.debug( "filterLanguage: no match returning first row" );
    rs.first();
  }

  public PreparedStatement prepare( String query ) throws SQLException {
    PreparedStatement = Connection.prepareStatement( query );
    PreparedStatement.closeOnCompletion();
    return PreparedStatement;
  }

  public PreparedStatement prepare( String query, int resultSetType, int resultSetConcurrency ) throws SQLException {
    PreparedStatement = Connection.prepareStatement( query, resultSetType, resultSetConcurrency );
    PreparedStatement.closeOnCompletion();
    return PreparedStatement;
  }

  public Boolean execute( String query ) throws SQLException {
    this.get( query );
    return true;
  }

  public ResultSet executeQuery( PreparedStatement psql ) throws SQLException {
    ResultSet = psql.executeQuery();
    return ResultSet;
  }

  public int executeUpdate( PreparedStatement psql ) throws SQLException {
    return psql.executeUpdate();
  }

  public void disableAutoCommit() throws SQLException {
    Connection.setAutoCommit( false );
  }

  public void rollback() throws SQLException {
    Connection.rollback();
  }

  public void commit() throws SQLException {
    Connection.commit();
  }

  @Override
  public void close() {
    if( ResultSet != null ) {
      try {
        ResultSet.close();
      } catch( SQLException ex ) {
        LOG.error( "Could not close Oracle ResultSet" );
      }
    }
    if( Statement != null ) {
      try {
        Statement.close();
      } catch( SQLException ex ) {
        LOG.error( "Could not close Oracle Statement" );
      }
    }
    if( PreparedStatement != null ) {
      try {
        PreparedStatement.close();
      } catch( SQLException ex ) {
        LOG.error( "Could not close Oracle PreparedStatement" );
      }
    }
    if( Connection != null ) {
      try {
        Connection.close();
      } catch( SQLException ex ) {
        LOG.error( "Could not close Oracle Connection" );
      }
    }
  }

}