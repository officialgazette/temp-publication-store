package com.systemfive.archive.db;

import com.systemfive.archive.config.Config;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import oracle.ucp.jdbc.PoolDataSource;
import oracle.ucp.jdbc.PoolDataSourceFactory;
import oracle.ucp.jdbc.ValidConnection;
import oracle.ucp.jdbc.oracle.OracleJDBCConnectionPoolStatistics;

public class Oracle {

  static final Logger LOG = LoggerFactory.getLogger( Oracle.class.getName() );
  private final static PoolDataSource PDS = PoolDataSourceFactory.getPoolDataSource();

  public static Connection getConnection() throws SQLException {
    return PDS.getConnection();
  }

  public static void connect() throws SQLException {
    if( Config.oracleONSNodes() == null )
      LOG.info( "Connecting to Oracle Standalone Database" );
    else
      LOG.info( "Connecting to Oracle Real Application Cluster" );
    PDS.setConnectionProperty( "oracle.net.ssl_version", Config.oracleSSLVersion() );
    PDS.setConnectionProperty( "oracle.net.ssl_cipher_suites", Config.oracleSSLCiphers() );
    PDS.setConnectionProperty( "oracle.net.ssl_server_dn_match", Config.oracleSSLMatchDN() );
    if( Config.truststore() != null ) {
      PDS.setConnectionProperty( "javax.net.ssl.trustStore", Config.truststore() );
      PDS.setConnectionProperty( "javax.net.ssl.trustStoreType", "JKS" );
      PDS.setConnectionProperty( "javax.net.ssl.trustStorePassword", Config.truststorePassword() );
    }
    PDS.setConnectionFactoryClassName( "oracle.jdbc.pool.OracleDataSource" );
    PDS.setUser( Config.oracleUsername() );
    PDS.setPassword( Config.oraclePassword() );
    String url = "jdbc:oracle:thin:@" + Config.oracleDatabase();
    PDS.setURL( url );
    Properties properties = new Properties();
    properties.put( oracle.net.ns.SQLnetDef.TCP_CONNTIMEOUT_STR, "5000" );
    PDS.setConnectionProperties( properties );
    PDS.setConnectionPoolName( "OraclePool" );
    PDS.setValidateConnectionOnBorrow( true );
    PDS.setMinPoolSize( 1 );
    PDS.setMaxPoolSize( Config.worker() );
    // RAC
    if( Config.oracleONSNodes() != null ) {
      String config = "nodes=" + Config.oracleONSNodes();
      if( Config.oracleONSWallet() != null ) {
        config += "\nwalletfile=" + Config.oracleONSWallet();
        if( Config.oracleONSWallet() != null )
          config += "\nwalletpassword=" + Config.oracleONSWalletPassword();
      }
      PDS.setONSConfiguration( config );
      PDS.setFastConnectionFailoverEnabled( true );
    }
    // END RAC
    int initPool = Config.worker() / 4;
    PDS.setInitialPoolSize( initPool > 0 ? initPool : 1 );
    try(
      Connection connection = PDS.getConnection();
      Statement statement = connection.createStatement();
      ResultSet rs = statement.executeQuery( "SELECT 'test' FROM DUAL" );
    ) {
      rs.next();
    } catch( SQLException ex ) {
      LOG.error( "Could not connect to database", ex );
      System.exit( 1 );
    }
  }

  public void start() throws Exception {
    Connection conn = PDS.getConnection();
    Statement stmt = conn.createStatement();
    while( true ) {
      try( ResultSet rs = stmt.executeQuery( "SELECT 'test' FROM DUAL" )) {
        while( rs.next() ) {
          LOG.info( "Result: " + rs.getString( 1 ));
          // Obtain number of borrowed and available connections from
          // pool statistics.
          // JDBCConnectionPoolStatistics stats = pds.getStatistics();
          // System.out.println("FCF Activ(" + pool + "): " + stats.getBorrowedConnectionsCount());
          // System.out.println("FCF Avail(" + pool + "): " + stats.getAvailableConnectionsCount()+"\n");
        }
      } catch( SQLException ex ) {
        ex.printStackTrace();
        // The recommended way to check connection usability after a
        // RAC-down event triggers UCP FCF actions.
        if( conn == null || !( (ValidConnection) conn ).isValid()) {
          LOG.warn( "Connection retry necessary..." );
          // Use UCP's FCF-specific statistics to verify the pool's
          // FCF actions.
          OracleJDBCConnectionPoolStatistics stats = (OracleJDBCConnectionPoolStatistics) PDS.getStatistics();
          LOG.info( stats.getFCFProcessingInfo() );
          try {
            conn.close();
          } catch( Exception closeEx) {
            LOG.error( "Exception detected while closing connection", closeEx );
          }
          // Retry to connect to surviving RAC instances
          conn = PDS.getConnection();
          stmt = conn.createStatement();
        }
      }
      Thread.sleep( 1000 );
    }
  }

}