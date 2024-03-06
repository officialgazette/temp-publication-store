package com.systemfive.archive.config;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Config extends SecurityManager {

  static final Logger LOG = LoggerFactory.getLogger( Config.class.getName() );

  private static final File CONFIG_FILE = new File( "conf" + File.separator + "archive.properties" );
  private static final Properties PROPERTIES = new Properties();
  private static final Config INSTANCE = new Config();
  private static final String PROFILE;
  private static final int WORKER;
  private static final int LISTEN_PORT;
  private static final String LISTEN_ADDRESS;
  private static final boolean LISTEN_SSL;
  private static final String ORACLE_USERNAME;
  private static final String ORACLE_PASSWORD;
  private static final String ORACLE_DATABASE;
  private static final String ORACLE_ONS_NODES;
  private static final String ORACLE_ONS_WALLET;
  private static final String ORACLE_ONS_WALLET_PASSWORD;
  private static final String ORACLE_SSL_VERSION;
  private static final String ORACLE_SSL_CIPHERS;
  private static final String ORACLE_SSL_MATCHDN;
  private static final String KEYSTORE;
  private static final String KEYSTORE_PASSWORD;
  private static final String TRUSTSTORE;
  private static final String TRUSTSTORE_PASSWORD;
  private static final List<String> ELASTICSEARCH_NODES;
  private static final boolean DOUBLEURLDECODE;

  static {

    LOG.info( "Loading configuration" );
    if( CONFIG_FILE.exists() && CONFIG_FILE.isFile() ) {
      try {
        PROPERTIES.load( new FileInputStream( CONFIG_FILE ));
      } catch( IOException ex ) {
        throw new ExceptionInInitializerError( ex );
      }
    }
   
    PROFILE = PROPERTIES.getProperty( "profile", "production" );
    WORKER = Integer.parseInt(PROPERTIES.getProperty( "worker", "20" ));

    LISTEN_ADDRESS = PROPERTIES.getProperty( "listen.address", "0.0.0.0" );
    LISTEN_PORT = Integer.parseInt(PROPERTIES.getProperty( "listen.port", "8888" ));
    LISTEN_SSL = Boolean.parseBoolean(PROPERTIES.getProperty( "listen.ssl", "false" ));
    
    ORACLE_USERNAME = PROPERTIES.getProperty( "oracle.username" );
    ORACLE_PASSWORD = PROPERTIES.getProperty( "oracle.password" );
    ORACLE_DATABASE = PROPERTIES.getProperty( "oracle.database" );
    
    ORACLE_SSL_VERSION = PROPERTIES.getProperty( "oracle.ssl.version", "1.2" );
    ORACLE_SSL_CIPHERS = PROPERTIES.getProperty( "oracle.ssl.ciphers", "(TLS_RSA_WITH_AES_128_GCM_SHA256)" );
    ORACLE_SSL_MATCHDN = PROPERTIES.getProperty( "oracle.ssl.matchdn", "true" );

    ORACLE_ONS_NODES = PROPERTIES.getProperty( "oracle.ons.nodes", PROPERTIES.getProperty( "oracle.onsnodes" ));
    ORACLE_ONS_WALLET = PROPERTIES.getProperty( "oracle.ons.wallet" );
    ORACLE_ONS_WALLET_PASSWORD = PROPERTIES.getProperty( "oracle.ons.wallet.password" );
   
    KEYSTORE = PROPERTIES.getProperty( "keystore" );
    KEYSTORE_PASSWORD = PROPERTIES.getProperty( "keystore.password" );

    TRUSTSTORE = PROPERTIES.getProperty( "truststore" );
    TRUSTSTORE_PASSWORD = PROPERTIES.getProperty( "truststore.password" );

    String elasticsearchNodes = PROPERTIES.getProperty( "elasticsearch.nodes" ).replace( "[\\t ]+", "" );
    ELASTICSEARCH_NODES = Arrays.asList(elasticsearchNodes.split( "," ));   

    DOUBLEURLDECODE = Boolean.parseBoolean( PROPERTIES.getProperty( "doubleurldecode", "false" ));
  }

  public static String profile() {
    return PROFILE;
  }
  
  public static int worker() {
    return WORKER;
  }

  public static int listenPort() {
    return LISTEN_PORT;
  }

  public static String listenAddress() {
    return LISTEN_ADDRESS;
  }

  public static boolean listenSSL() {
    return LISTEN_SSL;
  }

  public static String oracleUsername() {
    return ORACLE_USERNAME;
  }

  public static String oraclePassword() {
    if( hasAccess( com.systemfive.archive.db.Oracle.class ))
      return ORACLE_PASSWORD;
    return null;
  }

  public static String oracleDatabase() {
    return ORACLE_DATABASE;
  }

  public static String oracleONSNodes() {
    return ORACLE_ONS_NODES;
  }

  public static String oracleONSWallet() {
    return ORACLE_ONS_WALLET;
  }

  public static String oracleONSWalletPassword() {
    if( hasAccess( com.systemfive.archive.db.Oracle.class ))
      return ORACLE_ONS_WALLET_PASSWORD;
    return null;
  }

  public static String oracleSSLVersion() {
    return ORACLE_SSL_VERSION;
  }

  public static String oracleSSLCiphers() {
    return ORACLE_SSL_CIPHERS;
  }

  public static String oracleSSLMatchDN() {
    return ORACLE_SSL_MATCHDN;
  }
  
  public static String keystore() {
    return KEYSTORE;
  }

  public static String keystorePassword() {
    if( hasAccess( com.systemfive.archive.rest.HttpServer.class ))
      return KEYSTORE_PASSWORD;
    return null;
  }
  
  public static String truststore() {
    return TRUSTSTORE;
  }

  public static String truststorePassword() {
    if( hasAccess( com.systemfive.archive.rest.HttpServer.class, com.systemfive.archive.db.Oracle.class ))
      return TRUSTSTORE_PASSWORD;
    return null;
  }
  
  public static List<String> elasticSearchNodes() {
    return ELASTICSEARCH_NODES;
  }
  
  public static boolean doubleUrlDecode() {
    return DOUBLEURLDECODE;
  }

  private static boolean hasAccess( Class allowed ) {
    Class caller = INSTANCE.getClassContext()[2];
    return allowed.equals( caller );
  }

  private static boolean hasAccess( Class... classes ) {
    Class caller = INSTANCE.getClassContext()[2];
    for( Class c : classes ) {
      if( c.equals( caller ))
        return true;
    }
    return false;
  }

}