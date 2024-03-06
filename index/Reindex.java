package com.systemfive.archive.index;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.StringWriter;
import java.sql.Blob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import org.apache.http.HttpEntity;
import org.elasticsearch.client.Response;
import com.systemfive.archive.db.OracleQuery;
import com.systemfive.archive.entity.ContentType;
import com.systemfive.archive.entity.Document;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Reindex {

  private static final DateTimeFormatter DATETIME = DateTimeFormatter.ofPattern( "yyyy-MM-dd'T'HH:mm:ss.SSS" );
  static final Logger LOG = LoggerFactory.getLogger(Reindex.class.getName());
  static final String INDEX_ALIAS = "archive";
  static String OLD_INDEX, NEW_INDEX;
  private static final int BULK_SIZE_LIMIT = 90 * 1024 * 1024; // bytes
  private static final int BULK_PROGRESS_INTERVAL = 10000;     // progress document count

  public static void execute() throws Exception {
    getCurrentIndex();
    try {
      start();
      if( OLD_INDEX != null )
        activateIndex();
    } catch( Exception ex ) {
      throw ex;
    }
    System.exit( 0 );
  }

  private static void getCurrentIndex() throws Exception {
    // Generating new index name
    String new_date = LocalDateTime.now().format( DateTimeFormatter.ofPattern( "yyyyMMdd" ));
    int new_serial = 1;
    // Get current archive index
    Response response = null;
    try {
      response = ElasticSearch.client().performRequest( "GET", "/" + INDEX_ALIAS + "/" );
    } catch( IOException ex ) {
      LOG.info( "No old index found, just creating new one" );
    }
    if( response != null ) {
      HttpEntity responseEntity = response.getEntity();
      try( ByteArrayOutputStream result = new ByteArrayOutputStream() ) {
        try( InputStream inputStream = responseEntity.getContent() ) {
          byte[] buffer = new byte[1024];
          int len;
          while(( len = inputStream.read(buffer)) != -1 )
            result.write( buffer, 0, len );
        }
        // get first (and only) key from response
        @SuppressWarnings( "unchecked" )
        HashMap<String, Object> index = new ObjectMapper().readValue( result.toString( "UTF-8" ), HashMap.class );
        OLD_INDEX = index.keySet().iterator().next();
        LOG.info( "Current alias is linked to: " + OLD_INDEX );
        String old_date = OLD_INDEX.substring( OLD_INDEX.length() - 10, OLD_INDEX.length() - 2 );
        if( new_date.equals( old_date )) {
          int old_serial = Integer.parseInt( OLD_INDEX.substring( OLD_INDEX.length() - 2, OLD_INDEX.length() ));
          new_serial = old_serial + 1;
        }
      }
    }
    NEW_INDEX = INDEX_ALIAS + "-" + new_date + String.format( "%02d", new_serial );
  }

  private static void start() throws Exception {
    try( OracleQuery query = new OracleQuery() ) {
      LOG.info( "creating new index " + NEW_INDEX );
      try {
        esDELETE( "/" + NEW_INDEX );
      } catch( Exception ex ) {}
      esPUT( "/" + NEW_INDEX, "{"
        + "  \"settings\" : {"
        + "    \"analysis\" : {"
        + "      \"filter\" : {"
        + "        \"stop_tokens\"   : {"
        + "          \"type\"        : \"stop\","
        + "          \"stopwords\"   : \"_german_,_french_,_italian_,_english_\""
        + "        }"
        + "      },"
        + "      \"analyzer\" : {"
        + "        \"folding\" : {"
        + "          \"filter\"      : [ \"lowercase\", \"asciifolding\" ],"
        + "          \"tokenizer\"   : \"standard\""
        + "        }"
        + "      }"
        + "    }"
        + "  },"
        + "  \"mappings\" : {"
        + "    \"notice\" : {"
        + "      \"_source\" : {"
        + "        \"includes\" : [ \"tenants\", \"subtenant\", \"id\", \"heading\", \"subheading\", \"submitter\", \"title\", \"publicationTime\", \"status\" ]"
        + "      },"
        + "      \"properties\" : {"
        + "        \"id\"               : { \"type\" : \"string\", \"index\": \"not_analyzed\" },"
        + "        \"tenants\"          : { \"type\" : \"string\", \"index\": \"not_analyzed\" },"
        + "        \"subtenant\"        : { \"type\" : \"string\", \"index\": \"not_analyzed\", \"null_value\": \"NULL\" },"
        + "        \"cantons\"          : { \"type\" : \"string\", \"index\": \"not_analyzed\" },"
        + "        \"heading\"          : { \"type\" : \"string\", \"index\": \"not_analyzed\" },"
        + "        \"subheading\"       : { \"type\" : \"string\", \"index\": \"not_analyzed\" },"
        + "        \"submitter\"        : { \"type\" : \"string\", \"analyzer\": \"folding\" },"
        + "        \"title\"            : { \"type\" : \"string\", \"analyzer\": \"folding\" },"
        + "        \"text\"             : { \"type\" : \"string\", \"analyzer\": \"folding\" },"
        + "        \"publicationTime\"  : { \"type\" : \"date\" },"
        + "        \"publicUntil\"      : { \"type\" : \"date\" },"
        + "        \"status\"           : { \"type\" : \"string\", \"index\": \"not_analyzed\" },"
        + "        \"noFulltextSearch\" : { \"type\" : \"boolean\" }"
        + "      }"
        + "    },"
        + "    \"issue\" : {"
        + "      \"_source\" : {"
        + "        \"includes\" : [ \"tenants\", \"subtenant\", \"id\", \"publicationTime\", \"status\" ]"
        + "      },"
        + "      \"properties\" : {"
        + "        \"id\"              : { \"type\" : \"string\", \"index\": \"not_analyzed\" },"
        + "        \"tenants\"         : { \"type\" : \"string\", \"index\": \"not_analyzed\" },"
        + "        \"subtenant\"       : { \"type\" : \"string\", \"index\": \"not_analyzed\", \"null_value\": \"NULL\" },"
        + "        \"publicationTime\" : { \"type\" : \"date\" },"
        + "        \"publicUntil\"     : { \"type\" : \"date\" },"
        + "        \"status\"          : { \"type\" : \"string\", \"index\": \"not_analyzed\" }"
        + "      }"
        + "    }"
        + "  }"
        + "}"
      );
      // Link new index already now if initializing
      if( OLD_INDEX == null )
        activateIndex();
      JsonFactory jsonfactory = new JsonFactory();
      LocalDateTime publicationTime, publicUntil;
      StringBuilder bulk = new StringBuilder();
      // Get every possible type
      try( ResultSet types = query.get( "SELECT id, type FROM document_type" )) {
        while( types.next() ) {
          int document_type_id = types.getInt( 1 );
          String document_type = types.getString( 2 );
          LOG.info( "Getting document_type '" + document_type + "' for reindexing");
          try( PreparedStatement psql = query.prepare(
            "SELECT x.id, x.identifier, x.tenants, x.subtenant, x.cantons, x.heading, x.subheading, x.submitter, c.title, c.text, x.publication_time, x.public_until, x.status, x.no_fulltext_search FROM ("
            + "SELECT DISTINCT RAWTOHEX(d.id) id, td.identifier, td.subtenant, d.cantons, d.heading, d.subheading, d.submitter, d.publication_time, d.public_until, ds.status, d.no_fulltext_search,"
            + " ( SELECT LISTAGG(t.tenant, ',') WITHIN GROUP (ORDER BY t.id) FROM tenant t INNER JOIN tenant_document td2 ON td2.tenant_id = t.id WHERE td2.document_id = d.id ) tenants"
            + " FROM document d"
            + " INNER JOIN tenant_document td ON td.document_id = d.id"
            + " INNER JOIN document_type dt ON td.document_type_id = dt.id"
            + " INNER JOIN document_status ds ON d.document_status_id = ds.id"
            + " WHERE td.document_type_id = ? AND td.hidden = 0"
            + ") x INNER JOIN content c ON c.document_id = x.id WHERE c.content_type_id = ?"
          )) {
            psql.setInt( 1, document_type_id );
            psql.setInt( 2, ContentType.getPdfId() );
            String docid = null;
            try( ResultSet rs = query.executeQuery( psql )) {
              LOG.info( "Starting reindexing of document_type " + document_type );
              int i = 0;
              while( rs.next() ) {
                i++;
                try( StringWriter writer = new StringWriter() ) {
                  String dbid = rs.getString(1);
                  try( JsonGenerator json = jsonfactory.createGenerator( writer )) {
                    json.writeStartObject();
                    docid = rs.getString( 2 );
                    json.writeStringField( "id", docid );
                    json.writeArrayFieldStart( "tenants" );
                    for( String tenant : rs.getString( 3 ).split( "," ))
                      json.writeString( tenant );             
                    json.writeEndArray();
                    if( rs.getString(4) != null )
                        json.writeStringField( "subtenant", rs.getString(4) );
                    else
                        json.writeNullField("subtenant");
                    json.writeArrayFieldStart( "cantons" );
                    if( rs.getString(5) != null ) {
                      for( String canton : Arrays.asList( rs.getString( 5 ).split( "," )))
                        json.writeString( canton );
                    }
                    json.writeEndArray();
                    json.writeStringField( "heading", rs.getString( 6 ));
                    json.writeStringField( "subheading", rs.getString( 7 ));
                    json.writeStringField( "submitter", rs.getString( 8 ));
                    // Workaround until eshab web is honiroing the status field;
                    if( rs.getString( 13 ).equals( "revoked" ))
                      json.writeStringField( "title", rs.getString( 9 ) + " (annulliert)" );
                    else
                      json.writeStringField( "title", rs.getString( 9 ));
                    // End of workaround
                    Blob blob = rs.getBlob (10 );
                    if( blob != null ) {
                      byte[] text = blob.getBytes( 1, (int) blob.length() );
                      json.writeStringField( "text", new String( Document.uncompress( text )));
                    }
                    publicationTime = rs.getObject( 11, LocalDateTime.class );
                    json.writeStringField( "publicationTime", publicationTime.format( DATETIME ));
                    publicUntil = rs.getObject( 12, LocalDateTime.class );
                    if( publicUntil != null )
                      json.writeStringField( "publicUntil", publicUntil.format( DATETIME ));
                    else
                      json.writeNullField( "publicUntil" );
                    json.writeStringField( "status", rs.getString( 13 ));
                    json.writeBooleanField( "noFulltextSearch", ( rs.getInt( 14 ) > 0 ));
                    json.writeEndObject();
                  }
                  if( writer.getBuffer().length() + bulk.length() > BULK_SIZE_LIMIT )
                    bulk = pushToElasticsearch(bulk, document_type);
                  bulk.append( "{\"index\":{\"_id\":\"").append( dbid ).append( "\"}}\n" );
                  bulk.append( writer.toString() ).append( "\n" );
                }
                if( i % BULK_PROGRESS_INTERVAL == 0 )
                  LOG.info( "Reindexed " + i / 1000 + "k documents" );
              }
              if( bulk.length() > 0 )
                bulk = pushToElasticsearch( bulk, document_type );
              LOG.info( "Reindexed " + i + " " + document_type + " documents" );
            } catch( Exception ex ) {
              LOG.error( "Failed to reindex document: " + docid );
              throw ex;
            }
          }
        }
      }
    } catch( Exception ex ) {
      LOG.error( "Failed to reindex", ex );
    }
  }

  private static StringBuilder pushToElasticsearch( StringBuilder bulk, String document_type ) throws Exception {
    LOG.info( "Pushing bulk to elasticsearch (" + bulk.length() + ")" );
    esPOST( "/" + NEW_INDEX + "/" + document_type + "/_bulk", bulk.toString() );
    bulk.setLength( 0 );
    return bulk;
  }

  private static String esControl( String method, String uri, String data, boolean showError ) throws Exception {
    try {
      Map<String, String> params = new HashMap<>();
      int i = 30;
      Response response;
      while( true ) {
        if( data != null ) {
          HttpEntity entity = ElasticSearch.httpJsonEntity( data );
          response = ElasticSearch.client().performRequest( method, uri, params, entity );
        } else {
          response = ElasticSearch.client().performRequest( method, uri, params );
        }
        int status = response.getStatusLine().getStatusCode();
        if( status >= 500 ) {
          LOG.error( "Error: " + response.getStatusLine().getReasonPhrase() );
          LOG.error( "Retrying in 10 seconds..." );
          TimeUnit.SECONDS.sleep( 10 );
        } else {
          break;
        }
        if( i == 0 ) {
          LOG.error( "Giving up retrying..." );
          break;
        }
        i--;
      }
      // get and return body
      HttpEntity responseEntity = response.getEntity();
      try(
        InputStream inputStream = responseEntity.getContent();
        ByteArrayOutputStream result = new ByteArrayOutputStream();
      ) {
        byte[] buffer = new byte[1024];
        int len;
        while ((len = inputStream.read(buffer)) != -1) {
          result.write(buffer, 0, len);
        }
        return result.toString( "UTF-8" );
      }
    } catch( IOException | InterruptedException ex ) {
      if( showError )
        LOG.error( "Elasticsearch exception", ex );
    }
    return null;
  }

  private static String esPOST( String uri, String data ) throws Exception {
    return esControl( "POST", uri, data, true );
  }

  private static String esPUT( String uri, String data ) throws Exception {
    return esControl( "PUT", uri, data, true );
  }

  private static String esDELETE( String uri ) throws Exception {
    return esControl( "DELETE", uri, null, false );
  }

  private static void activateIndex() throws Exception {
    String response;
    String query = "{\"actions\" : [";
    if( OLD_INDEX != null )
      query += "{ \"remove\" : { \"index\" : \"" + OLD_INDEX + "\", \"alias\" : \"" + INDEX_ALIAS + "\" } },";
    query += "{ \"add\"    : { \"index\" : \"" + NEW_INDEX + "\", \"alias\" : \"" + INDEX_ALIAS + "\" } }]}";
    response = esPOST( "/_aliases", query );
    if( response != null ) {
      LOG.info( "Alias " + INDEX_ALIAS + " successfully remapped to " + NEW_INDEX );
      if( OLD_INDEX != null ) {
        response = esDELETE( "/" + OLD_INDEX );
        if( response != null )
          LOG.info( "Old index " + OLD_INDEX + " successfully removed" );
      }
    }
  }

}