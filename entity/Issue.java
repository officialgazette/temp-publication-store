package com.systemfive.archive.entity;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.systemfive.archive.db.OracleQuery;
import static com.systemfive.archive.entity.Document.DATETIME;
import com.systemfive.archive.index.ElasticSearch;
import com.systemfive.archive.index.Result;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import org.apache.http.HttpEntity;
import org.elasticsearch.client.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Issue extends Document {

  static final Logger LOG = LoggerFactory.getLogger( Issue.class.getName() );

  public Issue() {
    documentType = 2;
  }

  public Issue( Tenant tenant, String id ) {
    super( tenant, id );
    documentType = 2;
  }

  public Issue( String id ) {
    super( id );
    documentType = 2;
  }

  public static Issue get( Tenant tenant, String[] id_array, String[] languages ) {
    PreparedStatement psql = null;
    try( OracleQuery query = new OracleQuery() ) {
      if( id_array.length == 1 ) {
        String id = id_array[0];
        if( id.equals( "latest" )) {
          psql = query.prepare(
            "SELECT td.identifier, d.issue, d.publication_time, d.public_until, c.language_code, c.archive_time"
            + " FROM document d"
            + " INNER JOIN content c ON c.document_id = d.id"
            + " INNER JOIN tenant_document td ON td.document_id = d.id"
            + " WHERE td.tenant_id = ?"
            + "   AND td.document_type_id = 2"
            + "   AND c.content_type_id = ?"
            + "   AND d.document_status_id = 1"
            + (tenant.subtenant == null || tenant.subtenant.isEmpty() ? "" : " AND td.subtenant = ?")
            + "   AND d.publication_time = ("
            + "     SELECT max(d2.publication_time)"
            + "     FROM document d2"
            + "     JOIN tenant_document td2 ON d2.id = td2.document_id"
            + "     WHERE td2.tenant_id = td.tenant_id"
            + (tenant.subtenant == null || tenant.subtenant.isEmpty() ? "" : " AND td2.subtenant = td.subtenant")
            + "       AND td2.document_type_id = 2"
            + "       AND d2.publication_time < sysdate"
            + "       AND d2.document_status_id = 1"
            + " )",
            ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE
          );
          psql.setInt( 1, tenant.id );
          psql.setInt( 2, ContentType.getPdfId() );
            if (tenant.subtenant == null || tenant.subtenant.isEmpty()) {}
            else {
                psql.setString( 3, tenant.subtenant );
            }
        } else {
          psql = query.prepare(
            "SELECT td.identifier, d.issue, d.publication_time, d.public_until, c.language_code, c.archive_time"
            + " FROM document d"
            + " INNER JOIN content c ON c.document_id = d.id"
            + " INNER JOIN tenant_document td ON td.document_id = d.id"
            + " WHERE d.document_status_id = 1 AND td.identifier = ? AND td.tenant_id = ? AND td.document_type_id = 2 AND c.content_type_id = ?"
            + (tenant.subtenant == null || tenant.subtenant.isEmpty() ? "" : " AND td.subtenant = ?"),
            ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE
          );
          psql.setString( 1, id );
          psql.setInt( 2, tenant.id );
          psql.setInt( 3, ContentType.getPdfId() );
            if (tenant.subtenant == null || tenant.subtenant.isEmpty()) {}
            else {
                psql.setString( 4, tenant.subtenant );
            }
        }
      } else if( id_array.length > 1 ) {
        // Get issue by year and number (slow)
        psql = query.prepare(
          "SELECT td.identifier, d.issue, d.publication_time, d.public_until, c.language_code, c.archive_time"
          + " FROM document d"
          + " INNER JOIN content c ON c.document_id = d.id"
          + " INNER JOIN tenant_document td ON td.document_id = d.id"
          + " WHERE d.document_status_id = 1 AND extract(year from publication_time) = ? AND d.issue = ? AND td.tenant_id = ? AND td.document_type_id = 2"
          + "   AND c.content_type_id = ?" + (tenant.subtenant == null || tenant.subtenant.isEmpty() ? "" : " AND td.subtenant = ?"),
          ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE
        );
        psql.setString( 1, id_array[0] );
        psql.setString( 2, id_array[1] );
        psql.setInt( 3, tenant.id );
        psql.setInt( 4, ContentType.getPdfId() );
        if (tenant.subtenant == null || tenant.subtenant.isEmpty()) {}
        else {
            psql.setString( 5, tenant.subtenant );
        }
      }
      try( ResultSet rs = query.executeQuery( psql )) {
        query.filterLanguage(rs, languages, 5);
        if( !rs.isAfterLast() ) {
          Issue doc = new Issue( tenant, rs.getString(1) );
          doc.issue( rs.getInt( 2 ));
          doc.publicationTime( rs.getObject( 3, LocalDateTime.class ));
          doc.publicUntil( rs.getObject( 4, LocalDateTime.class ));
          doc.language( Language.get( rs.getString( 5 )));
          doc.archiveTime( rs.getObject( 6, LocalDateTime.class ));
          return doc;
        } else {
          return null;
        }
      }
    } catch( Exception ex ) {
      LOG.error( "Failed to get issue", ex );
    } finally {
      try {
        if( psql != null )
          psql.close();
      } catch( SQLException ex ) {
        LOG.error( "Could not close prepared statement used to get issue", ex );
      }
    }
    return null;
  }

  public static Issue getLazy( Tenant tenant, String[] id, String[] languages, boolean getFields, ContentType contentType ) {
    Issue issue;
    if( getFields )
      issue = Issue.get( tenant, id, languages );
    else
      issue = new Issue();
    if( contentType != null ) {
      issue.data = Document.getContent( 2, tenant, id, languages, contentType );
      if( issue.data == null )
        return null;
    }
    return issue;
  }

  public static Result search( String tenantName, String subtenant, String id, LocalDateTime publicationTimeFrom, LocalDateTime publicationTimeTo, int page, int pagesize, boolean isPublic ) {
    DateTimeFormatter datetime = DateTimeFormatter.ofPattern( "yyyy-MM-dd'T'HH:mm:ss.SSS" );
    Map<String, String> params = new HashMap<>();
    params.put( "size", Integer.toString( pagesize ));
    int offset;
    if( page <= 1 )
      offset = 0;
    else
      offset = ( page - 1 ) * pagesize;
    params.put( "from", Integer.toString( offset ));
    JsonFactory jsonfactory = new JsonFactory();
    String query;
    try( StringWriter writer = new StringWriter() ) {
      try( JsonGenerator json = jsonfactory.createGenerator( writer )) {
        json.writeRaw( "{\"sort\":[{\"publicationTime\":\"desc\"},{\"id\":\"desc\"}],\"filter\":{\"bool\":" );
        json.writeStartObject();
        json.writeArrayFieldStart( "must" );
        if( id != null && !id.isEmpty() ) {
          json.writeStartObject();
          json.writeObjectFieldStart( "term" );
          json.writeStringField( "id", id );
          json.writeEndObject();
          json.writeEndObject();
        }
        if( tenantName != null && !tenantName.isEmpty() ) {
          json.writeStartObject();
          json.writeObjectFieldStart( "term" );
          json.writeStringField( "tenants", tenantName );
          json.writeEndObject();
          json.writeEndObject();
        }
        if( subtenant != null && !subtenant.isEmpty() ) {
          json.writeStartObject();
          json.writeObjectFieldStart( "term" );
          json.writeStringField( "subtenant", subtenant );
          json.writeEndObject();
          json.writeEndObject();
        }
        if( publicationTimeFrom != null || publicationTimeTo != null ) {
          json.writeStartObject();
          json.writeObjectFieldStart( "range" );
          json.writeObjectFieldStart( "publicationTime" );
          if( publicationTimeFrom != null )
            json.writeStringField( "gte", publicationTimeFrom.format( datetime ));
          if( publicationTimeTo != null )
            json.writeStringField( "lte", publicationTimeTo.format( datetime ));
          json.writeEndObject();
          json.writeEndObject();
          json.writeEndObject();
        }
        if( isPublic ) {
          json.writeStartObject();
          json.writeObjectFieldStart( "bool" );
          json.writeArrayFieldStart( "should" );
          json.writeStartObject();
          json.writeObjectFieldStart( "range" );
          json.writeObjectFieldStart( "publicUntil" );
          json.writeStringField( "gte", "now" );
          json.writeEndObject();
          json.writeEndObject();
          json.writeEndObject();
          json.writeStartObject();
          json.writeObjectFieldStart( "bool" );
          json.writeObjectFieldStart( "must_not" );
          json.writeObjectFieldStart( "exists" );
          json.writeStringField( "field", "publicUntil" );
          json.writeEndObject();
          json.writeEndObject();
          json.writeEndObject();
          json.writeEndObject();
          json.writeEndArray();
          json.writeEndObject();
          json.writeEndObject();
        }
        json.writeEndArray();
        json.writeRaw( "}}" );
      }
      query = writer.toString();
    } catch( IOException ex ) {
      LOG.error( "Failed to build Elasticsearch query", ex );
      return null;
    }
    LOG.debug( "Running the following Elasticsearch query: " + query );
    HttpEntity entity = ElasticSearch.httpJsonEntity( query );
    try {
      Response response = ElasticSearch.client().performRequest( "POST", "/archive/issue/_search", params, entity );
      HttpEntity responseEntity = response.getEntity();
      try(
        InputStream inputStream = responseEntity.getContent();
        ByteArrayOutputStream result = new ByteArrayOutputStream();
      ) {
        byte[] buffer = new byte[1024];
        int len;
        while(( len = inputStream.read(buffer)) != -1 )
          result.write(buffer, 0, len);
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure( DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false );
        return mapper.readValue( result.toString( "UTF-8" ), Result.class );
      }
    } catch( IOException ex ) {
      LOG.error( "Failed to query Elasticsearch", ex );
    }
    return null;
  }

  public void save() throws Exception {
    LocalDateTime time = LocalDateTime.now();
    boolean addition = false;
    try( OracleQuery query = new OracleQuery() ) {
      query.disableAutoCommit();
      byte[] documentId = null;
      // check if this document already exist
      for( Tenant tenant : tenants ) {
        try( PreparedStatement psql = query.prepare(
          "SELECT document_id "
          + "FROM tenant_document "
          + "WHERE identifier = ? "
          + "  AND tenant_id = ?"
        )) {
          psql.setString( 1, id );
          psql.setInt( 2, tenant.id );
          try( ResultSet rs = query.executeQuery( psql )) {
            if( rs.next() ) {
              documentId = rs.getBytes( 1 );
              break;
            }
          }
        }
      }
      // insert document if not yet existing
      if( documentId == null ) {
        addition = true;
        // create new GUID
        try( ResultSet rs = query.get( "SELECT SYS_GUID() FROM DUAL" )) {
          rs.next();
          documentId = rs.getBytes( 1 );
        }
        // document
        try( PreparedStatement psql = query.prepare(
          "INSERT INTO document"
          + " ( id, document_status_id, issue, publication_time, public_until )"
          + " VALUES ( ?, ?, ?, ?, ? )"
        )) {
          psql.setBytes( 1, documentId );
          psql.setInt( 2, documentStatus );
          if( issue != null )
            psql.setInt( 3, issue );
          else
            psql.setNull( 3, Types.NUMERIC );
          psql.setObject(4, publicationTime);
          psql.setObject(5, publicUntil);
          query.executeUpdate( psql );
        } catch( Exception ex ) {
          query.rollback();
          LOG.error( "Failed to insert into document table", ex );
          throw ex;
        }
      }
      // tenant_document
      for( Tenant tenant : tenants ) {
        try( PreparedStatement psql = query.prepare(
          "INSERT INTO tenant_document"
          + " ( tenant_id, document_type_id, identifier, document_id, subtenant )"
          + " VALUES ( ?, ?, ?, ?, ? )"
        )) {
          psql.setInt( 1, tenant.id );
          psql.setInt( 2, documentType );
          psql.setString( 3, id );
          psql.setBytes( 4, documentId );
          psql.setString( 5, tenant.subtenant );
          query.executeUpdate( psql );
        } catch( SQLException ex ) {
          if( ex.getErrorCode() != 00001 ) { // ignore ORA-00001: unique constraint violation
            query.rollback();
            LOG.error( "Failed to insert into tenant_document table", ex );        
          }
        } catch( Exception ex ) {
          query.rollback();
          LOG.error( "Failed to insert into tenant_document table", ex );
          throw ex;
        }
      }
      // content
      try( PreparedStatement psql = query.prepare(
        "INSERT INTO content"
        + " ( document_id, content_type_id, language_code, archive_time, data )"
        + " VALUES ( ?, ?, ?, ?, ? )"
      )) {
        psql.setBytes( 1, documentId );
        psql.setInt( 2, ContentType.getPdfId() );
        if( language != null )
          psql.setString( 3, language.code );
        else
          psql.setNull( 3, Types.VARCHAR );
        psql.setObject( 4, time );
        psql.setBinaryStream( 5, new ByteArrayInputStream( data ));
        query.executeUpdate( psql );
      } catch( Exception ex ) {
        query.rollback();
        LOG.error( "Failed to insert into content table", ex );
        throw ex;
      }
      // Update ElasticSearch Index
      JsonFactory jsonfactory = new JsonFactory();
      try( StringWriter writer = new StringWriter() ) {
        try( JsonGenerator json = jsonfactory.createGenerator( writer )) {
          json.writeStartObject();
          json.writeStringField( "id", id );
          json.writeArrayFieldStart( "tenants" );
          for( Tenant tenant : tenants )
            json.writeString( tenant.name );
          json.writeEndArray();
          json.writeStringField( "publicationTime", publicationTime.format( DATETIME ));
          if( publicUntil == null )
            json.writeNullField( "publicUntil" );
          else
            json.writeStringField( "publicUntil", publicUntil.format( DATETIME ));
          json.writeStringField( "status", "published" );
          json.writeEndObject();
        }
        HttpEntity entity = ElasticSearch.httpJsonEntity( writer.toString() );
        Response response = ElasticSearch.client().performRequest( "POST", "/archive/issue/" + bytesToHex( documentId ), new HashMap<>(), entity );
        int status = response.getStatusLine().getStatusCode();
        if( status != 200 && status != 201 ) {
          query.rollback();
          throw new Exception( "Failed to update Elasticsearch index" );
        }
      }
      query.commit();
      archiveTime = time;
      if( addition )
        LOG.info( "Document stored with ID: " + bytesToHex( documentId ));
      else
        LOG.info( "Content added to document with ID: " + bytesToHex( documentId ));        
    } catch( Exception ex ) {
      throw ex;
    }
  }
}