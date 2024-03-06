package com.systemfive.archive.entity;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.systemfive.archive.db.OracleQuery;
import com.systemfive.archive.index.ElasticSearch;
import com.systemfive.archive.index.Result;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.sql.Blob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.http.HttpEntity;
import org.elasticsearch.client.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Notice extends Document {
  
  static final Logger LOG = LoggerFactory.getLogger( Notice.class.getName() );

  public Notice() {
    documentType = 1;
  }

  public Notice( Tenant tenant, String id ) {
    super( tenant, id );
    documentType = 1;
  }

  public Notice( String id ) {
    super( id );
    documentType = 1;
  }

  public static Notice get( Tenant tenant, String[] id, String[] languages ) {
    try( OracleQuery query = new OracleQuery() ) {
      boolean isEhraId = false;
      if( id[0].length() == 10 && id[0].matches( "1\\d+" )) {
        LOG.info( "EHRA ID detected, looking for HR0[123]-" + id[0] + " instead" );
        isEhraId = true;
      }
      PreparedStatement psql;
        if( tenant.subtenant == null || tenant.subtenant.isEmpty() ){
            psql = query.prepare(
                "SELECT d.heading, d.subheading, d.submitter, d.cantons, d.issue, d.publication_time, d.public_until, c.language_code, c.title, c.archive_time, c.text"
                + " FROM document d"
                + " INNER JOIN content c ON c.document_id = d.id"
                + " INNER JOIN tenant_document td ON td.document_id = d.id"
                + " WHERE d.document_status_id = 1 AND td.document_type_id = 1 AND td.tenant_id = ? AND c.content_type_id = ? "
                + "   AND td.identifier " + ( isEhraId ? "IN (?,?,?)" : "= ?" ),
                ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE
            );
            psql.setInt( 1, tenant.id );
            psql.setInt( 2, ContentType.getPdfId() );
            if( isEhraId ) {
                psql.setString( 3, "HR01-" + id[0] );
                psql.setString( 4, "HR02-" + id[0] );
                psql.setString( 5, "HR03-" + id[0] );
            } else {
                psql.setString( 3, id[0] );
            }
        } else {
            psql = query.prepare(
                "SELECT d.heading, d.subheading, d.submitter, d.cantons, d.issue, d.publication_time, d.public_until, c.language_code, c.title, c.archive_time, c.text"
                + " FROM document d"
                + " INNER JOIN content c ON c.document_id = d.id"
                + " INNER JOIN tenant_document td ON td.document_id = d.id"
                + " WHERE d.document_status_id = 1 AND td.document_type_id = 1 AND td.tenant_id = ? AND c.content_type_id = ? AND td.subtenant = ?"
                + "   AND td.identifier " + ( isEhraId ? "IN (?,?,?)" : "= ?" ),
                ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE
            );
            psql.setInt( 1, tenant.id );
            psql.setInt( 2, ContentType.getPdfId() );
            psql.setString( 3, tenant.subtenant );
            if( isEhraId ) {
                psql.setString( 4, "HR01-" + id[0] );
                psql.setString( 5, "HR02-" + id[0] );
                psql.setString( 6, "HR03-" + id[0] );
            } else {
                psql.setString( 4, id[0] );
            }
        }
        try( ResultSet rs = query.executeQuery( psql )) {
          query.filterLanguage( rs, languages, 8 );
          if( !rs.isAfterLast() ) {
            Notice notice = new Notice( tenant, id[0] );
            notice.heading( rs.getString( 1 ));
            notice.subheading( rs.getString( 2 ));
            notice.submitter( rs.getString( 3 ));
            String cantons = rs.getString( 4 );
            if( cantons != null )
              notice.cantons().addAll( Arrays.asList( cantons.split( "," )));
            notice.issue( rs.getInt( 5 ));
            notice.publicationTime( rs.getObject( 6, LocalDateTime.class ));
            notice.publicUntil(rs.getObject( 7, LocalDateTime.class ));
            notice.language( Language.get( rs.getString( 8 )));
            notice.title(rs.getString( 9 ));
            notice.archiveTime( rs.getObject( 10, LocalDateTime.class ));
            Blob blob = rs.getBlob( 11 );
            if( blob != null )
              notice.text( blob.getBytes( 1, (int) blob.length() ));
            return notice;
          } else {
            return null;
          }
        }
    } catch( Exception ex ) {
      LOG.error( "Could not get notice", ex );
    }
    return null;
  }

  public static Notice getLazy( Tenant tenant, String[] id, String[] languages, boolean getFields, ContentType contentType ) {
    Notice notice;
    if( getFields )
      notice = Notice.get( tenant, id, languages );
    else
      notice = new Notice();
    if( contentType != null ) {
      notice.data = Document.getContent( 1, tenant, id, languages, contentType );
      if( notice.data == null )
        return null;
    }
    return notice;
  }

  public static Result search( String tenantName, String subtenant, String id, String[] languages, List<String> cantons, List<String> headings, List<String> subheadings, String title, String text, String submitter, LocalDateTime publicationTimeFrom, LocalDateTime publicationTimeTo, int page, int pagesize, boolean isPublic ) {
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
        if( !cantons.isEmpty() ) {
          json.writeStartObject();
          json.writeObjectFieldStart( "terms" );
          json.writeArrayFieldStart( "cantons" );
          for( String canton : cantons )
            json.writeString( canton );
          json.writeEndArray();
          json.writeEndObject();
          json.writeEndObject();
        }
        if( !headings.isEmpty() && !subheadings.isEmpty() ) {
          json.writeStartObject();
          json.writeObjectFieldStart( "bool" );
          json.writeArrayFieldStart( "should" );
          json.writeStartObject();
          json.writeObjectFieldStart( "terms" );
          json.writeArrayFieldStart( "heading" );
          for( String heading : headings )
            json.writeString( heading );
          json.writeEndArray();
          json.writeEndObject();
          json.writeEndObject();
          json.writeStartObject();
          json.writeObjectFieldStart( "terms" );
          json.writeArrayFieldStart( "subheading" );
          for( String subheading : subheadings )
            json.writeString( subheading );
          json.writeEndArray();
          json.writeEndObject();
          json.writeEndObject();
          json.writeEndArray();
          json.writeEndObject();
          json.writeEndObject();
        } else {
          if( !headings.isEmpty() ) {
            json.writeStartObject();
            json.writeObjectFieldStart( "terms" );
            json.writeArrayFieldStart( "heading" );
            for( String heading : headings )
              json.writeString( heading );
            json.writeEndArray();
            json.writeEndObject();
            json.writeEndObject();
          } else if( !subheadings.isEmpty() ) {
            json.writeStartObject();
            json.writeObjectFieldStart( "terms" );
            json.writeArrayFieldStart( "subheading" );
            for( String subheading : subheadings )
              json.writeString(subheading);
            json.writeEndArray();
            json.writeEndObject();
            json.writeEndObject();
          }
        }
        boolean isFulltextSearch = false;
        if( text != null && !text.isEmpty() ) {
          json.writeStartObject();
          json.writeObjectFieldStart( "query_string" );
          json.writeArrayFieldStart( "fields" );
          json.writeString( "title" );
          json.writeString( "text" );
          json.writeEndArray();
          json.writeStringField( "default_operator", "AND" );
          json.writeStringField( "query", convertSyntax( text ));
          json.writeEndObject();
          json.writeEndObject();
          isFulltextSearch = true;
        }
        if( title != null && !title.isEmpty() ) {
          json.writeStartObject();
          json.writeObjectFieldStart( "match" );
          json.writeStringField( "title", title );
          json.writeEndObject();
          json.writeEndObject();
          isFulltextSearch = true;
        }       
        if( submitter != null && !submitter.isEmpty() ) {
          json.writeStartObject();
          json.writeObjectFieldStart( "match" );
          json.writeStringField( "submitter", submitter );
          json.writeEndObject();
          json.writeEndObject();
        }
        if( publicationTimeFrom != null || publicationTimeTo != null ) {
          json.writeStartObject();
          json.writeObjectFieldStart( "range" );
          json.writeObjectFieldStart( "publicationTime" );
          if( publicationTimeFrom != null )
            json.writeStringField( "gte", publicationTimeFrom.format( datetime ));
          if (publicationTimeTo != null)
            json.writeStringField( "lte", publicationTimeTo.format( datetime ));
          json.writeEndObject();
          json.writeEndObject();
          json.writeEndObject();
        }
        if( isPublic ) {
          if( isFulltextSearch ) {
            json.writeStartObject();
            json.writeObjectFieldStart( "bool" );
            json.writeArrayFieldStart( "should" );
            json.writeStartObject();
            json.writeObjectFieldStart( "term" );
            json.writeBooleanField( "noFulltextSearch", false );
            json.writeEndObject();
            json.writeEndObject();
            json.writeStartObject();
            json.writeObjectFieldStart( "missing" );
            json.writeStringField( "field", "noFulltextSearch" );
            json.writeEndObject();
            json.writeEndObject();
            json.writeEndArray();
            json.writeEndObject();
            json.writeEndObject();
          }
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
    LOG.info( "Running the following Elasticsearch query: " + query );
    HttpEntity entity = ElasticSearch.httpJsonEntity( query );
    try {
      Response response = ElasticSearch.client().performRequest( "POST", "/archive/notice/_search", params, entity );
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
  
  public static void addContent( Tenant tenant, String id, Language language, ContentType contentType, byte[] content ) throws Exception {
    LocalDateTime time = LocalDateTime.now();
    try( OracleQuery query = new OracleQuery() ) {
      query.disableAutoCommit();
      byte[] documentId = null;
      // Get document id
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
          }
        }
      }
      if( documentId == null )
        throw new SQLException( "Document does not exist", null, 9999 );
      int count = 0;
      try( PreparedStatement psql = query.prepare(
        "SELECT count(*) "
        + "FROM content "
        + "WHERE document_id = ? "
        + "  AND content_type_id = ?"
        + "  AND language_code = ?"
      )) {
        psql.setBytes( 1, documentId );
        psql.setInt( 2, contentType.id );
        if( language != null )
          psql.setString( 3, language.code );
        else
          psql.setNull( 3, Types.VARCHAR );
        try( ResultSet rs = query.executeQuery( psql )) {
          if( rs.next() ) {
            count = rs.getInt( 1 );
          }
        }
      }
      if( count > 0 )
        throw new SQLException( "Content already exist", null, 9998 );
      // Insert content
      try( PreparedStatement psql = query.prepare(
        "INSERT INTO content"
        + " ( document_id, content_type_id, language_code, title, archive_time, data, text )"
        + " VALUES ( ?, ?, ?, ?, ?, ?, ? )"
      )) {
        psql.setBytes( 1, documentId );
        psql.setInt( 2, contentType.id );
        if( language != null )
          psql.setString( 3, language.code );
        else
          psql.setNull( 3, Types.VARCHAR );
        psql.setNull( 4, Types.VARCHAR );
        psql.setObject( 5, time );
        psql.setBinaryStream( 6, new ByteArrayInputStream( compress( content )));
        psql.setNull( 7, Types.BLOB );
        query.executeUpdate( psql );
      } catch( Exception ex ) {
        query.rollback();
        throw new SQLException( "Failed to insert into content table", ex );
      }  
      query.commit();
      LOG.info( "Content added to document with ID: " + bytesToHex( documentId ));        
    } catch( Exception ex ) {
      throw ex;
    }
  }

  private static String convertSyntax( String text ) {
    if( text == null || text.isEmpty() )
      return text;
    String syntax = text.replaceAll( "\\+*\\|\\+*", " OR " );
    LOG.info( "Elasticsearch syntax: " + syntax );
    return syntax;
  }

  public void save() throws Exception {
    LocalDateTime time = LocalDateTime.now();
    try( OracleQuery query = new OracleQuery() ) {
      query.disableAutoCommit();
      byte[] documentId = null;
      // check if this document already exist
      for (Tenant tenant : tenants) {
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
      if (documentId == null) {
        // create new GUID
        try( ResultSet rs = query.get( "SELECT SYS_GUID() FROM DUAL" )) {
          rs.next();
          documentId = rs.getBytes( 1 );
        }
        // document
        try( PreparedStatement psql = query.prepare(
          "INSERT INTO document"
          + " ( id, document_status_id, heading, subheading, submitter, cantons, issue, publication_time, public_until )"
          + " VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ? )"
        )) {
          psql.setBytes( 1, documentId );
          psql.setInt( 2, documentStatus );
          psql.setString( 3, heading );
          psql.setString( 4, subheading );
          psql.setString( 5, submitter );
          // Uniquify and sort cantons
          Set<String> cantonSet = new HashSet<>();
          cantonSet.addAll( cantons );
          cantons.clear();
          cantons.addAll( cantonSet );
          Collections.sort( cantons );
          psql.setString( 6, String.join( ",", cantons ));
          if( issue != null )
            psql.setInt( 7, issue );
          else
            psql.setNull( 7, Types.NUMERIC );
          psql.setObject( 8, publicationTime );
          psql.setObject( 9, publicUntil );
          query.executeUpdate(psql);
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
        } catch( Exception ex ) {
          query.rollback();
          LOG.error( "Failed to insert into tenant_document table", ex );
          throw ex;
        }
      }
      // content
      try( PreparedStatement psql = query.prepare(
        "INSERT INTO content"
        + " ( document_id, content_type_id, language_code, title, archive_time, data, text )"
        + " VALUES ( ?, ?, ?, ?, ?, ?, ? )"
      )) {
        psql.setBytes( 1, documentId );
        psql.setInt( 2, ContentType.getPdfId() );
        if( language != null )
          psql.setString( 3, language.code );
        else
          psql.setNull( 3, Types.VARCHAR );
        psql.setString( 4, title );
        psql.setObject( 5, time );
        psql.setBinaryStream( 6, new ByteArrayInputStream( data ));
        if( text != null )
          psql.setBinaryStream( 7, new ByteArrayInputStream( compress( text.getBytes() )));
        else
          psql.setNull( 7, Types.BLOB );
        query.executeUpdate( psql );
      } catch( Exception ex ) {
        query.rollback();
        LOG.error( "Failed to insert into content table", ex );
        throw ex;
      }  
      // Update ElasticSearch Index
      // Disable for Bulk import, might be a good thing to make this configurable via config property
      JsonFactory jsonfactory = new JsonFactory();
      try( StringWriter writer = new StringWriter() ) {
        try( JsonGenerator json = jsonfactory.createGenerator( writer )) {
          json.writeStartObject();
          json.writeStringField( "id", id );
          json.writeArrayFieldStart( "tenants" );
          for( Tenant tenant : tenants )
            json.writeString( tenant.name );
          json.writeEndArray();
          json.writeArrayFieldStart( "cantons" );
          for( String canton : cantons )
            json.writeString( canton );
          json.writeEndArray();
          json.writeStringField( "heading", heading );
          json.writeStringField( "subheading", subheading );
          json.writeStringField( "submitter", submitter );
          json.writeStringField( "title", title );
          json.writeStringField( "text", text );
          json.writeStringField( "publicationTime", publicationTime.format( DATETIME ));
          if( publicUntil == null )
            json.writeNullField( "publicUntil" );
          else
            json.writeStringField( "publicUntil", publicUntil.format( DATETIME ));
          json.writeStringField( "status", "published" );
          json.writeBooleanField( "noFulltextSearch", false );
          json.writeEndObject();
        }
        HttpEntity entity = ElasticSearch.httpJsonEntity( writer.toString() );
        Response response = ElasticSearch.client().performRequest( "POST", "/archive/notice/" + bytesToHex( documentId ), new HashMap<>(), entity );
        int status = response.getStatusLine().getStatusCode();
        if( status != 200 && status != 201 ) {
          query.rollback();
          throw new Exception( "Failed to update Elasticsearch Index" );
        }
      }
      query.commit();
      archiveTime = time;
      LOG.info( "Document stored with ID: " + bytesToHex( documentId ));
    } catch( Exception ex ) {
      throw ex;
    }
  }

  @Override
  public List<Tenant> tenants() {
    return tenants;
  }

  public List<String> cantons() {
    return cantons;
  }

  public String heading() {
    return heading;
  }

  public void heading( String heading ) {
    this.heading = heading;
  }

  public String subheading() {
    return subheading;
  }

  public void subheading( String subheading ) {
    this.subheading = subheading;
  }

  public String title() {
    return title;
  }

  public void title( String title ) {
    this.title = title;
  }

  public String submitter() {
    return submitter;
  }

  public void submitter( String submitter ) {
    this.submitter = submitter;
  }

  public String text() {
    return text;
  }

  public void text( String text ) {
    this.text = text;
  }

  public void text( byte[] text ) {
    this.text = new String( uncompress( text ));
  }

}