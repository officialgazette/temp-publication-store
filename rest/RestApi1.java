package com.systemfive.archive.rest;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.systemfive.archive.config.Config;
import com.systemfive.archive.db.OracleQuery;
import com.systemfive.archive.entity.ContentType;
import com.systemfive.archive.entity.Language;
import com.systemfive.archive.entity.Notice;
import com.systemfive.archive.entity.Issue;
import com.systemfive.archive.entity.Tenant;
import com.systemfive.archive.index.HitEntry;
import com.systemfive.archive.index.Result;
import java.io.IOException;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.rapidoid.buffer.Buf;
import org.rapidoid.data.BufRange;
import org.rapidoid.data.KeyValueRanges;
import org.rapidoid.http.HttpStatus;
import org.rapidoid.http.impl.HttpParser;
import org.rapidoid.io.Upload;
import org.rapidoid.net.abstracts.Channel;
import org.rapidoid.net.impl.RapidoidHelper;

public class RestApi1 implements RestApi {

  static final Logger LOG = LoggerFactory.getLogger( RestApi1.class.getName() );
  private static final HttpParser HTTP_PARSER = new HttpParser();
  private static final DateTimeFormatter DATETIME = DateTimeFormatter.ofPattern( "yyyy-MM-dd'T'HH:mm:ss.SSS" );
  private static final DateTimeFormatter DATETIMEZONE = DateTimeFormatter.ofPattern( "yyyy-MM-dd'T'HH:mm:ss.SSSZ" );

  @Override
  public HttpStatus handle( HttpServer http, Channel ctx, Buf buf, RapidoidHelper req, String[] path, int index ) {
    boolean isPublic = false;
    String method = buf.get(req.verb);
    RestApi1Response response = new RestApi1Response(http, ctx, req.isKeepAlive.value);
    if( index >= path.length )
      response.error( 404, "Not found" );
    if( path.length > index && path[index].equals( "public" )) {
      isPublic = true;
      index++;
    }
    KeyValueRanges params = new KeyValueRanges( 100 );
    HTTP_PARSER.parseParams( buf, params, req.query );
    String[] default_languages = {"en", "de"};
    String[] languages = null;
    int i;
    for( i = 0; i < params.keys.length; i++ ) {
      BufRange key = params.keys[i];
      switch( buf.get( key )) {
        case "lang":
          languages = new String[default_languages.length + 1];
          String[] this_lang = { buf.get( params.values[i] ) };
          System.arraycopy( this_lang, 0, languages, 0, 1 );
          System.arraycopy( default_languages, 0, languages, 1, default_languages.length );
          break;
      }
    }
    if( languages == null )
      languages = default_languages;
    if( index < path.length ) {
      String tenant;
      String[] fullTenant;
      String type;
      String[] id_array;
      switch( path[index] ) {
        case "document":
          index++;
          if( path.length == index )
            return response.error( 400, "Incomplete request" );
          tenant = null;
          type = "notice";
          if( path.length == index + 1 ) {
            //document/id
          } else if( path[index].equals( "notice" ) || path[index].equals( "issue" )) {
            //document/type/id
            type = path[index];
            index++;
          } else if( path.length == index + 2 ) {
            //document/tenant/id
            tenant = path[index];
            index++;
          } else {
            //document/tenant/type/id
            tenant = path[index];
            type = path[index + 1];
            index += 2;
          }
          fullTenant = splitTenant(tenant);
          id_array = Arrays.copyOfRange( path, index, path.length );
          String last_id = id_array[id_array.length - 1];
          ContentType contentType = ContentType.getByFilename( last_id );
          if( contentType != null )
            id_array[id_array.length - 1] = last_id.substring( 0, last_id.length() - contentType.extension.length() - 1 );
          RapidoidHelper helper = new RapidoidHelper();
          KeyValueRanges headers = new KeyValueRanges( req.headers.count );
          HTTP_PARSER.parseHeadersIntoKV( buf, req.headers, headers, null, helper );
          switch( method ) {
            case "GET":
              return get( response, fullTenant[0], fullTenant[1], type, id_array, languages, contentType, hasGzip( headers, buf ));
            case "PUT":
              Map<String, List<Upload>> files = new HashMap<>();
              try {
                Map<String, Object> dest = new HashMap<>();
                HTTP_PARSER.parsePosted( buf, headers, req.body, params, files, helper, dest );
              } catch( RuntimeException ex ) {
                LOG.error( "Too many parts in multipart content", ex );
                return response.error( 422, "Too many parts in multipart content" );
              }
              if( files.isEmpty() )
                return response.error( 422, "No content file attached" );
              List values = (List) files.values().toArray()[0];
              Upload file = (Upload) values.toArray()[0];
              String key = (String) files.keySet().toArray()[0];
              String jsonString = null;
              if( params.values.length > 0 && !buf.get( params.keys[0] ).equals( "lang" ))
                jsonString = buf.get( params.values[0] );
              if( jsonString == null || jsonString.isEmpty() ) {
                return addContent( response, tenant, type, id_array, languages[0], file.content(), ContentType.get( key ));
              } else {
                Map<String, Object> json = parseJson( jsonString );
                return store( response, tenant, type, id_array, json, file.content() );
              }
            default:
              return response.error( 405, "Method not allowed" );
          }
        case "search":
          if ( !method.equals( "GET" ))
            return response.error( 405, "Method not allowed" );
          index++;
          if( path.length > index )
            tenant = path[index];
          else
            tenant = "";
          if( path.length > index + 2 )
            return response.error( 400, "Request path too long" );
          index++;
          if( path.length > index )
            type = path[index];
          else
            type = "";
          List<String> cantons = new ArrayList<>();
          List<String> headings = new ArrayList<>();
          List<String> subheadings = new ArrayList<>();
          String id = "";
          String title = "";
          String text = "";
          String submitter = "";
          LocalDateTime publicationTimeFrom = null;
          LocalDateTime publicationTimeTo = null;
          i = 0;
          int page = 1;
          int pagesize = 10;
          for( BufRange key : params.keys ) {
            switch( buf.get( key )) {
              case "tenant":
                if( tenant.isEmpty() )
                  tenant = buf.get( params.values[i] );
                break;
              case "type":
                if( type.isEmpty() )
                  type = buf.get( params.values[i] );
                break;
              case "id":
                id = buf.get( params.values[i] ).replaceAll( "^0+", "" );
                break;
              case "canton":
                cantons.addAll( Arrays.asList(( buf.get( params.values[i] )).split( "," )));
                break;
              case "heading":
                headings.addAll( Arrays.asList(( buf.get( params.values[i] )).split( "," )));
                break;
              case "subheading":
                subheadings.addAll( Arrays.asList(( buf.get( params.values[i] )).split( "," )));
                break;
              case "title":
                try {
                  title = urlDecode( buf.get( params.values[i] ));
                } catch( UnsupportedEncodingException ex ) {
                  LOG.error( "Unsupported encoding", ex);
                  return response.error( 422, "Unsupported encoding" );
                }
                break;
              case "notice":
                try {
                  text = urlDecode( buf.get( params.values[i] ));
                } catch( UnsupportedEncodingException ex ) {
                  LOG.error( "Unsupported encoding", ex);
                  return response.error( 422, "Unsupported encoding" );
                }
                break;
              case "submitter":
                try {
                  submitter = urlDecode( buf.get( params.values[i] ));
                } catch( UnsupportedEncodingException ex ) {
                  LOG.error( "Unsupported encoding", ex);
                  return response.error( 422, "Unsupported encoding" );
                }
                break;
              case "publicationTime":
                publicationTimeFrom = toDateTime( buf.get( params.values[i] ), false );
                publicationTimeTo = toDateTime( buf.get( params.values[i] ), true );
                if( publicationTimeFrom == null || publicationTimeTo == null )
                  return response.error( 422, "Error while parsing publicationTime" );
                break;
              case "publicationTime.from":
                publicationTimeFrom = toDateTime( buf.get( params.values[i] ), false );
                if( publicationTimeFrom == null )
                  return response.error( 422, "Error while parsing publicationTime.from" );
                break;
              case "publicationTime.to":
                publicationTimeTo = toDateTime( buf.get( params.values[i] ), true );
                if( publicationTimeTo == null )
                  return response.error( 422, "Error while parsing publicationTime.to" );
                break;
              case "page":
                try {
                  page = Integer.parseInt( buf.get( params.values[i] ));
                } catch( NumberFormatException ex ) {
                  return response.error( 422, "Page needs to be numeric" );
                }
                break;
              case "pagesize":
                try {
                  pagesize = Integer.parseInt( buf.get(params.values[i] ));
                } catch( NumberFormatException ex ) {
                  return response.error( 422, "Pagesize needs to be numeric" );
                }
                break;
            }
            i++;
          }
          if( type.isEmpty() )
            type = "notice";
          fullTenant = splitTenant(tenant);
          return search( response, fullTenant[0], fullTenant[1], type, id, languages, cantons, headings, subheadings, title, text, submitter, publicationTimeFrom, publicationTimeTo, page, pagesize, isPublic );
        case "admin":
          index++;
          switch( path[index] ) {
            case "ping":
              try( OracleQuery query = new OracleQuery() ) {
                String test = null;
                try( ResultSet rs = query.get( "SELECT 'test' FROM DUAL" )) {
                  if( rs != null && rs.next() )
                    test = rs.getString( 1 );
                } catch( Exception ex ) {
                  LOG.error( "Ping: could not get string from database", ex );
                }
                if( test != null && test.equals( "test" ))
                  return response.json(( "{\"ping\":\"pong\"}\n" ).getBytes() );
                else
                  return response.error( 422, "Could not get data from database" );
              }
            case "purge":
              if( Config.profile().equals( "development" )) {
                Boolean success = false;
                try( OracleQuery query = new OracleQuery() ) {
                  query.execute( "TRUNCATE TABLE content DROP STORAGE" );
                  query.execute( "TRUNCATE TABLE tenant_document DROP STORAGE");
                  query.execute( "ALTER TABLE document MODIFY CONSTRAINT fk_document_superseeded_by DISABLE");
                  query.execute( "TRUNCATE TABLE tenant_document DROP STORAGE");
                  query.execute( "ALTER TABLE document MODIFY CONSTRAINT fk_document_superseeded_by ENABLE");
                  success = true;
                  LOG.info( "Database was purged successfully" );
                } catch( Exception ex ) {
                  LOG.error( "Could not purge archive storage", ex );
                }
                if( success )
                  return response.json(( "{\"purged\":\"true\"}\n" ).getBytes() );
                else
                  return response.error( 500, "An error occured while purging archive database" );
              } else {
                LOG.warn( "Purge not allowed on current profile: " + Config.profile() );
                return response.error( 403, "Not allowed" );
              }
          }
      }
    }
    return response.error( 404, "Not found" );
  }

  private HttpStatus get( RestApi1Response response, String tenantName, String subtenant, String type, String[] id, String[] languages, ContentType contentType, boolean hasGzip ) {
    Tenant tenant = Tenant.get( tenantName );
    tenant.subtenant = subtenant;
    
    if( tenant == null )
      return response.error( 400, "Tenant not found" );
    switch( type ) {
      case "notice":
        if( id.length > 1 )
          return response.error( 400, "Request too long" );
        Notice notice;
        if( contentType != null ) {
          notice = Notice.getLazy( tenant, id, languages, false, contentType );
          if( notice != null ) {
            if( hasGzip )
              return response.content( notice.data( false ), contentType, true );
            else
              return response.content( notice.data(), contentType );
          }
        } else {
          notice = Notice.getLazy( tenant, id, languages, true, contentType );
          if( notice == null )
            return response.error( 404, "Document not found" );
          JsonFactory jsonfactory = new JsonFactory();
          try( StringWriter writer = new StringWriter() ) {
            try( JsonGenerator json = jsonfactory.createGenerator( writer )) {
              json.writeStartObject();
              writeJson( json, "id", notice.id() );
              writeJson( json, "tenants", notice.tenantNames() );
              writeJson( json, "cantons", notice.cantons() );
              writeJson( json, "heading", notice.heading() );
              writeJson( json, "subheading", notice.subheading() );
              writeJson( json, "submitter", notice.submitter() );
              writeJson( json, "language", notice.language() );
              writeJson( json, "issue", notice.issue() );
              writeJson( json, "title", notice.title() );
              writeJson( json, "notice", notice.text() );
              writeJson( json, "publicationTime", notice.publicationTime() );
              writeJson( json, "publicUntil", notice.publicUntil() );
              writeJson( json, "archiveTime", notice.archiveTime() );
              json.writeEndObject();
            }
            return response.json( writer.toString().getBytes() );
          } catch( IOException ex ) {
            LOG.error( "Could not build JSON document", ex );
          }
        }
      case "issue":
        if( id.length > 2 )
          return response.error(400, "Request too long");
        Issue issue;
        if( contentType != null ) {
          issue = Issue.getLazy( tenant, id, languages, false, contentType );
          if( issue != null ) {
            if( hasGzip )
              return response.content( issue.data( false ), contentType, true );
            else
              return response.content( issue.data(), contentType );
          }
        } else {
          issue = Issue.getLazy( tenant, id, languages, true, contentType );
          if( issue == null )
            return response.error( 404, "Document not found" );
          JsonFactory jsonfactory = new JsonFactory();
          try( StringWriter writer = new StringWriter() ) {
            try( JsonGenerator json = jsonfactory.createGenerator( writer )) {
              json.writeStartObject();
              writeJson( json, "id", issue.id() );
              writeJson( json, "tenants", issue.tenantNames() );
              writeJson( json, "language", issue.language() );
              writeJson( json, "issue", issue.issue() );
              writeJson( json, "publicationTime", issue.publicationTime() );
              writeJson( json, "publicUntil", issue.publicUntil() );
              writeJson( json, "archiveTime", issue.archiveTime() );
              json.writeEndObject();
            }
            return response.json( writer.toString().getBytes() );
          } catch( IOException ex ) {
            LOG.error( "Could not build JSON document", ex );
          }
        }
    }
    return response.error( 404, "Document not found" );
  }

  private HttpStatus store( RestApi1Response response, String tenantName, String type, String[] a_id, Map<String, Object> json, byte[] pdf ) {
    LOG.info( "The JSON file looks like this: " + json );
    String id = a_id[0];
    if( json == null )
      return response.error( 400, "JSON parser error" );
    List<String[]> tenants = new ArrayList<>();
    if( tenantName != null ) {
      String[] parentTenant = new String[2];
      if(tenantName.contains("-" )) {
        parentTenant = tenantName.split("-");
      }
      else {
        parentTenant[0] = tenantName;
        parentTenant[1] = "";
      }
      tenants.add( parentTenant );
    }
    else {
      List<String> jsonTenants = getJsonStringArray( json, "tenants" );
        for ( String tenantString : jsonTenants ) {
          String[] parentTenant = new String[2];
          if(tenantString.contains("-" )) {
            parentTenant = tenantString.split("-");
          }
          else {
            parentTenant[0] = tenantString;
            parentTenant[1] = "";
          }
          tenants.add( parentTenant );
        }
    }
    if( tenants.isEmpty() )
      return response.error( 400, "No tenant defined" );
    for( String[] tenantString : tenants ) {
      Tenant tenant = Tenant.get( tenantString[0] );
      if( tenant == null ) {
        try {
          Tenant.create( tenantString[0] );
        } catch (Exception ex) {
          return response.error( 400, "Unable to create tenant: " + tenantString[0] + "(" + ex.getLocalizedMessage() + ")" );
        }
      }
    }
    String languageCode = getJsonString( json, "language" );
    Language language = null;
    if ( !isEmpty( languageCode ))
      language = Language.get( languageCode.toLowerCase() );
    LocalDateTime publicationTime;
    String default_error_msg;
    switch( type ) {
      case "notice":
        Notice notice = new Notice( id );
        for( String[] tenantString : tenants ) {
          Tenant newTenant = Tenant.get( tenantString[0] );
          if ( tenantString[1].isEmpty() ) {
              newTenant.subtenant = null;
          }
          else {
              newTenant.subtenant = tenantString[1];
          }
          notice.tenants().add( newTenant );
        }
        if( notice.tenants().isEmpty() )
          return response.error( 400, "No tenant specified" );
        notice.language( language );
        String heading = getJsonString( json, "heading" );
        if( isEmpty( heading ))
          return response.error( 400, "Heading not specified" );
        notice.heading( heading.toLowerCase() );
        String subheading = getJsonString( json, "subheading" );
        if( isEmpty( subheading ))
          return response.error( 400, "Subheading not specified" );
        notice.subheading( subheading.toLowerCase() );
        String canton = getJsonString( json, "canton" );
        if( !isEmpty( canton ))
          notice.cantons().add(canton);
        List<String> cantons = getJsonStringArray( json, "cantons" );
        if( cantons != null ) {
          cantons.forEach( cantonString -> {
            notice.cantons().add( cantonString );
          });
        }
        String title = getJsonString( json, "title" );
        if( isEmpty( title ))
          return response.error( 400, "Title not specified" );
        notice.title( title );
        notice.text( getJsonString( json, "notice" ));
        String submitter = getJsonString( json, "submitter" );
        if( isEmpty( submitter ))
          return response.error( 400, "Submitter not specified" );
        notice.submitter( submitter );
        notice.issue( getJsonInteger( json, "issue" ));
        publicationTime = getJsonDateTime( json, "publicationTime" );
        if( isEmpty( publicationTime ))
          return response.error( 400, "publicationTime not specified (or wrong format)" );
        notice.publicationTime( publicationTime );
        notice.publicUntil( getJsonDateTime( json, "publicUntil", true ));
        notice.data( pdf );
        default_error_msg = "Exception while storing notice";
        try {
          notice.save();
        } catch( SQLException ex ) {
          if( ex.getErrorCode() == 00001 ) { // ORA-00001: unique constraint violation
            LOG.info( "Notice already exists: " + notice.id() );
            return response.error( 400, "Notice already exists" );
          } else {
            LOG.error( default_error_msg, ex );
            return response.error( 400, default_error_msg );
          }
        } catch( Exception ex ) {
          LOG.error( default_error_msg, ex );
          return response.error( 400, default_error_msg );
        }
        break;
      case "issue":
        Issue issue = new Issue( id );
        issue.language( language );
        for( String[] tenantString : tenants ) {
          Tenant newTenant = Tenant.get( tenantString[0] );
          if ( tenantString[1].isEmpty() ) {
              newTenant.subtenant = null;
          }
          else {
              newTenant.subtenant = tenantString[1];
          }
          issue.tenants().add( newTenant );
        }
        if( issue.tenants().isEmpty() )
          return response.error( 400, "No tenant specified" );
        issue.issue( getJsonInteger( json, "issue" ));
        publicationTime = getJsonDateTime( json, "publicationTime" );
        if( isEmpty( publicationTime ))
          return response.error( 400, "publicationTime not specified (or wrong format)" );
        issue.publicationTime( publicationTime );
        issue.publicUntil( getJsonDateTime( json, "publicUntil", true ));
        issue.data( pdf );
        default_error_msg = "Exception while storing issue";
        try {
          issue.save();
        } catch( SQLException ex ) {
          if( ex.getErrorCode() == 00001) { // ORA-00001: unique constraint violation
            LOG.info( "Issue already exists: " + issue.id() );
            return response.error( 400, "Issue already exists" );
          } else {
            LOG.error( default_error_msg, ex );
            return response.error( 400, default_error_msg );
          }
        } catch( Exception ex ) {
          LOG.error( default_error_msg, ex );
          return response.error( 400, default_error_msg );
        }
        break;
      default:
        return response.error( 400, "Unknown document type: " + type );
    }
    return response.message( "Document stored" );
  }

  private HttpStatus addContent( RestApi1Response response, String tenantName, String type, String[] a_id, String languageCode, byte[] content, ContentType contentType ) {
    String id = a_id[0];
    if( tenantName == null )
      return response.error( 400, "No tenant defined" );
    Tenant tenant = Tenant.get( tenantName );
    if( tenant == null )
      return response.error( 400, "Unknown tenant" );
    Language language = Language.get( languageCode.toLowerCase() );
    String default_error_msg = "Exception while adding content to notice";
    switch( type ) {
      case "notice":
        try {
          Notice.addContent( tenant, id, language, contentType, content );
        } catch( SQLException ex ) {
          if( ex.getErrorCode() == 9999 )
            return response.error( 404, "Notice does not exist" );
          if( ex.getErrorCode() == 9998 )
            return response.error( 409, "Content with content type " + contentType.type + " and language " + language.code + " already exists for notice " + id );
          LOG.error( default_error_msg, ex );
          return response.error( 400, default_error_msg );
        } catch( Exception ex ) {
          LOG.error( default_error_msg, ex );
          return response.error( 400, default_error_msg );
        }
        break;
      case "issue":
        return response.error( 400, "Cannot add additional content to issue" );
      default:
        return response.error( 400, "Unknown document type: " + type );
    }
    return response.message( "Content added" );
  }
  
  private HttpStatus search( RestApi1Response response, String tenantName, String subtenant, String type, String id, String[] language, List<String> cantons, List<String> headings, List<String> subheadings, String title, String text, String submitter, LocalDateTime publicationTimeFrom, LocalDateTime publicationTimeTo, int page, int pagesize, boolean isPublic ) {
    JsonFactory jsonfactory = new JsonFactory();
    try( StringWriter writer = new StringWriter() ) {
      Result result;
      switch( type ) {
        case "notice":
          result = Notice.search( tenantName, subtenant, id, language, cantons, headings, subheadings, title, text, submitter, publicationTimeFrom, publicationTimeTo, page, pagesize, isPublic );
          LOG.info("Total results: " + result.hits.total);
          try( JsonGenerator json = jsonfactory.createGenerator( writer )) {
            json.writeStartObject();
            if( result != null && result.hits.total > 0 ) {
              writeJson( json, "page", page );
              int pageCount = (int) Math.ceil( (float) result.hits.total / pagesize );
              writeJson( json, "pageCount", pageCount );
              writeJson( json, "resultCount", result.hits.total );
              json.writeArrayFieldStart( "results" );
              for( HitEntry hit : result.hits.hits ) {
                json.writeStartObject();
                writeJson( json, "id", hit._source.id );
                if( tenantName != null && !tenantName.isEmpty() ) {
                  writeJson( json, "tenant", tenantName );                  
                } else {
                  writeJson( json, "tenant", hit._source.tenants.get( 0 ));
                  if( hit._source.tenants.size() > 1 )
                    writeJson( json, "tenants", hit._source.tenants );
                }
                if( subtenant != null && !subtenant.isEmpty() ) {
                    writeJson( json, "subtenant", hit._source.subtenant );
                }
                writeJson( json, "heading", hit._source.heading );
                writeJson( json, "subheading", hit._source.subheading );
                writeJson( json, "publicationTime", hit._source.publicationTime );
                writeJson( json, "title", hit._source.title );
                writeJson( json, "submitter", hit._source.submitter );
                writeJson( json, "status", hit._source.status );
                json.writeEndObject();
              }
              json.writeEndArray();
            } else {
              writeJson( json, "page", 0 );
              writeJson( json, "pageCount", 0 );
              writeJson( json, "resultCount", 0 );
              json.writeArrayFieldStart( "results" );
              json.writeEndArray();
            }
            writeJson( json, "type", "notice" );
            json.writeEndObject();
            json.close();
          }
          return response.json( writer.toString().getBytes() );
        case "issue":
          result = Issue.search( tenantName, subtenant, id, publicationTimeFrom, publicationTimeTo, page, pagesize, isPublic );
          LOG.info("Total results: " + result.hits.total);
          try( JsonGenerator json = jsonfactory.createGenerator( writer )) {
            json.writeStartObject();
            if( result != null && result.hits.total > 0 ) {
              writeJson( json, "page", page );
              int pageCount = (int) Math.ceil( (float) result.hits.total / pagesize );
              writeJson( json, "pageCount", pageCount );
              writeJson( json, "resultCount", result.hits.total );
              json.writeArrayFieldStart( "results" );
              for( HitEntry hit : result.hits.hits ) {
                json.writeStartObject();
                writeJson( json, "id", hit._source.id );
                if( tenantName != null && !tenantName.isEmpty() ) {
                  writeJson( json, "tenant", tenantName );                  
                } else {
                  writeJson( json, "tenant", hit._source.tenants.get( 0 ));
                  if( hit._source.tenants.size() > 1 )
                    writeJson( json, "tenants", hit._source.tenants );
                }
                writeJson( json, "publicationTime", hit._source.publicationTime );
                writeJson( json, "status", hit._source.status );
                json.writeEndObject();
              }
              json.writeEndArray();
            } else {
              writeJson( json, "page", 0 );
              writeJson( json, "pageCount", 0 );
              writeJson( json, "resultCount", 0 );
              json.writeArrayFieldStart( "results" );
              json.writeEndArray();
            }
            writeJson( json, "type", "issue" );
            json.writeEndObject();
          }
          return response.json( writer.toString().getBytes() );
      }
    } catch( IOException ex ) {
      LOG.error( "Failed to get search results", ex );
      return response.error( 400, "Exception while fetching results" );
    }
    return null;
  }

  @SuppressWarnings( "unchecked" )
  private Map<String, Object> parseJson( String json ) {
    ObjectMapper mapper = new ObjectMapper();
    try {
      return mapper.readValue( json, HashMap.class );
    } catch( IOException ex ) {
      LOG.error( "JSON parser error", ex );
      return null;
    }
  }

  private String getJsonString( Map<String, Object> json, String key ) {
    if( json.containsKey( key ))
      return (String) json.get(key);
    return null;
  }

  @SuppressWarnings( "unchecked" )
  private List<String> getJsonStringArray( Map<String, Object> json, String key ) {
    if( json.containsKey( key ))
      return (List<String>) json.get( key );
    return null;
  }

  private Integer getJsonInteger( Map<String, Object> json, String key ) {
    if( json.containsKey( key ))
      return (Integer) json.get( key );
    return null;
  }

  private LocalDateTime getJsonDateTime( Map<String, Object> json, String key ) {
    return getJsonDateTime( json, key, false );
  }

  private LocalDateTime getJsonDateTime( Map<String, Object> json, String key, boolean isUntil ) {
    if( json.containsKey( key ))
      return toDateTime( (String) json.get( key ), isUntil);
    return null;
  }

  private void writeJson( JsonGenerator json, String field, String data ) throws IOException {
    if( data != null )
      json.writeStringField( field, data );
    else
      json.writeNullField( field );
  }

  private void writeJson( JsonGenerator json, String field, List<String> data ) throws IOException {
    if( data != null && !data.isEmpty() ) {
      json.writeArrayFieldStart( field );
      for( String entry : data )
        json.writeString( entry );
      json.writeEndArray();
    } else {
      json.writeNullField( field );
    }
  }

  private void writeJson( JsonGenerator json, String field, Integer data ) throws IOException {
    if( data != null )
      json.writeNumberField( field, data );
    else
      json.writeNullField( field );
  }

  private void writeJson( JsonGenerator json, String field, LocalDateTime data ) throws IOException {
    if( data != null )
      json.writeStringField( field, data.format( DATETIME ));
    else
      json.writeNullField( field );
  }

  private void writeJson( JsonGenerator json, String field, Language data ) throws IOException {
    if( data != null )
      json.writeStringField( field, data.code );
    else
      json.writeNullField( field );
  }

  private boolean isEmpty( String string ) {
    return ( string == null || string.isEmpty() );
  }

  private boolean isEmpty( LocalDateTime date ) {
    return ( date == null );
  }

  private LocalDateTime toDateTime( String date, boolean isUntil ) {
    if( date == null || date.isEmpty() )
      return null;
    try {
      switch( date.length() ) {
        case 10:
          if( isUntil )
            return LocalDateTime.parse( date + "T23:59:59.999", DATETIME );
          return LocalDateTime.parse( date + "T00:00:00.000", DATETIME );
        case 19:
          if( isUntil )
            return LocalDateTime.parse( date + ".999", DATETIME );
          return LocalDateTime.parse( date + ".000", DATETIME );
        case 23:
          return LocalDateTime.parse( date, DATETIME );
        default:
          return LocalDateTime.parse( date, DATETIMEZONE );
      }
    } catch( DateTimeParseException ex ) {
      LOG.error( "Date parser error", ex );
      return null;
    }
  }

  private boolean hasGzip( KeyValueRanges headers, Buf buf ) {
    for( int i = 0; i < headers.keys.length; i++ ) {
      if( buf.get( headers.keys[i] ).toLowerCase().equals( "accept-encoding" )) {
        return buf.get(headers.values[i]).contains( "gzip" );
      }
    }
    return false;
  }
  
  private String urlDecode( String encoded ) throws UnsupportedEncodingException {
    String decoded = URLDecoder.decode( encoded, "UTF-8" );
    if( Config.doubleUrlDecode() )
      decoded = URLDecoder.decode( decoded, "UTF-8" );
    return decoded;
  }
  
  private String[] splitTenant(String tenantName) {
    String[] tenant = new String[2];
    if(tenantName.contains("-" )) {
      tenant = tenantName.split("-");
    }
    else {
      tenant[0] = tenantName;
      tenant[1] = "";
    }
    
    return tenant;
  }
}