package com.systemfive.archive.entity;

import com.systemfive.archive.db.OracleQuery;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.StringWriter;
import java.sql.Blob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.Deflater;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.text.PDFTextStripper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class Document implements Entity {

  static final Logger LOG = LoggerFactory.getLogger( Document.class.getName() );
  static final DateTimeFormatter DATETIME = DateTimeFormatter.ofPattern( "yyyy-MM-dd'T'HH:mm:ss.SSS" );
  private final static char[] HEXARRAY = "0123456789ABCDEF".toCharArray();
  String id;
  final List<Tenant> tenants = new ArrayList<>();
  final List<String> cantons = new ArrayList<>();
  Language language;
  LocalDateTime publicationTime;
  LocalDateTime publicUntil;
  LocalDateTime archiveTime;
  String heading;
  String subheading;
  String title;
  String submitter;
  String text;
  Integer issue;
  int documentType;
  int documentStatus = 1;
  byte[] data;

  public Document() {
  }

  public Document( Tenant tenant, String id ) {
    this.tenants.add( tenant );
    this.id = id;
  }

  public Document( String id ) {
    this.id = id;
  }

  public static byte[] getContent( int documentType, Tenant tenant, String[] id_array, String[] languages, ContentType contentType ) {
      try( OracleQuery query = new OracleQuery() ) {
      PreparedStatement psql = null;
      if( id_array.length == 1 ) {
        String id = id_array[0];
        if( id.equals( "latest" ) && documentType == 2 ) {
          if( tenant.subtenant == null || tenant.subtenant.isEmpty() ){
            psql = query.prepare(
            "SELECT c.data, c.language_code"
            + " FROM document d"
            + " INNER JOIN content c ON c.document_id = d.id"
            + " INNER JOIN tenant_document td ON td.document_id = d.id"
            + " WHERE d.document_status_id = 1 AND td.tenant_id = ? AND td.document_type_id = 2 AND c.content_type_id = ?"
            + "   AND d.publication_time = ("
            + "     SELECT max(d2.publication_time)"
            + "     FROM document d2"
            + "     JOIN tenant_document td2 ON d2.id = td2.document_id"
            + "     WHERE td2.tenant_id = td.tenant_id"
            + "       AND td2.document_type_id = 2"
            + "       AND d2.publication_time < sysdate"
            + "       AND d2.document_status_id = 1"
            + "   )",
            ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE
          );
          psql.setInt( 1, tenant.id );
          psql.setInt( 2, contentType.id );
          } else {
            psql = query.prepare(
            "SELECT c.data, c.language_code"
            + " FROM document d"
            + " INNER JOIN content c ON c.document_id = d.id"
            + " INNER JOIN tenant_document td ON td.document_id = d.id"
            + " WHERE d.document_status_id = 1 AND td.tenant_id = ? AND td.document_type_id = 2 AND c.content_type_id = ? AND td.subtenant = ?"
            + "   AND d.publication_time = ("
            + "     SELECT max(d2.publication_time)"
            + "     FROM document d2"
            + "     JOIN tenant_document td2 ON d2.id = td2.document_id"
            + "     WHERE td2.tenant_id = td.tenant_id"
            + "       AND td2.document_type_id = 2"
            + "       AND d2.publication_time < sysdate"
            + "       AND d2.document_status_id = 1"
            + "   )",
            ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE
          );
          psql.setInt( 1, tenant.id );
          psql.setInt( 2, contentType.id );
          psql.setString( 3, tenant.subtenant );
          }
        } else {
          boolean isEhraId = false;
          if( documentType == 1 && id.length() == 10 && id.matches( "1\\d+" )) {
            LOG.info( "EHRA ID detected, looking for HR0[123]-" + id + " instead" );
            isEhraId = true;
          }
          if( tenant.subtenant == null || tenant.subtenant.isEmpty() ){
            psql = query.prepare(
            "SELECT c.data, c.language_code"
            + " FROM document d"
            + " INNER JOIN content c ON c.document_id = d.id"
            + " INNER JOIN tenant_document td ON td.document_id = d.id"
            + " WHERE d.document_status_id = 1 AND td.document_type_id = ? AND td.tenant_id = ? AND c.content_type_id = ?"
            + "   AND td.identifier " + ( isEhraId ? "IN (?,?,?)" : "= ?" ),
            ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE
            );
            psql.setInt( 1, documentType );
            psql.setInt( 2, tenant.id );
            psql.setInt( 3, contentType.id );
            if( isEhraId ) {
              psql.setString( 4, "HR01-" + id );
              psql.setString( 5, "HR02-" + id );
              psql.setString( 6, "HR03-" + id ) ;
            } else {
              psql.setString( 4, id );
            } 
          } else {
              psql = query.prepare(
            "SELECT c.data, c.language_code"
            + " FROM document d"
            + " INNER JOIN content c ON c.document_id = d.id"
            + " INNER JOIN tenant_document td ON td.document_id = d.id"
            + " WHERE d.document_status_id = 1 AND td.document_type_id = ? AND td.tenant_id = ? AND c.content_type_id = ? AND td.subtenant = ?"
            + "   AND td.identifier " + ( isEhraId ? "IN (?,?,?)" : "= ?" ),
            ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE
            );
            psql.setInt( 1, documentType );
            psql.setInt( 2, tenant.id );
            psql.setInt( 3, contentType.id );
            psql.setString( 4, tenant.subtenant );
            if( isEhraId ) {
              psql.setString( 5, "HR01-" + id );
              psql.setString( 6, "HR02-" + id );
              psql.setString( 7, "HR03-" + id ) ;
            } else {
              psql.setString( 5, id );
            } 
          }
          
        }
      } else if( id_array.length > 1 ) {
        // Get issue by year and number
        if( tenant.subtenant == null || tenant.subtenant.isEmpty() ){
            psql = query.prepare(
            "SELECT c.data, c.language_code"
            + " FROM document d"
            + " INNER JOIN content c ON c.document_id = d.id"
            + " INNER JOIN tenant_document td ON td.document_id = d.id"
            + " WHERE d.document_status_id = 1 AND extract(year from publication_time) = ? AND d.issue = ? AND td.tenant_id = ? AND td.document_type_id = ?"
            + "   AND c.content_type_id = ?",
            ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE
            );
            psql.setString( 1, id_array[0] );
            psql.setString( 2, id_array[1] );
            psql.setInt( 3, tenant.id );
            psql.setInt( 4, documentType );
            psql.setInt( 5, contentType.id );
        } else {
            psql = query.prepare(
            "SELECT c.data, c.language_code"
            + " FROM document d"
            + " INNER JOIN content c ON c.document_id = d.id"
            + " INNER JOIN tenant_document td ON td.document_id = d.id"
            + " WHERE d.document_status_id = 1 AND extract(year from publication_time) = ? AND d.issue = ? AND td.tenant_id = ? AND td.document_type_id = ?"
            + "   AND c.content_type_id = ? AND td.subtenant = ?",
            ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE
            );
            psql.setString( 1, id_array[0] );
            psql.setString( 2, id_array[1] );
            psql.setInt( 3, tenant.id );
            psql.setInt( 4, documentType );
            psql.setInt( 5, contentType.id );
            psql.setString( 6, tenant.subtenant );
        }
      }
      try( ResultSet rs = query.executeQuery( psql )) {
        query.filterLanguage( rs, languages, 2 );
        if( !rs.isAfterLast() ) {
          Blob blob = rs.getBlob( 1 );
          return blob.getBytes( 1, (int) blob.length() );
        } else {
          return null;
        }
      }
    } catch( Exception ex ) {
      LOG.error( "Failed to get PDF", ex );
    }
    return null;
  }

  public String id() {
    return id;
  }

  public List<Tenant> tenants() {
    return tenants;
  }

  public List<String> tenantNames() {
    List<String> names = new ArrayList<>();
    tenants.forEach( tenant -> {
      names.add( tenant.name );
    });
    return names;
  }

  public Language language() {
    return language;
  }

  public void language( Language language ) {
    this.language = language;
  }

  public Integer issue() {
    return issue;
  }

  public void issue( Integer issue ) {
    this.issue = issue;
  }

  public LocalDateTime publicationTime() {
    return publicationTime;
  }

  public void publicationTime( LocalDateTime publicationTime ) {
    this.publicationTime = publicationTime;
  }

  public LocalDateTime publicUntil() {
    return publicUntil;
  }

  public void publicUntil( LocalDateTime publicUntil ) {
    this.publicUntil = publicUntil;
  }

  public LocalDateTime archiveTime() {
    return archiveTime;
  }

  public void archiveTime( LocalDateTime archiveTime ) {
    this.archiveTime = archiveTime;
  }

  public byte[] data() {
    return data( true );
  }

  public byte[] data( boolean uncompress ) {
    if( uncompress )
      return uncompress( data );
    return data;
  }

  public void data( byte[] data ) {
    this.data = compress( data );
    if( documentType == 1 && text == null )
      text = getTextFromPDF( data );
  }

  public static String bytesToHex( byte[] bytes ) {
    char[] hexChars = new char[bytes.length * 2];
    for( int j = 0; j < bytes.length; j++ ) {
      int v = bytes[j] & 0xFF;
      hexChars[j * 2] = HEXARRAY[v >>> 4];
      hexChars[j * 2 + 1] = HEXARRAY[v & 0x0F];
    }
    return new String( hexChars );
  }

  public static byte[] compress( byte[] data ) {
    try(
      ByteArrayOutputStream output = new ByteArrayOutputStream();
      GZIPOutputStream gzip = new GZIPOutputStreamBest( output );
    ) {
      gzip.write( data, 0, data.length );
      gzip.close();
      return output.toByteArray();
    } catch( IOException ex ) {
      LOG.error( "Failed to compress", ex );
    }
    return null;
  }

  public static byte[] uncompress( byte[] data ) {
    try(
      GZIPInputStream gzip = new GZIPInputStream( new ByteArrayInputStream( data ));
      ByteArrayOutputStream output = new ByteArrayOutputStream();
    ) {
      byte[] buffer = new byte[1024];
      int count;
      while(( count = gzip.read( buffer )) != -1 ) {
        output.write(buffer, 0, count);
      }
      gzip.close();
      return output.toByteArray();
    } catch( IOException ex ) {
      LOG.error( "Failed to uncompress", ex);
    }
    return null;
  }

  private String getTextFromPDF( byte[] data ) {
    try (
      PDDocument pdf = PDDocument.load( data );
      StringWriter writer = new StringWriter();
    ) {
      PDFTextStripper stripper = new PDFTextStripper();
      stripper.writeText( pdf, writer );
      String pdfText = writer.getBuffer().toString().trim()
        .replaceAll( "(?m)\r\n", "\n" )
        .replaceAll( "(?m)^\\s*\n", "" )
        .replaceAll( "(?m)(\\p{L})-\n(\\p{Ll})", "$1$2" )
        .replaceAll( "\\s+", " " );
      if( !pdfText.isEmpty() )
        return pdfText;
    } catch( Exception ex ) {
      LOG.error( "Failed to get text from PDF", ex);
    }
    return null;
  }

  static class GZIPOutputStreamBest extends GZIPOutputStream {
    public GZIPOutputStreamBest( OutputStream out ) throws IOException {
      super( out );
      def.setLevel( Deflater.BEST_COMPRESSION );
    }
  }

}