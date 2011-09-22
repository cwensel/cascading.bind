/*
 * Copyright (c) 2007-2011 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.concurrentinc.com/
 */

package cascading.bind;

import cascading.bind.tap.JDBCScheme;
import cascading.bind.tap.JSONScheme;
import cascading.scheme.hadoop.SequenceFile;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tuple.Fields;

/**
 * A mock Schema that represents a 'person' type.
 * <p/>
 * Note the constant fields which can be used inside a Cascading application.
 */
public class PersonSchema extends Schema<Protocol, Format>
  {
  public static final Fields FIRST = new Fields( "firstName" );
  public static final Fields LAST = new Fields( "lastName" );
  public static final Fields ADDRESS = new Fields( "address" );

  public static final Fields FIELDS = FIRST.append( LAST ).append( ADDRESS );

  public PersonSchema()
    {
    super( Protocol.HDFS );

    addSchemeFor( Format.TSV, new TextDelimited( FIELDS, "\t" ) );
    addSchemeFor( Format.CSV_HEADERS, new TextDelimited( FIELDS, true, ",", "\"" ) );
    addSchemeFor( Format.CSV, new TextDelimited( FIELDS, ",", "\"" ) );
    addSchemeFor( Format.Native, new SequenceFile( FIELDS ) );

    addSchemeFor( Protocol.JDBC, Format.Native, new JDBCScheme( FIELDS ) );
    addSchemeFor( Protocol.HTTP, Format.JSON, new JSONScheme( FIELDS ) );
    addSchemeFor( Protocol.HTTP, Format.TSV, new TextDelimited( FIELDS, "\t" ) );
    }
  }
