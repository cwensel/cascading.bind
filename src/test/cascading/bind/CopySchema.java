/*
 * Copyright (c) 2007-2011 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.concurrentinc.com/
 */

package cascading.bind;

import cascading.scheme.hadoop.TextLine;
import cascading.tuple.Fields;

/**
 * A mock Schema that represents a 'person' type.
 * <p/>
 * Note the constant fields which can be used inside a Cascading application.
 */
public class CopySchema extends Schema<Protocol, Format>
  {
  protected CopySchema()
    {
    super( Protocol.HDFS );

    addSchemeFor( Format.TSV, new TextLine( new Fields( "line" ), new Fields( "line" ) ) );
    addSchemeFor( Format.CSV, new TextLine( new Fields( "line" ), new Fields( "line" ) ) );
    addSchemeFor( Format.JSON, new TextLine( new Fields( "line" ), new Fields( "line" ) ) );
    }
  }
