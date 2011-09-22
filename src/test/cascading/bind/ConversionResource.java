/*
 * Copyright (c) 2007-2011 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.concurrentinc.com/
 */

package cascading.bind;

import cascading.bind.tap.HTTPTap;
import cascading.bind.tap.JDBCScheme;
import cascading.bind.tap.JDBCTap;
import cascading.scheme.Scheme;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;

/** A mock resource that acts as a factory for specific Tap types. */
public class ConversionResource extends TapResource<Protocol, Format>
  {
  public ConversionResource( String path, Protocol protocol, Format format )
    {
    super( path, protocol, format, SinkMode.KEEP );
    }

  public ConversionResource( String path, Protocol protocol, Format format, SinkMode mode )
    {
    super( path, protocol, format, mode );
    }

  @Override
  public Tap createTapFor( Scheme scheme )
    {
    Protocol protocol = getProtocol();

    if( protocol == null )
      protocol = Protocol.HDFS;

    switch( protocol )
      {
      case HDFS:
        return new Hfs( scheme, getIdentifier(), getMode() );
      case JDBC:
        return new JDBCTap( (JDBCScheme) scheme, getIdentifier(), getMode() );
      case HTTP:
        return new HTTPTap( scheme, getIdentifier(), getMode() );
      }

    throw new IllegalStateException( "no tap for given protocol: " + getProtocol() );
    }
  }
