/*
 * Copyright (c) 2007-2011 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.concurrentinc.com/
 */

package cascading.bind.tap;

import java.io.IOException;

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;

/** A mock Tap for testing purposes. */
public class HTTPTap extends Tap
  {
  String url;

  public HTTPTap( Scheme scheme, String url, SinkMode sinkMode )
    {
    super( scheme, sinkMode );
    this.url = url;
    }

  @Override
  public String getIdentifier()
    {
    return url;
    }

  @Override
  public long getModifiedTime( Object conf ) throws IOException
    {
    return 0;
    }

  @Override
  public boolean resourceExists( Object conf ) throws IOException
    {
    return true;
    }

  @Override
  public boolean deleteResource( Object conf ) throws IOException
    {
    return true;
    }

  @Override
  public boolean createResource( Object conf ) throws IOException
    {
    return true;
    }

  @Override
  public TupleEntryIterator openForRead( FlowProcess flowProcess, Object object ) throws IOException
    {
    return null;
    }

  @Override
  public TupleEntryCollector openForWrite( FlowProcess flowProcess, Object object ) throws IOException
    {
    return super.openForWrite( flowProcess, object );
    }
  }
