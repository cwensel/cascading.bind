/*
 * Copyright (c) 2007-2011 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.concurrentinc.com/
 */

package cascading.bind.tap;

import java.io.IOException;

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.Tap;
import cascading.tuple.Fields;

/** A mock Scheme for testing purposes. */
public class JDBCScheme extends Scheme
  {

  public JDBCScheme( Fields fields )
    {
    super( fields, fields );
    }

  @Override
  public void sourceConfInit( FlowProcess flowProcess, Tap tap, Object conf )
    {
    }

  @Override
  public void sinkConfInit( FlowProcess flowProcess, Tap tap, Object conf )
    {
    }

  @Override
  public boolean source( FlowProcess flowProcess, SourceCall sourceCall ) throws IOException
    {
    return false;
    }

  @Override
  public void sink( FlowProcess flowProcess, SinkCall sinkCall ) throws IOException
    {
    }
  }
