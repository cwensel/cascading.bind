/*
 * Copyright (c) 2007-2011 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.concurrentinc.com/
 */

package cascading.bind;

import java.util.Properties;

import cascading.bind.factory.FlowFactory;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.pipe.Pipe;

/**
 * A mock FlowFactory that creates a working Flow for conversion of data between
 * two formats and across multiple protocols.
 */
public class TestCopyFactory extends FlowFactory
  {
  private String name;

  public TestCopyFactory( String name )
    {
    this( null, name );
    }

  public TestCopyFactory( Properties properties, String name )
    {
    super( properties );
    this.name = name;

    setSourceSchema( name, new CopySchema() );
    setSinkSchema( name, new CopySchema() );
    }

  public void addSourceResource( TapResource resource )
    {
    addSourceResource( name, resource );
    }

  public void addSinkResource( TapResource resource )
    {
    addSinkResource( name, resource );
    }

  @Override
  protected FlowConnector getFlowConnector()
    {
    return new HadoopFlowConnector( getProperties() );
    }

  @Override
  public Flow create()
    {
    Pipe pipe = new Pipe( name ); // this forces pipe-lining between the source and sink

    return createFlowFrom( name, pipe );
    }
  }
