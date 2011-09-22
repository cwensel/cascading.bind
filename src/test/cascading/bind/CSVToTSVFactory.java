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
public class CSVToTSVFactory extends FlowFactory
  {
  private String name;
  private boolean hasHeaders;

  public CSVToTSVFactory( String name, Schema schema )
    {
    this( name, schema, false );
    }

  public CSVToTSVFactory( String name, Schema schema, boolean hasHeaders )
    {
    this( null, name, schema, hasHeaders );
    }

  public CSVToTSVFactory( Properties properties, String name, Schema schema, boolean hasHeaders )
    {
    super( properties );
    this.name = name;
    this.hasHeaders = hasHeaders;

    setSourceSchema( name, schema );
    setSinkSchema( name, schema );
    }

  public void setSource( String path )
    {
    setSource( (Protocol) getSourceSchema( name ).getDefaultProtocol(), path );
    }

  public void setSource( Protocol protocol, String path )
    {
    addSourceResource( name, new ConversionResource( path, protocol, hasHeaders ? Format.CSV_HEADERS : Format.CSV ) );
    }

  public void setSink( String path )
    {
    setSink( (Protocol) getSinkSchema( name ).getDefaultProtocol(), path );
    }

  public void setSink( Protocol protocol, String path )
    {
    addSinkResource( name, new ConversionResource( path, protocol, Format.TSV ) );
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
