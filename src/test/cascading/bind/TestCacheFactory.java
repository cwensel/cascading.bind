/*
 * Copyright (c) 2007-2011 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.concurrentinc.com/
 */

package cascading.bind;

import java.util.Collection;
import java.util.Properties;

import cascading.bind.factory.CascadeFactory;
import cascading.bind.factory.ProcessFactory;
import cascading.cascade.Cascade;

/** A mock FlowFactory that creates copies a from from one location to another. */
public class TestCacheFactory extends CascadeFactory
  {
  private String sourceString;
  private String sinkString;

  public TestCacheFactory( String name )
    {
    this( null, name );
    }

  public TestCacheFactory( Properties properties, String name )
    {
    super( properties, name );
    }

  public void setSource( String path )
    {
    sourceString = path;
    }

  public void setSink( String path )
    {
    sinkString = path;
    }

  @Override
  public Cascade create()
    {
    Collection<ConversionResource> resources = getResourcesWith( sourceString );

    for( ConversionResource resource : resources )
      {
      Collection<ProcessFactory> dependencies = getSourceDependenciesOn( resource );

      if( dependencies.isEmpty() || dependencies.size() < 2 ) // don't cache if more than one dep
        continue;

      ConversionResource cachedResource = new ConversionResource( sinkString, resource.getProtocol(), resource.getFormat(), resource.getMode() );

      TestCopyFactory cache = new TestCopyFactory( getProperties(), getName() + "-" + resource );

      cache.addSourceResource( resource );
      cache.addSinkResource( cachedResource );

      for( ProcessFactory dependency : dependencies )
        dependency.replaceSourceResource( resource, cachedResource );

      addProcessFactory( cache );
      }

    return super.create();
    }
  }
