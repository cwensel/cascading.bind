/*
 * Copyright (c) 2007-2012 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.cascading.org/
 *
 * This file is part of the Cascading project.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
