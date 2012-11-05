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

package cascading.bind.factory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import cascading.bind.TapResource;
import cascading.cascade.Cascade;
import cascading.cascade.CascadeConnector;
import cascading.flow.Flow;
import org.jgrapht.graph.SimpleDirectedGraph;

/**
 *
 */
public class CascadeFactory<Resource extends TapResource> extends Factory<Cascade>
  {
  private final String name;

  private SimpleDirectedGraph<Resource, ProcessFactoryHolder> resourceGraph = null;
  private final List<ProcessFactory> processFactories = new ArrayList<ProcessFactory>();

  public static class ProcessFactoryHolder
    {
    ProcessFactory processFactory;

    public ProcessFactoryHolder()
      {
      }

    private ProcessFactoryHolder( ProcessFactory processFactory )
      {
      this.processFactory = processFactory;
      }
    }

  public CascadeFactory( Properties properties, String name )
    {
    super( properties );
    this.name = name;
    }

  public String getName()
    {
    return name;
    }

  public void addAllProcessFactories( Collection<ProcessFactory<?, Resource>> processFactories )
    {
    for( ProcessFactory<?, Resource> processFactory : processFactories )
      addProcessFactory( processFactory );
    }

  public void addProcessFactory( ProcessFactory<?, Resource> processFactory )
    {
    if( processFactories.contains( processFactory ) )
      throw new IllegalStateException( "may not add identical process factories, received: " + processFactory );

    processFactories.add( processFactory );
    }

  protected Collection<Resource> getAllResources()
    {
    initResourceGraph();

    Set<Resource> resources = new HashSet<Resource>();

    resources.addAll( resourceGraph.vertexSet() );

    return resources;
    }

  protected Collection<Resource> getResourcesWith( String identifier )
    {
    initResourceGraph();

    Set<Resource> resources = new HashSet<Resource>();

    for( Resource resource : resourceGraph.vertexSet() )
      {
      if( resource.getIdentifier().equals( identifier ) )
        resources.add( resource );
      }

    return resources;
    }

  protected Collection<ProcessFactory> getSourceDependenciesOn( Resource sourceResource )
    {
    initResourceGraph();

    Set<ProcessFactory> factories = new HashSet<ProcessFactory>();
    Set<ProcessFactoryHolder> outgoing = resourceGraph.outgoingEdgesOf( sourceResource );

    for( ProcessFactoryHolder processFactoryHolder : outgoing )
      factories.add( processFactoryHolder.processFactory );

    return factories;
    }

  protected Collection<ProcessFactory> getSinkDependenciesOn( Resource sourceResource )
    {
    initResourceGraph();

    Set<ProcessFactory> factories = new HashSet<ProcessFactory>();
    Set<ProcessFactoryHolder> incoming = resourceGraph.incomingEdgesOf( sourceResource );

    for( ProcessFactoryHolder processFactoryHolder : incoming )
      factories.add( processFactoryHolder.processFactory );

    return factories;
    }

  protected void initResourceGraph()
    {
    if( resourceGraph == null )
      rebuildResourceGraph();
    }

  protected void rebuildResourceGraph()
    {
    resourceGraph = new SimpleDirectedGraph<Resource, ProcessFactoryHolder>( ProcessFactoryHolder.class );

    for( ProcessFactory processFactory : processFactories )
      insertProcessFactory( processFactory );
    }

  private void insertProcessFactory( ProcessFactory<?, Resource> processFactory )
    {
    for( Resource resource : processFactory.getAllSourceResources() )
      resourceGraph.addVertex( resource );

    for( Resource resource : processFactory.getAllSinkResources() )
      resourceGraph.addVertex( resource );

    for( Resource incoming : processFactory.getAllSourceResources() )
      {
      for( Resource outgoing : processFactory.getAllSinkResources() )
        resourceGraph.addEdge( incoming, outgoing, new ProcessFactoryHolder( processFactory ) );
      }
    }

  protected CascadeConnector getCascadeConnector()
    {
    return new CascadeConnector( getProperties() );
    }

  @Override
  public Cascade create()
    {
    List<Flow> flows = new ArrayList<Flow>();

    for( ProcessFactory processFactory : processFactories )
      {
      Object o = processFactory.create();

      if( o == null )
        new IllegalStateException( "factory returned null: " + processFactory );

      if( o instanceof Flow )
        flows.add( (Flow) o );
      else if( o instanceof Cascade )
        flows.addAll( ( (Cascade) o ).getFlows() );
      else
        throw new IllegalStateException( "process type not supported: " + o.getClass().getName() + ", returned by: " + processFactory );
      }

    if( flows.isEmpty() )
      throw new IllegalStateException( "now flows were created from the given process factories" );

    return getCascadeConnector().connect( name, flows );
    }

  }
