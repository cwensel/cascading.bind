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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import cascading.bind.catalog.Schema;
import cascading.bind.tap.TapResource;

/**
 * Class ProcessFactory is an abstract base class for creating process based factories. Where a 'process'
 * has source and sink resources as defined by {@link cascading.bind.tap.TapFactory} instances.
 *
 * @param <Process>  a 'process' type
 * @param <Resource> a resource type sub-classing {@link cascading.bind.catalog.Resource}
 * @see FlowFactory
 */
public abstract class ProcessFactory<Process, Resource extends TapResource> extends Factory<Process>
  {
  final Map<String, Schema> sourceSchemas = new HashMap<String, Schema>();
  final Map<String, Schema> sinkSchemas = new HashMap<String, Schema>();
  final Map<String, List<Resource>> sourceResources = new HashMap<String, List<Resource>>();
  final Map<String, List<Resource>> sinkResources = new HashMap<String, List<Resource>>();

  protected ProcessFactory( Properties properties )
    {
    super( properties );
    }

  public ProcessFactory()
    {
    }

  /**
   * Method addSourceSchema binds a given {@link cascading.bind.tap.TapFactory} instance to the given 'name'.
   * <p/>
   * Only one Schema may be bound to a source name.
   *
   * @param sourceName
   * @param schema
   */
  protected void setSourceSchema( String sourceName, Schema schema )
    {
    if( sourceName == null || sourceName.isEmpty() )
      throw new IllegalArgumentException( "sourceName may not be null or empty" );

    if( schema == null )
      throw new IllegalArgumentException( "schema may not be null" );

    sourceSchemas.put( sourceName, schema );
    }

  protected Schema getSourceSchema( String sourceName )
    {
    return sourceSchemas.get( sourceName );
    }

  /**
   * Method addSinkSchema binds a given {@link cascading.bind.tap.TapFactory} instance to the given 'name'.
   * <p/>
   * Only one Schema may be bound to a sink name.
   *
   * @param sinkName
   * @param schema
   */
  protected void setSinkSchema( String sinkName, Schema schema )
    {
    if( sinkName == null || sinkName.isEmpty() )
      throw new IllegalArgumentException( "sinkName may not be null or empty" );

    if( schema == null )
      throw new IllegalArgumentException( "schema may not be null" );

    sinkSchemas.put( sinkName, schema );
    }

  protected Schema getSinkSchema( String sinkName )
    {
    return sinkSchemas.get( sinkName );
    }

  /**
   * Method addSourceResource binds a name to the given resources.
   * <p/>
   * This method may be called repeatedly with the same sourceName, all given
   * resources will be added to the binding.
   * <p/>
   * Any null resource values will be removed.
   *
   * @param sourceName
   * @param resources
   */
  protected void addSourceResource( String sourceName, Resource... resources )
    {
    if( resources == null || resources.length == 0 )
      return;

    List<Resource> resourceList = getSourceResources( sourceName );

    Collections.addAll( resourceList, resources );

    while( resourceList.contains( null ) )
      resourceList.remove( null );
    }

  /**
   * Method getSourceResources returns a List of resources associated with the given name.
   *
   * @param sourceName
   * @return
   */
  protected List<Resource> getSourceResources( String sourceName )
    {
    List<Resource> resourceList = sourceResources.get( sourceName );

    if( resourceList == null )
      {
      resourceList = new ArrayList<Resource>();
      sourceResources.put( sourceName, resourceList );
      }

    return resourceList;
    }

  /**
   * Method getAllSourceResources returns a Collection of all Resources instances add via
   * {@link #addSourceResource(String, cascading.bind.tap.TapResource[])}.
   *
   * @return Collection of Resource instances
   */
  public Collection<Resource> getAllSourceResources()
    {
    Set<Resource> set = new HashSet<Resource>();

    for( List<Resource> resources : sourceResources.values() )
      set.addAll( resources );

    return set;
    }

  public boolean replaceSourceResource( Resource from, Resource to )
    {
    return replaceResourceIn( from, to, sourceResources );
    }

  /** Method clearSourceResources removes all bindings for all names. */
  protected void clearSourceResources()
    {
    sourceResources.clear();
    }

  public Schema getSourceSchemaFor( TapResource resource )
    {
    return getSchemaFor( resource, sourceResources, sourceSchemas );
    }

  /**
   * Method addSinkResource binds a name to the given resources.
   * <p/>
   * This method may be called repeatedly with the same sinkName, all given
   * resources will be added to the binding.
   * <p/>
   * Any null resource values will be removed.
   *
   * @param sinkName
   * @param resources
   */
  protected void addSinkResource( String sinkName, Resource... resources )
    {
    if( resources == null || resources.length == 0 )
      return;

    List<Resource> resourceList = getSinkResources( sinkName );

    Collections.addAll( resourceList, resources );

    while( resourceList.contains( null ) )
      resourceList.remove( null );
    }

  /**
   * Method getSinkResources returns a List of resources associated with the given name.
   *
   * @param sinkName
   * @return
   */
  protected List<Resource> getSinkResources( String sinkName )
    {
    List<Resource> resourceList = sinkResources.get( sinkName );

    if( resourceList == null )
      {
      resourceList = new ArrayList<Resource>();
      sinkResources.put( sinkName, resourceList );
      }

    return resourceList;
    }

  /**
   * Method getAllSinkResources returns a Collection of all Resources instances add via
   * {@link #addSinkResource(String, cascading.bind.tap.TapResource[])}.
   *
   * @return Collection of Resource instances
   */
  public Collection<Resource> getAllSinkResources()
    {
    Set<Resource> set = new HashSet<Resource>();

    for( List<Resource> resources : sinkResources.values() )
      set.addAll( resources );

    return set;
    }

  public boolean replaceSinkResource( Resource from, Resource to )
    {
    return replaceResourceIn( from, to, sinkResources );
    }

  /** Method clearSinkResources removes all bindings for all names. */
  protected void clearSinkResources()
    {
    sinkResources.clear();
    }

  private boolean replaceResourceIn( Resource from, Resource to, Map<String, List<Resource>> resourceMap )
    {
    boolean found = false;

    for( String name : resourceMap.keySet() )
      {
      List<Resource> resources = resourceMap.get( name );
      int index = resources.indexOf( from );

      if( index != -1 )
        resources.set( index, to );

      found = found || index != -1;
      }

    return found;
    }

  public Schema getSinkSchemaFor( TapResource resource )
    {
    return getSchemaFor( resource, sinkResources, sinkSchemas );
    }

  private Schema getSchemaFor( TapResource resource, Map<String, List<Resource>> resources, Map<String, Schema> schemas )
    {
    String name = null;

    for( Map.Entry<String, List<Resource>> entry : resources.entrySet() )
      {
      if( !entry.getValue().contains( resource ) )
        continue;

      name = entry.getKey();
      break;
      }

    if( name == null )
      return null;

    return schemas.get( name );
    }

  protected Collection<String> getSourceNames()
    {
    return sourceResources.keySet();
    }

  protected Collection<String> getSinkNames()
    {
    return sinkResources.keySet();
    }

  }
