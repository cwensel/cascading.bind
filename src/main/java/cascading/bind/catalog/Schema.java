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

package cascading.bind.catalog;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cascading.scheme.Scheme;
import cascading.tuple.Fields;

/**
 * Class Schema is used to map between 'protocols' and 'formats' to available Cascading Scheme instances.
 * <p/>
 * This is particularly useful when creating data processing applications that need to deal with
 * multiple data formats for data (tab delimited, JSON, thrift, etc) and multiple ways to access the data
 * (HDFS, S3, JDBC, Memcached, etc).
 * <p/>
 * This class is type parameterized for P and F where P represent a 'protocol' and F represents
 * a file or data 'format'. Typically P and F are of type {@link Enum}, but may be any standard class.
 * <p/>
 * It is a common practice to sub-class Schema so that each new class represents a particular abstract
 * data type like 'person' or an Apache server log record.
 *
 * @param <Protocol> a 'protocol' type
 * @param <Format>   a data 'format' type
 */
public class Schema<Protocol, Format>
  {
  String name = getClass().getSimpleName().replaceAll( "Schema$", "" );
  Protocol defaultProtocol;
  Fields fields;
  final Map<Point, Scheme> schemes = new HashMap<Point, Scheme>();

  public Schema( Protocol defaultProtocol )
    {
    this( defaultProtocol, null, null );
    }

  public Schema( Protocol defaultProtocol, String name, Fields fields )
    {
    this.defaultProtocol = defaultProtocol;
    this.name = name == null ? this.name : name;
    this.fields = fields;

    if( defaultProtocol == null )
      throw new IllegalArgumentException( "defaultProtocol may not be null" );
    }

  public String getName()
    {
    return name;
    }

  protected void setName( String name )
    {
    this.name = name;
    }

  public Protocol getDefaultProtocol()
    {
    return defaultProtocol;
    }

  public Fields getFields()
    {
    return fields;
    }

  private void setFields( Scheme scheme )
    {
    if( scheme.isSource() )
      {
      if( fields == null )
        fields = scheme.getSourceFields();
      else if( !fields.equals( scheme.getSourceFields() ) )
        throw new IllegalArgumentException( "all schemes added to schema must have the same source fields, expected: " + fields + ", received: " + scheme.getSourceFields() + " in schema: " + getName() );
      }

    if( scheme.isSink() )
      {
      if( fields == null )
        fields = scheme.getSinkFields();
      else if( !fields.equals( scheme.getSinkFields() ) )
        throw new IllegalArgumentException( "all schemes added to schema must have the same sink fields, expected: " + fields + ", received: " + scheme.getSinkFields() + " in schema: " + getName() );
      }
    }

  protected void addSchemeFor( Protocol protocol, Format format, Scheme scheme )
    {
    if( protocol == null )
      {
      addSchemeFor( format, scheme );
      return;
      }

    setFields( scheme );

    schemes.put( new Point<Protocol, Format>( protocol, format ), scheme );
    }

  protected void addSchemeFor( Format format, Scheme scheme )
    {
    setFields( scheme );

    schemes.put( new Point<Protocol, Format>( defaultProtocol, format ), scheme );
    }

  public Scheme getSchemeFor( Protocol protocol, Format format )
    {
    if( protocol == null )
      return getSchemeFor( format );

    return schemes.get( new Point<Protocol, Format>( protocol, format ) );
    }

  public Scheme getSchemeFor( Format format )
    {
    return schemes.get( new Point<Protocol, Format>( defaultProtocol, format ) );
    }

  public Collection<Scheme> getAllSchemesFor( Format format )
    {
    List<Scheme> found = new ArrayList<Scheme>();

    for( Map.Entry<Point, Scheme> entry : schemes.entrySet() )
      {
      if( format.equals( entry.getKey().format ) )
        found.add( entry.getValue() );
      }

    return found;
    }

  public boolean containsSchemeFor( Format format )
    {
    return !getAllSchemesFor( format ).isEmpty();
    }

  protected Point<Protocol, Format> pair( Resource<Protocol, Format, ?> resource )
    {
    Protocol protocol = resource.getProtocol();

    if( protocol == null )
      protocol = defaultProtocol;

    return new Point<Protocol, Format>( protocol, resource.getFormat() );
    }

  protected Point<Protocol, Format> pair( Protocol protocol, Format format )
    {
    if( protocol == null )
      protocol = defaultProtocol;

    return new Point<Protocol, Format>( protocol, format );
    }

  @Override
  public boolean equals( Object object )
    {
    if( this == object )
      return true;
    if( object == null || getClass() != object.getClass() )
      return false;

    Schema schema = (Schema) object;

    if( fields != null ? !fields.equals( schema.fields ) : schema.fields != null )
      return false;

    return true;
    }

  @Override
  public int hashCode()
    {
    return fields != null ? fields.hashCode() : 0;
    }
  }
