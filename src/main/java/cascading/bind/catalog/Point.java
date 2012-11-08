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

/**
 *
 */
public class Point<Protocol, Format>
  {
  public final Protocol protocol;
  public final Format format;

  public Point( Protocol protocol, Format format )
    {
    this.protocol = protocol;
    this.format = format;
    }

  @Override
  public boolean equals( Object object )
    {
    if( this == object )
      return true;
    if( object == null || getClass() != object.getClass() )
      return false;

    Point point = (Point) object;

    if( format != null ? !format.equals( point.format ) : point.format != null )
      return false;
    if( protocol != null ? !protocol.equals( point.protocol ) : point.protocol != null )
      return false;

    return true;
    }

  @Override
  public int hashCode()
    {
    int result = protocol != null ? protocol.hashCode() : 0;
    result = 31 * result + ( format != null ? format.hashCode() : 0 );
    return result;
    }

  @Override
  public String toString()
    {
    final StringBuilder sb = new StringBuilder();
    sb.append( "[protocol=" ).append( protocol );
    sb.append( ", format=" ).append( format );
    sb.append( "]" );
    return sb.toString();
    }
  }
