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

import cascading.bind.tap.HTTPTap;
import cascading.bind.tap.JDBCScheme;
import cascading.bind.tap.JDBCTap;
import cascading.scheme.Scheme;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;

/** A mock resource that acts as a factory for specific Tap types. */
public class ConversionResource extends TapResource<Protocol, Format>
  {
  public ConversionResource( String path, Protocol protocol, Format format )
    {
    super( path, protocol, format, SinkMode.KEEP );
    }

  public ConversionResource( String path, Protocol protocol, Format format, SinkMode mode )
    {
    super( path, protocol, format, mode );
    }

  @Override
  public Tap createTapFor( Scheme scheme )
    {
    Protocol protocol = getProtocol();

    if( protocol == null )
      protocol = Protocol.HDFS;

    switch( protocol )
      {
      case HDFS:
        return new Hfs( scheme, getIdentifier(), getMode() );
      case JDBC:
        return new JDBCTap( (JDBCScheme) scheme, getIdentifier(), getMode() );
      case HTTP:
        return new HTTPTap( scheme, getIdentifier(), getMode() );
      }

    throw new IllegalStateException( "no tap for given protocol: " + getProtocol() );
    }
  }
