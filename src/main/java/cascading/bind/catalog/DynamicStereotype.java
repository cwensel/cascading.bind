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

import java.util.HashMap;
import java.util.Map;

import cascading.scheme.Scheme;
import cascading.tuple.Fields;

/**
 *
 */
public class DynamicStereotype<Protocol, Format> extends Stereotype<Protocol, Format>
  {
  Map<Point, SchemeFactory> factoryMap = new HashMap<Point, SchemeFactory>();

  /**
   *
   */
  public static interface SchemeFactory<Protocol, Format>
    {
    Scheme createScheme( Protocol protocol, Format format, Fields fields );
    }

  public DynamicStereotype( Protocol defaultProtocol, String name, Fields fields )
    {
    super( defaultProtocol, name, fields );
    }

  public void addSchemeFactory( SchemeFactory schemeFactory, Protocol protocol, Format format )
    {
    factoryMap.put( pair( protocol, format ), schemeFactory );
    }

  @Override
  public Scheme getSchemeFor( Protocol protocol, Format format )
    {
    SchemeFactory<Protocol, Format> factory = factoryMap.get( pair( protocol, format ) );

    if( factory == null )
      return null;

    return factory.createScheme( protocol, format, getFields() );
    }

  @Override
  public Scheme getSchemeFor( Format format )
    {
    return getSchemeFor( getDefaultProtocol(), format );
    }
  }
