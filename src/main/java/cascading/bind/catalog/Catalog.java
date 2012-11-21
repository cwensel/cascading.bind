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

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import cascading.tuple.Fields;

/**
 *
 */
public class Catalog<Protocol, Format> implements Serializable
  {
  Map<String, Stereotype<Protocol, Format>> nameToStereotype = new HashMap<String, Stereotype<Protocol, Format>>();
  Map<Fields, Stereotype<Protocol, Format>> fieldsToStereotype = new HashMap<Fields, Stereotype<Protocol, Format>>();

  public Catalog()
    {
    }

  public Stereotype<Protocol, Format> getStereotypeFor( String name )
    {
    if( name == null || name.isEmpty() )
      throw new IllegalArgumentException( "name may not be null" );

    return nameToStereotype.get( name );
    }

  public Stereotype<Protocol, Format> getStereotypeFor( Fields fields )
    {
    return fieldsToStereotype.get( normalize( fields ) );
    }

  public void addStereotype( Stereotype<Protocol, Format> stereotype )
    {
    if( nameToStereotype.containsKey( stereotype.getName() ) )
      throw new IllegalArgumentException( "catalog already contains stereotype for: " + stereotype.getName() + ", with fields: " + nameToStereotype.get( stereotype.getName() ).getFields() );

    if( fieldsToStereotype.containsKey( stereotype.getFields() ) )
      throw new IllegalArgumentException( "catalog already contains stereotype for: " + stereotype.getFields() + ", named: " + fieldsToStereotype.get( stereotype.getFields() ).getName() );

    nameToStereotype.put( stereotype.getName(), stereotype );
    fieldsToStereotype.put( stereotype.getFields(), stereotype );
    }

  private Fields normalize( Fields fields )
    {
    if( fields.equals( Fields.ALL ) )
      fields = Fields.UNKNOWN;

    return fields;
    }
  }
