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

import cascading.tuple.Fields;

/**
 *
 */
public abstract class Catalog<Protocol, Format>
  {
  Map<Fields, Schema> schemas = new HashMap<Fields, Schema>();

  protected Catalog()
    {
    }

  public Schema<Protocol, Format> getSchemaFor( Fields fields )
    {
    return schemas.get( normalize( fields ) );
    }

  protected void addSchema( Schema<Protocol, Format> schema )
    {
    if( schemas.containsKey( schema.getFields() ) )
      throw new IllegalStateException( "catalog already contains schema for: " + schema.getFields() + ", named: " + schemas.get( schema.getFields() ).getName() );

    schemas.put( schema.getFields(), schema );
    }

  private Fields normalize( Fields fields )
    {
    if( fields.equals( Fields.ALL ) )
      fields = Fields.UNKNOWN;

    return fields;
    }
  }
