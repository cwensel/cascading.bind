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

import cascading.bind.catalog.Schema;
import cascading.bind.tap.JDBCScheme;
import cascading.bind.tap.JSONScheme;
import cascading.scheme.local.TextDelimited;
import cascading.tuple.Fields;

/**
 * A mock Schema that represents a 'person' type.
 * <p/>
 * Note the constant fields which can be used inside a Cascading application.
 */
public class PersonSchema extends Schema<Protocol, Format>
  {
  public static final Fields FIRST = new Fields( "firstName" );
  public static final Fields LAST = new Fields( "lastName" );
  public static final Fields ADDRESS = new Fields( "address" );

  public static final Fields FIELDS = FIRST.append( LAST ).append( ADDRESS );

  public PersonSchema()
    {
    super( Protocol.FILE );

    addSchemeFor( Format.TSV, new TextDelimited( FIELDS, "\t" ) );
    addSchemeFor( Format.CSV_HEADERS, new TextDelimited( FIELDS, true, ",", "\"" ) );
    addSchemeFor( Format.CSV, new TextDelimited( FIELDS, ",", "\"" ) );
    addSchemeFor( Format.Native, new TextDelimited( FIELDS, ",", "\"" ) );

    addSchemeFor( Protocol.JDBC, Format.Native, new JDBCScheme( FIELDS ) );
    addSchemeFor( Protocol.HTTP, Format.JSON, new JSONScheme( FIELDS ) );
    addSchemeFor( Protocol.HTTP, Format.TSV, new TextDelimited( FIELDS, "\t" ) );
    }
  }
