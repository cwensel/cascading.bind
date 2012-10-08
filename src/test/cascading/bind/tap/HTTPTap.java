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

package cascading.bind.tap;

import java.io.IOException;

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;

/** A mock Tap for testing purposes. */
public class HTTPTap extends Tap
  {
  String url;

  public HTTPTap( Scheme scheme, String url, SinkMode sinkMode )
    {
    super( scheme, sinkMode );
    this.url = url;
    }

  @Override
  public String getIdentifier()
    {
    return url;
    }

  @Override
  public long getModifiedTime( Object conf ) throws IOException
    {
    return 0;
    }

  @Override
  public boolean resourceExists( Object conf ) throws IOException
    {
    return true;
    }

  @Override
  public boolean deleteResource( Object conf ) throws IOException
    {
    return true;
    }

  @Override
  public boolean createResource( Object conf ) throws IOException
    {
    return true;
    }

  @Override
  public TupleEntryIterator openForRead( FlowProcess flowProcess, Object object ) throws IOException
    {
    return null;
    }

  @Override
  public TupleEntryCollector openForWrite( FlowProcess flowProcess, Object object ) throws IOException
    {
    return super.openForWrite( flowProcess, object );
    }
  }
