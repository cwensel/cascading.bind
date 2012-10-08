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
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.Tap;
import cascading.tuple.Fields;

/** A mock Scheme for testing purposes. */
public class JSONScheme extends Scheme
  {

  public JSONScheme( Fields fields )
    {
    super( fields, fields );
    }

  @Override
  public void sourceConfInit( FlowProcess flowProcess, Tap tap, Object conf )
    {
    }

  @Override
  public void sinkConfInit( FlowProcess flowProcess, Tap tap, Object conf )
    {
    }

  @Override
  public boolean source( FlowProcess flowProcess, SourceCall sourceCall ) throws IOException
    {
    return false;
    }

  @Override
  public void sink( FlowProcess flowProcess, SinkCall sinkCall ) throws IOException
    {
    }

  }
