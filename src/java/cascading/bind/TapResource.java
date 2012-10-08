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

import cascading.scheme.Scheme;
import cascading.tap.SinkMode;
import cascading.tap.Tap;

/**
 * Class TapResource is an abstract base class to be used with the {@link Schema} class for dynamically
 * looking up Cascading {@link Tap} instances based on a given 'protocol' and 'format'.
 * <p/>
 * TapResource extends {@link Resource} and binds {@link SinkMode} as the Resource 'mode' type.
 *
 * @param <P> a 'protocol' type
 * @param <F> a data 'format' type
 */
public abstract class TapResource<P, F> extends Resource<P, F, SinkMode>
  {
  protected TapResource()
    {
    }

  public TapResource( String path, P protocol, F format, SinkMode mode )
    {
    super( path, protocol, format, mode );
    }

  public String getSimpleIdentifier()
    {
    String name = getIdentifier();

    if( name.endsWith( "/" ) )
      name = name.substring( 0, name.length() - 1 );

    return name.substring( name.lastIndexOf( "/" ) + 1 );
    }

  public abstract Tap createTapFor( Scheme scheme );
  }
