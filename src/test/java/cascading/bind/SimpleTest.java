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

import cascading.CascadingTestCase;
import cascading.bind.tap.TapFactory;
import cascading.cascade.Cascade;
import cascading.flow.Flow;
import cascading.tap.Tap;
import junit.framework.Assert;

/**
 *
 */
public class SimpleTest extends CascadingTestCase
  {
  public void testStereotype()
    {
    PersonStereotype stereotype = new PersonStereotype();
    TapFactory tapFactory = new TapFactory( stereotype );

    Assert.assertNotNull( tapFactory.getTapFor( new ConversionResource( "foo", Protocol.FILE, Format.TSV ) ) );
    Assert.assertNotNull( tapFactory.getTapFor( new ConversionResource( "foo", Protocol.HTTP, Format.JSON ) ) );
    }

  public void testFlowFactory()
    {
    CSVToTSVFactory factory = new CSVToTSVFactory( "convert", new PersonStereotype() );

    factory.setSource( Protocol.FILE, "some/path" );
    factory.setSink( Protocol.HTTP, "http://some/place" );

    Flow flow = factory.create();

    Assert.assertEquals( 1, flow.getSourcesCollection().size() );
    Assert.assertEquals( 1, flow.getSinksCollection().size() );

    Assert.assertEquals( "some/path", flow.getSource( "convert" ).getIdentifier() );
    Assert.assertEquals( "http://some/place", flow.getSink( "convert" ).getIdentifier() );
    }

  public void testCascadeFactory()
    {
    // these factories read from the same location, but pretend they do something different
    CSVToTSVFactory factory1 = new CSVToTSVFactory( "convert1", new PersonStereotype() );
    factory1.setSource( Protocol.FILE, "some/remote/path" );
    factory1.setSink( Protocol.FILE, "some/place/first" );

    CSVToTSVFactory factory2 = new CSVToTSVFactory( "convert2", new PersonStereotype() );
    factory2.setSource( Protocol.FILE, "some/remote/path" );
    factory2.setSink( Protocol.FILE, "some/place/second" );

    CSVToTSVFactory factory3 = new CSVToTSVFactory( "convert3", new PersonStereotype() );
    factory3.setSource( Protocol.FILE, "some/remote/path" );
    factory3.setSink( Protocol.FILE, "some/place/third" );

    // will insert a copy flow if more than one other flow-factory is reading
    // from the same remote source, then update the dependent factories
    // with the new location
    TestCacheFactory resourcesFactory = new TestCacheFactory( "copy" );
    resourcesFactory.setSource( "some/remote/path" );
    resourcesFactory.setSink( "some/local/path" );

    resourcesFactory.addProcessFactory( factory1 );
    resourcesFactory.addProcessFactory( factory2 );
    resourcesFactory.addProcessFactory( factory3 );

    Cascade cascade = resourcesFactory.create();

    Assert.assertTrue( cascade.getFlows().get( 0 ).getName(), cascade.getFlows().get( 0 ).getName().startsWith( "copy" ) );

    for( Flow flow : cascade.getFlows() )
      {
      if( flow.getName().startsWith( "convert" ) )
        Assert.assertEquals( "some/local/path", ( (Tap) flow.getSourcesCollection().iterator().next() ).getIdentifier() );
      }
    }
  }
