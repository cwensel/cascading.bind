/*
 * Copyright (c) 2007-2011 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.concurrentinc.com/
 */

package cascading.bind;

import cascading.CascadingTestCase;
import cascading.cascade.Cascade;
import cascading.flow.Flow;
import cascading.tap.Tap;
import cascading.test.PlatformTest;
import junit.framework.Assert;

/**
 *
 */
@PlatformTest(platforms = {"none"})
public class SimpleTest extends CascadingTestCase
  {
  public void testSchema()
    {
    PersonSchema schema = new PersonSchema();

    Assert.assertNotNull( schema.getTapFor( new ConversionResource( "foo", Protocol.HDFS, Format.TSV ) ) );
    Assert.assertNotNull( schema.getTapFor( new ConversionResource( "foo", Protocol.HTTP, Format.JSON ) ) );
    }

  public void testFlowFactory()
    {
    CSVToTSVFactory factory = new CSVToTSVFactory( "convert", new PersonSchema() );

    factory.setSource( Protocol.HDFS, "some/path" );
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
    CSVToTSVFactory factory1 = new CSVToTSVFactory( "convert1", new PersonSchema() );
    factory1.setSource( Protocol.HDFS, "some/remote/path" );
    factory1.setSink( Protocol.HDFS, "some/place/first" );

    CSVToTSVFactory factory2 = new CSVToTSVFactory( "convert2", new PersonSchema() );
    factory2.setSource( Protocol.HDFS, "some/remote/path" );
    factory2.setSink( Protocol.HDFS, "some/place/second" );

    CSVToTSVFactory factory3 = new CSVToTSVFactory( "convert3", new PersonSchema() );
    factory3.setSource( Protocol.HDFS, "some/remote/path" );
    factory3.setSink( Protocol.HDFS, "some/place/third" );

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
