/*
 * Copyright (c) 2007-2011 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.concurrentinc.com/
 */

package cascading.bind.factory;

import java.util.Properties;

/**
 *
 */
public abstract class Factory<P>
  {
  private Properties properties;

  protected Factory( Properties properties )
    {
    this.properties = properties;
    }

  public Factory()
    {
    }

  protected void setProperties( Properties properties )
    {
    this.properties = properties;
    }

  public Properties getProperties()
    {
    return properties;
    }

  /**
   * Method create returns a new instance of the type this factory creates.
   *
   * @return
   */
  public abstract P create();
  }
