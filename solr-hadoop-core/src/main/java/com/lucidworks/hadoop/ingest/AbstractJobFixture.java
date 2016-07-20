package com.lucidworks.hadoop.ingest;

import java.io.Closeable;
import java.io.IOException;

import org.apache.hadoop.mapred.JobConf;

/**
 * This class is used to provide setup and teardown methods for custom Mappers and such
 */
public abstract class AbstractJobFixture implements Closeable {

  /**
   * Perform any necessary configuration for the underlying Mapper, InputFormat, etc.
   * This is called during the job setup.
   */
  public abstract void init(JobConf conf) throws IOException;

  /**
   * Perform any necessary cleanup  of resources from the underlying Mapper, InputFormat,
   * etc. This is called after the job completes.
   */
  @Override
  public void close() throws IOException {
  }

}
