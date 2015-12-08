package com.lucidworks.hadoop.ingest;

import java.io.Closeable;
import java.io.IOException;

import com.lucidworks.hadoop.io.LWDocumentProvider;
import org.apache.hadoop.mapred.JobConf;

/**
 * This class is used to provide setup and teardown methods for custom Mappers and such
 */
public abstract class AbstractJobFixture implements Closeable {

  /**
   * Perform any necessary configuration for the underlying Mapper, InputFormat, etc.
   * This is called during the job setup.
   */
  public void init(JobConf conf) throws IOException {
    try {
      LWDocumentProvider.init(conf);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Perform any necessary cleanup  of resources from the underlying Mapper, InputFormat,
   * etc. This is called after the job completes.
   */
  @Override
  public void close() throws IOException {
  }

}
