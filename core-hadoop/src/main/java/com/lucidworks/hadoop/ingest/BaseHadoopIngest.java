package com.lucidworks.hadoop.ingest;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;

import static com.lucidworks.hadoop.utils.ConfigurationKeys.COLLECTION;

/**
 *
 *
 **/
public abstract class BaseHadoopIngest {

  private static final String UNKNOWN = "__unknown__";
  protected Configuration conf;

  public void configure(JobConf conf) {

    this.conf = conf;
    // FIXME: there is another validation on IngestJob
    //    String connectStr = conf.get(ZK_CONNECT,
    //            conf.get(SOLR_SERVER_URL, "invalid"));
    //    if (connectStr == null || connectStr.equals("invalid") == true) {
    //      throw new RuntimeException("No " + ZK_CONNECT + " or " + SOLR_SERVER_URL
    //              + " property set for Ingest");
    //    }
  }

  public void close() throws IOException {
  }

  /**
   * Return the target collection for this job
   */
  public final String getCollection() {
    return conf.get(COLLECTION, UNKNOWN);
  }

  /**
   * Get the AbstractJobFixture implementation for the subclass. The fixture
   * will define how this class is configured, and how it is cleaned up
   *
   * @see com.lucidworks.hadoop.ingest.AbstractJobFixture
   */
  public abstract AbstractJobFixture getFixture();

  public enum Counters {
    DOCS_PUT_FAILED, DOCS_ADDED, DOCS_CONVERT_FAILED,
  }
}
