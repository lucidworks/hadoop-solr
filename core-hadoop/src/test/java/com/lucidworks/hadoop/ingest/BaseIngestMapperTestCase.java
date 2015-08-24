package com.lucidworks.hadoop.ingest;

import org.apache.hadoop.conf.Configuration;

import static com.lucidworks.hadoop.utils.ConfigurationKeys.COLLECTION;
import static com.lucidworks.hadoop.utils.ConfigurationKeys.ZK_CONNECT;

/**
 *
 *
 **/
public class BaseIngestMapperTestCase {

  protected void setupCommonConf(Configuration conf) {
    conf.set(COLLECTION, "collection");
    conf.set(ZK_CONNECT, "localhost:0000");
    conf.set("idField", "id");
  }

  protected Configuration createConf() {
    Configuration configuration = new Configuration();
    // FIXME: Move to test-base
    configuration.set("io.serializations", "com.lucidworks.hadoop.io.impl.LWMockSerealization");
    return configuration;
  }
}
