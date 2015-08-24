package com.lucidworks.hadoop.utils;

import com.lucidworks.hadoop.ingest.BaseHadoopIngest;

public final class ConfigurationKeys {

  // Config keys
  public static final String COLLECTION = BaseHadoopIngest.class.getName() + ".collection";
  public static final String MIME_TYPE = BaseHadoopIngest.class.getName() + ".mimeType";
  public static final String ZK_CONNECT = BaseHadoopIngest.class.getName() + ".zkConnect";
  public static final String SOLR_SERVER_URL = BaseHadoopIngest.class.getName() + ".solr";
  public static final String TEMP_DIR = BaseHadoopIngest.class.getName() + ".tmpDir";
  public static final String OVERWRITE = BaseHadoopIngest.class.getName() + ".overwrite";
}
