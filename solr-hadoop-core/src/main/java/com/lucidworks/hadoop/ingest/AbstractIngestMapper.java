package com.lucidworks.hadoop.ingest;

import com.lucidworks.hadoop.io.LWDocument;
import com.lucidworks.hadoop.io.LWDocumentWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Abstract Mapper that transforms <K,V> provided by an FileInputFormat into
 * Documents. The documents returned by this mapper.
 * <p/>
 * Needs to define how records are transformed (see toDocuments) and how this
 * Mapper is configured (see getFixture)
 */
public abstract class AbstractIngestMapper<K extends Writable, V extends Writable>
    extends BaseHadoopIngest implements Mapper<K, V, Text, LWDocumentWritable> {

  protected static final Logger log = LoggerFactory.getLogger(AbstractIngestMapper.class);

  public void configure(JobConf conf) {
    super.configure(conf);
  }

  @Override
  public final void map(K key, V value, OutputCollector<Text, LWDocumentWritable> output,
                        Reporter reporter) throws IOException {
    // TODO: potential OOM here if we create a lot of docs from 1.
    LWDocument[] documents = null;

    try {
      documents = toDocuments(key, value, reporter, conf);
    } catch (OutOfMemoryError e) {
      log.error("Ran out of memory trying to convert: " + key, e);
      reporter.getCounter(Counters.DOCS_CONVERT_FAILED).increment(1);
    }
    if (documents != null && documents.length > 0) {
      // TODO: can we batch put these into the OutputFormat? can we still deal w/ the errors properly
      for (LWDocument doc : documents) {
        if (log.isDebugEnabled()) {
          log.debug("AIM doc: " + doc.toString());
        }
        try {
          output.collect(new Text(doc.getId()), new LWDocumentWritable(doc));
          reporter.getCounter(Counters.DOCS_ADDED).increment(1);
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    } else {
      log.warn("No documents were created for key: {}", key);
      reporter.getCounter(Counters.DOCS_CONVERT_FAILED).increment(1);
    }
  }

  /**
   * Transform the key and value into a set of PipelineDocuments. This is called
   * from within the map method in the MapReduce execution context
   */
  protected abstract LWDocument[] toDocuments(K key, V value, Reporter reporter, Configuration conf)
      throws IOException;
}
