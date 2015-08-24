package com.lucidworks.hadoop.ingest;

import com.lucidworks.hadoop.io.LWDocument;
import com.lucidworks.hadoop.io.LWDocumentFactoryProvider;
import com.lucidworks.hadoop.io.LWDocumentWritable;
import java.io.IOException;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract Mapper that transforms <K,V> provided by an FileInputFormat into SDA
 * Documents. The documents returned by this mapper are sent to HBase.
 * <p/>
 * Needs to define how records are transformed (see toDocuments) and how this
 * Mapper is configured (see getFixture)
 */
public abstract class AbstractIngestMapper<K extends Writable, V extends Writable>
    extends BaseHadoopIngest implements Mapper<K, V, Text, LWDocumentWritable> {

  protected static final Logger log = LoggerFactory.getLogger(AbstractIngestMapper.class);

  // TODO: move the properties
  public static final String TIKA_INCLUDE_IMAGES = "default.include.images";
  public static final String TIKA_FLATENN_COMPOUND = "default.faltten.compound";
  public static final String TIKA_ADD_FAILED_DOCS = "default.add.failed.docs";
  public static final String TIKA_ADD_ORIGINAL_CONTENT = "default.add.original.content";
  public static final String FIELD_MAPPING_RENAME_UNKNOWN = "default.rename.unknown";

  public static boolean includeImages = false;
  public static boolean flattenCompound = false;
  public static boolean addFailedDocs = false;
  public static boolean addOriginalContent = false;
  public static boolean renameUnknown = false;

  public void configure(JobConf conf) {
    super.configure(conf);
    includeImages = Boolean.parseBoolean(conf.get(TIKA_INCLUDE_IMAGES, "false"));
    flattenCompound = Boolean.parseBoolean(conf.get(TIKA_FLATENN_COMPOUND, "false"));
    addFailedDocs = Boolean.parseBoolean(conf.get(TIKA_ADD_FAILED_DOCS, "false"));
    addOriginalContent = Boolean.parseBoolean(conf.get(TIKA_ADD_ORIGINAL_CONTENT, "false"));
    renameUnknown = Boolean.parseBoolean(conf.get(FIELD_MAPPING_RENAME_UNKNOWN, "false"));

    System.out.println("includeImages: " + includeImages + " - flattenCompound: " + flattenCompound
        + " - addFailedDocs: " + addFailedDocs + " - addOriginalContent: " + addOriginalContent
        + " - renameUnknown: " + renameUnknown);
  }

  public void init(JobConf conf) throws IOException {
    LWDocumentFactoryProvider.init(conf);
  }

  public LWDocument createDocument() {
    return LWDocumentFactoryProvider.getDocumentFactory((JobConf) conf).createDocument();
  }

  public LWDocument createDocument(String id, Map<String, String> metadata) {
    return LWDocumentFactoryProvider.getDocumentFactory((JobConf) conf)
        .createDocument(id, metadata);
  }

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
        log.info("AIM doc: " + doc.toString());
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
