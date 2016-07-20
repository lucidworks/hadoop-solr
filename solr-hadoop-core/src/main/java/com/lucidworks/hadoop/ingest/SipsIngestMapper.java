package com.lucidworks.hadoop.ingest;

import com.lucidworks.hadoop.io.LWDocument;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SipsIngestMapper can be used to take the output of Mahout's statistically interesting phrases (collocations) and
 * index them into a collection in Solr
 */
public class SipsIngestMapper extends AbstractIngestMapper<Text, DoubleWritable> {
  private static final Logger log = LoggerFactory.getLogger(SipsIngestMapper.class);

  private final AbstractJobFixture fixture = new AbstractJobFixture() {
    @Override
    public void init(JobConf conf) throws IOException {
      boolean override = conf.getBoolean(IngestJob.INPUT_FORMAT_OVERRIDE, false);
      if (override == false) {
        conf.setInputFormat(SequenceFileInputFormat.class);
      }// else the user has overridden the input format and we assume it is OK.
    }
  };

  @Override
  protected LWDocument[] toDocuments(Text key, DoubleWritable value, Reporter reporter,
      Configuration conf) throws IOException {
    Map<String, String> metadata = new HashMap<String, String>();
    LWDocument doc = createDocument(key.toString(), metadata);
    doc.addField("sip_score", value);
    return new LWDocument[] {doc};
  }

  @Override
  public AbstractJobFixture getFixture() {
    return fixture;
  }

  /*public void map(Text key, DoubleWritable value,
                  OutputCollector<NullWritable, NullWritable> outputCollector,
                  Reporter reporter) throws IOException {
    solrDocument = new SolrInputDocument();
    solrDocument.addField("id", key.toString());
    solrDocument.addField("sip_score", value);
    try {
      solrServer.add(solrDocument);
      reporter.incrCounter(LucidCounters.SIPS_TO_SOLR_INDEXED, 1);
    } catch (SolrServerException e) {
      log.warn("Cannot index collocations: ", e);
      reporter.incrCounter(LucidCounters.SIPS_TO_SOLR_FAILED, 1);
    }
  }

  @Override
  public void close() throws IOException {
    try {
      solrServer.commit(false, false);
      solrServer.shutdown();
    } catch (final SolrServerException e) {
      log.error("Cannot close index for collocations: ", e);
    }
  }*/

}
