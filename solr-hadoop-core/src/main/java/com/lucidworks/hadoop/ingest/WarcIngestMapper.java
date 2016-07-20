package com.lucidworks.hadoop.ingest;

import com.lucidworks.hadoop.io.LWDocument;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Map;

import edu.cmu.lemurproject.WarcFileInputFormat;
import edu.cmu.lemurproject.WarcRecord;
import edu.cmu.lemurproject.WritableWarcRecord;

public class WarcIngestMapper extends AbstractIngestMapper<LongWritable, WritableWarcRecord> {

  private static final String WARC_FIELD = "warc.";

  private final AbstractJobFixture fixture = new AbstractJobFixture() {
    @Override
    public void init(JobConf conf) throws IOException {
      conf.setInputFormat(WarcFileInputFormat.class);
    }
  };

  @Override
  public AbstractJobFixture getFixture() {
    return fixture;
  }

  @Override
  public LWDocument[] toDocuments(
    LongWritable _,
    WritableWarcRecord value,
    Reporter reporter,
    Configuration conf) throws IOException {
    String id = value.getRecord().getHeaderMetadataItem("WARC-Record-ID");
    LWDocument doc = createDocument(id, null);
    WarcRecord record = value.getRecord();
    doc.setContent(record.getContent());
    //doc.contentType = null; // Not setting the content type, that way Tika can detect it
    for (Map.Entry<String, String> entry : record.getHeaderMetadata()) {
      doc.addField(WARC_FIELD + entry.getKey(), entry.getValue());
    }
    return new LWDocument[]{doc};
  }

}
