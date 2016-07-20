package com.lucidworks.hadoop.ingest;

import com.lucidworks.hadoop.io.LWDocument;
import com.lucidworks.hadoop.io.ZipFileInputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static com.lucidworks.hadoop.utils.ConfigurationKeys.MIME_TYPE;

public class ZipIngestMapper extends AbstractIngestMapper<Text, BytesWritable> {

  private final AbstractJobFixture fixture = new AbstractJobFixture() {
    @Override
    public void init(JobConf conf) throws IOException {
      conf.setInputFormat(ZipFileInputFormat.class);
      ZipFileInputFormat.setLenient(true);
    }
  };

  @Override
  public void configure(JobConf conf) {
    super.configure(conf);
  }

  @Override
  public AbstractJobFixture getFixture() {
    return fixture;
  }

  @Override
  public LWDocument[] toDocuments(
    Text key,
    BytesWritable value,
    Reporter reporter,
    Configuration conf) throws IOException {

    Map<String, String> metadata = new HashMap<>();
    String mimeType = conf.get(MIME_TYPE, null);
    if (mimeType != null) {
      metadata.put(MIME_TYPE, mimeType);
    }
    LWDocument document = createDocument(key.toString(), metadata);
    document.setContent(value.getBytes());

    return new LWDocument[] {document};
  }

}
