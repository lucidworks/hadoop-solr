package com.lucidworks.hadoop.ingest;

import com.lucidworks.hadoop.io.LWDocument;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;

public class SequenceFileIngestMapper extends AbstractIngestMapper<Writable, Writable> {

  private final AbstractJobFixture fixture = new AbstractJobFixture() {
    @Override
    public void init(JobConf conf) throws IOException {
      conf.setInputFormat(SequenceFileInputFormat.class);
    }
  };

  @Override
  public AbstractJobFixture getFixture() {
    return fixture;
  }

  public enum Counters {
    TEXT,
    BYTES_WRITABLE,
    RAW_WRITABLE
  }

  @Override
  public LWDocument[] toDocuments(Writable key, Writable value, Reporter reporter,
      Configuration conf) throws IOException {
    LWDocument doc = createDocument(key.toString(), null);
    if (value instanceof Text) {
      doc.setContent(((Text) value).getBytes());
      reporter.getCounter(Counters.TEXT).increment(1);
    } else if (value instanceof BytesWritable) {
      // Copy the bytes for this one
      BytesWritable value_ = (BytesWritable) value;
      byte[] data = new byte[value_.getLength()];
      doc.setContent(data);
      System.arraycopy(value_.getBytes(), 0, data, 0, value_.getLength());
      reporter.getCounter(Counters.BYTES_WRITABLE).increment(1);
    } else {
      doc.setContent(WritableUtils.toByteArray(value));
      reporter.getCounter(Counters.RAW_WRITABLE).increment(1);
    }
    return new LWDocument[] {doc};
  }

}
