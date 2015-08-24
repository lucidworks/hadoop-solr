package com.lucidworks.hadoop.ingest;

import com.lucidworks.hadoop.io.LWDocumentWritable;
import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.junit.Ignore;

/**
 * Simple custom ingest reducer
 */
@Ignore
public class TestIngestReducer extends IngestReducer {
  long count = 0;

  @Override
  public void reduce(Text key, Iterator<LWDocumentWritable> values,
      OutputCollector<Text, LWDocumentWritable> output, Reporter reporter) throws IOException {
    count++;
    reduce(key, values, output, reporter);
    reporter.incrCounter("TestIngestReducer", "count", count);
  }
}
