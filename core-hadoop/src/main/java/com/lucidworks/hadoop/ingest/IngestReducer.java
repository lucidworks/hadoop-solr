package com.lucidworks.hadoop.ingest;

import com.lucidworks.hadoop.io.LWDocumentWritable;
import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/**
 *
 *
 **/
public class IngestReducer extends BaseHadoopIngest
    implements Reducer<Text, LWDocumentWritable, Text, LWDocumentWritable> {

  private AbstractJobFixture fixture = new AbstractJobFixture() {
    @Override
    public void init(JobConf conf) throws IOException {
      //DO nothing by default
    }
  };

  @Override
  public AbstractJobFixture getFixture() {
    return fixture;
  }

  @Override
  public void reduce(Text key, Iterator<LWDocumentWritable> values,
      OutputCollector<Text, LWDocumentWritable> output, Reporter reporter) throws IOException {
    while (values.hasNext()) {
      //TODO: LWSHADOOP-32:  Hook in Reduce side stages
      output.collect(key, values.next());
    }
  }

}
