package com.lucidworks.hadoop.ingest;

import com.lucidworks.hadoop.io.LWDocument;
import com.lucidworks.hadoop.io.LWDocumentWritable;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mrunit.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 *
 *
 **/
public class SipsIngestMapperTest extends BaseIngestMapperTestCase {

  private MapDriver<Text, DoubleWritable, Text, LWDocumentWritable> mapDriver;

  @Before
  public void setUp() throws IOException {
    SipsIngestMapper mapper = new SipsIngestMapper();
    mapDriver = new MapDriver<Text, DoubleWritable, Text, LWDocumentWritable>();
    mapDriver.setConfiguration(createConf());

    mapDriver.setMapper(mapper);
    Configuration conf = mapDriver.getConfiguration();
    setupCommonConf(conf);
    mapper.getFixture().init(new JobConf(mapDriver.getConfiguration()));
  }

  @Test
  public void test() throws Exception {
    for (int i = 0; i < 100; i++) {
      mapDriver.withInput(new Text("sip_" + i), new DoubleWritable(i / 100.0));
    }

    mapDriver.withCounter(BaseHadoopIngest.Counters.DOCS_ADDED, 100);
    List<Pair<Text, LWDocumentWritable>> run = mapDriver.run();
    Assert.assertEquals(100, run.size());
    LWDocument doc = run.get(0).getSecond().getLWDocument();
    Assert.assertNotNull("document is null", doc);
    Assert.assertTrue(doc.getId().startsWith("sip_"));

  }
}
