package com.lucidworks.hadoop.ingest;

import com.lucidworks.hadoop.io.LWDocument;
import com.lucidworks.hadoop.io.LWDocumentProvider;
import com.lucidworks.hadoop.io.LWDocumentWritable;
import com.lucidworks.hadoop.utils.TestUtils;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mrunit.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static com.lucidworks.hadoop.utils.ConfigurationKeys.COLLECTION;
import static com.lucidworks.hadoop.utils.ConfigurationKeys.ZK_CONNECT;
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;

/**
 *
 *
 **/
public class CSVIngestMapperTest extends BaseIngestMapperTestCase {

  private MapDriver<LongWritable, Text, Text, LWDocumentWritable> mapDriver;

  @Before
  public void setUp() throws IOException {
    CSVIngestMapper mapper = new CSVIngestMapper();
    mapDriver = new MapDriver<>();
    mapDriver.setMapper(mapper);

    mapDriver.setConfiguration(createConf());
    Configuration configuration = mapDriver.getConfiguration();
    configuration.set(COLLECTION, "collection");
    configuration.set(ZK_CONNECT, "localhost:0000");
    configuration.set("idField", "id");

    JobConf conf = new JobConf(configuration);
    LWDocumentProvider.init(conf);
  }

  @Test
  public void test() throws Exception {
    Configuration conf = mapDriver.getConfiguration();
    conf.set(CSVIngestMapper.CSV_IGNORE_FIRST_LINE_COMMENT, "true");
    conf.set(CSVIngestMapper.CSV_DELIMITER, ",");
    conf.set(CSVIngestMapper.CSV_FIELD_MAPPING, "0=id,1=bar, 2=junk , 3 = zen ,   4 = hockey");

    LongWritable key = new LongWritable(0);// should be skipped
    Text value = new Text("id,bar,junk,zen,hockey");// we skip the 0th line

    mapDriver.withInput(key, value);
    mapDriver.runTest();
    assertEquals("Should be 0 documents", 0,
        mapDriver.getCounters().findCounter(BaseHadoopIngest.Counters.DOCS_ADDED).getValue());
    key = new LongWritable(1);
    value = new Text("id-1, The quick brown fox, jumped, head, gretzky, extra");
    // extra should be mapped to field_5
    mapDriver.withInput(key, value);
    mapDriver.withCounter(BaseHadoopIngest.Counters.DOCS_ADDED, 1);
    LWDocumentWritable val = TestUtils
        .createLWDocumentWritable("id-1", "bar", "The quick brown fox", "junk", "jumped", "zen",
            "head", "hockey", "gretzky", "field_5", "extra");
    mapDriver.withOutput(new Text("id-1"), val);
    mapDriver.runTest();
  }

  @Test
  public void testDefaultFieldID() throws Exception {

    Configuration conf = mapDriver.getConfiguration();
    conf.set(CSVIngestMapper.CSV_IGNORE_FIRST_LINE_COMMENT, "true");
    conf.set(CSVIngestMapper.CSV_DELIMITER, ",");
    // The "id" is in a different position
    conf.set(CSVIngestMapper.CSV_FIELD_MAPPING, "0=bar, 1=id, 2=junk, 3=zen, 4 = hockey");

    LongWritable key = new LongWritable(0);// should be skipped
    Text value = new Text("bar, id,junk,zen,hockey");// we skip the 0th line

    mapDriver.withInput(key, value);
    mapDriver.runTest();
    assertEquals("Should be 0 documents", 0,
        mapDriver.getCounters().findCounter(BaseHadoopIngest.Counters.DOCS_ADDED).getValue());
    key = new LongWritable(1);
    value = new Text("The quick brown fox, id-1, jumped, head, gretzky, extra");
    // extra should be mapped to field_5
    mapDriver.withInput(key, value);
    mapDriver.withCounter(BaseHadoopIngest.Counters.DOCS_ADDED, 1);
    LWDocumentWritable val = TestUtils
        .createLWDocumentWritable("id-1", "bar", "The quick brown fox", "junk", "jumped", "zen",
            "head", "hockey", "gretzky", "field_5", "extra");
    mapDriver.withOutput(new Text("id-1"), val);
    mapDriver.runTest();
  }

  @Test
  public void testFieldID() throws Exception {

    Configuration conf = mapDriver.getConfiguration();
    conf.set(CSVIngestMapper.CSV_IGNORE_FIRST_LINE_COMMENT, "true");
    conf.set(CSVIngestMapper.CSV_DELIMITER, ",");
    // The "id" is in a different position and has a diferent name my-id
    conf.set(CSVIngestMapper.CSV_FIELD_MAPPING, "0=bar, 1=junk, 2=my-id, 3=zen, 4 = hockey");
    // Set the field id
    conf.set("idField", "my-id");
    LongWritable key = new LongWritable(0);// should be skipped
    Text value = new Text("bar, id,junk,zen,hockey");// we skip the 0th line

    mapDriver.withInput(key, value);
    mapDriver.runTest();
    assertEquals("Should be 0 documents", 0,
        mapDriver.getCounters().findCounter(BaseHadoopIngest.Counters.DOCS_ADDED).getValue());
    key = new LongWritable(1);
    value = new Text("The quick brown fox, jumped, id-1, head, gretzky, extra");
    // extra should be mapped to field_5
    mapDriver.withInput(key, value);
    mapDriver.withCounter(BaseHadoopIngest.Counters.DOCS_ADDED, 1);
    LWDocumentWritable val = TestUtils
        .createLWDocumentWritable("id-1", "bar", "The quick brown fox", "junk", "jumped", "zen",
            "head", "hockey", "gretzky", "field_5", "extra");
    mapDriver.withOutput(new Text("id-1"), val);
    mapDriver.runTest();
  }

  @Test
  public void testWithoutFieldID() throws Exception {

    Configuration conf = mapDriver.getConfiguration();
    conf.set(CSVIngestMapper.CSV_IGNORE_FIRST_LINE_COMMENT, "true");
    conf.set(CSVIngestMapper.CSV_DELIMITER, ",");
    // The "id" is in a different position
    conf.set(CSVIngestMapper.CSV_FIELD_MAPPING, "0=bar, 1=id, 2=junk, 3=zen, 4 = hockey");
    // Set another field to be the id
    conf.set("idField", "junk");

    LongWritable key = new LongWritable(0);// should be skipped
    Text value = new Text("bar, id,junk,zen,hockey");// we skip the 0th line

    mapDriver.withInput(key, value);
    mapDriver.runTest();
    assertEquals("Should be 0 documents", 0,
        mapDriver.getCounters().findCounter(BaseHadoopIngest.Counters.DOCS_ADDED).getValue());
    key = new LongWritable(1);
    value = new Text("The quick brown fox, id-1, jumped, head, gretzky, extra");
    // extra should be mapped to field_5
    mapDriver.withInput(key, value);
    mapDriver.withCounter(BaseHadoopIngest.Counters.DOCS_ADDED, 1);
    LWDocumentWritable val = TestUtils
        .createLWDocumentWritable("jumped", "bar", "The quick brown fox", "id", "id-1", "zen",
            "head", "hockey", "gretzky", "field_5", "extra");
    mapDriver.withOutput(new Text("jumped"), val);
    mapDriver.runTest();
  }

  @Test
  public void testOptions() throws Exception {

    Configuration conf = mapDriver.getConfiguration();
    conf.set(CSVIngestMapper.CSV_FIELD_MAPPING, "0=id,1=bar, 2=junk , 3 = zen ,   4 = hockey");
    conf.set(CSVIngestMapper.CSV_DELIMITER, "|");
    conf.set(CSVIngestMapper.CSV_IGNORE_FIRST_LINE_COMMENT, "false");

    LongWritable key = new LongWritable(0);// not skipped
    Text value = new Text("id-0|bar|junk|zen|hockey");
    mapDriver.withInput(key, value);
    mapDriver.withCounter(BaseHadoopIngest.Counters.DOCS_ADDED, 1);

    LWDocumentWritable val = TestUtils
        .createLWDocumentWritable("id-0", "bar", "bar", "junk", "junk", "zen", "zen", "hockey",
            "hockey");
    mapDriver.withOutput(new Text("id-0"), val);
    mapDriver.runTest();


    key = new LongWritable(1);
    value = new Text("id-1|The quick brown fox|jumped|head|gretzky|extra");
    // extra should be mapped to field_5
    mapDriver.withInput(key, value);
    val = TestUtils
        .createLWDocumentWritable("id-1", "bar", "The quick brown fox", "junk", "jumped", "zen",
            "head", "hockey", "gretzky", "field_5", "extra");
    mapDriver.withOutput(new Text("id-1"), val);
    mapDriver.resetExpectedCounters();
    mapDriver.runTest();
    // TODO: check fields
  }

  @Test
  public void testVariations() throws Exception {
    Configuration conf = mapDriver.getConfiguration();
    conf.clear();
    conf.set("io.serializations", "com.lucidworks.hadoop.io.impl.LWMockSerealization");
    conf.set(ZK_CONNECT, "localhost:0000");
    conf.set("idField", "id");
    // no collection set
    LongWritable key = new LongWritable(0);// not skipped
    Text value = new Text("id-0|bar|junk|zen|hockey");
    mapDriver.withInput(key, value);
    try {
      List<Pair<Text, LWDocumentWritable>> run = mapDriver.run();
      Assert.fail();
    } catch (Exception e) {
      // expected
    }
    conf.set(COLLECTION, "foo");
    conf.set(CSVIngestMapper.CSV_DELIMITER, "|");
    conf.set(CSVIngestMapper.CSV_IGNORE_FIRST_LINE_COMMENT, "false");
    // no field mapping
    List<Pair<Text, LWDocumentWritable>> run = mapDriver.run();
    Assert.assertEquals(1, run.size());
    Pair<Text, LWDocumentWritable> pair = run.get(0);
    LWDocument doc = pair.getSecond().getLWDocument();
    Assert.assertNotNull(doc);
    // TODO: check fields

  }

  @Test
  public void testStrategy() throws Exception {
    Configuration conf = mapDriver.getConfiguration();
    conf.set(CSVIngestMapper.CSV_IGNORE_FIRST_LINE_COMMENT, "false");
    conf.set(CSVIngestMapper.CSV_DELIMITER, ",");
    conf.set(CSVIngestMapper.CSV_FIELD_MAPPING, "0=id,1=count,2=body, 3=title,4=footer");
    conf.set(CSVIngestMapper.CSV_STRATEGY, CSVIngestMapper.EXCEL_STRATEGY);
    LongWritable key = new LongWritable(0);// not skipped
    Text value = new Text("id-0,bar,junk,zen,hockey");
    LWDocument doc;
    mapDriver.withInput(key, value);
    List<Pair<Text, LWDocumentWritable>> run = mapDriver.run(false);
    Assert.assertEquals(1, run.size());
    doc = run.get(0).getSecond().getLWDocument();

    Assert.assertNotNull(doc);
    // TODO: check fields
  }

  @Test
  public void testFrankenstein() throws Exception {
    Configuration conf = mapDriver.getConfiguration();

    conf.set(CSVIngestMapper.CSV_IGNORE_FIRST_LINE_COMMENT, "true");
    conf.set(CSVIngestMapper.CSV_DELIMITER, ",");
    conf.set(CSVIngestMapper.CSV_FIELD_MAPPING, "0=id,1=count,2=body, 3=title,4=footer");

    InputStream frank = CSVIngestMapperTest.class.getClassLoader()
        .getResourceAsStream("csv" + File.separator + "frank.csv");
    assertNotNull("frank is null", frank);
    BufferedReader reader = new BufferedReader(new InputStreamReader(frank));
    String line = null;
    int i = 0;
    while ((line = reader.readLine()) != null) {
      String[] splits = line.split(",");
      String id = splits[0];
      LongWritable lineNumb = new LongWritable(i);
      mapDriver.withInput(lineNumb, new Text(line));

      i++;
    }
    List<Pair<Text, LWDocumentWritable>> out = mapDriver.run(true);
    // TODO: do some validation here, as previous attempts above aren't working
    assertEquals(79, i);
    assertEquals(i - 1, out.size());// we are skipping first line
  }
}
