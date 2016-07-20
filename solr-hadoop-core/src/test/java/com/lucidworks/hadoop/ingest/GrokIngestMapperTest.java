package com.lucidworks.hadoop.ingest;

import com.lucidworks.hadoop.ingest.util.GrokHelper;
import com.lucidworks.hadoop.io.LWDocument;
import com.lucidworks.hadoop.io.LWDocumentProvider;
import com.lucidworks.hadoop.io.LWDocumentWritable;
import java.io.File;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mrunit.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.jruby.Ruby;
import org.jruby.RubyArray;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class GrokIngestMapperTest extends BaseIngestMapperTestCase {

  private MapDriver<LongWritable, Text, Text, LWDocumentWritable> mapDriver;
  private JobConf jobConf;
  private GrokIngestMapper mapper;

  @Before
  public void setUp() throws Exception {
    mapper = new GrokIngestMapper();
    mapDriver = new MapDriver<LongWritable, Text, Text, LWDocumentWritable>();
    mapDriver.setConfiguration(createConf());

    mapDriver.setMapper(mapper);
    Configuration configuration = mapDriver.getConfiguration();

    setupCommonConf(configuration);
    jobConf = new JobConf(configuration);
  }

  @Test
  public void testIPWORDpattern() throws Exception {
    Path path = new Path(GrokIngestMapperTest.class.getClassLoader()
        .getResource("grok" + File.separator + "IP-WORD.conf").getPath());
    jobConf.set(GrokIngestMapper.GROK_URI, path.toString());
    mapper.getFixture().init(jobConf);
    mapDriver.withConfiguration(jobConf);
    String splitFilePath = "/path/to/log";
    mapDriver.setMapInputPath(new Path(splitFilePath));

    LongWritable lineNumb = new LongWritable(10);

    String message = "192.168.1.1 WORD__1 This is the rest of the message";
    mapDriver.withInput(lineNumb, new Text(message));

    // TODO: Check Fields
  }

  @Test
  public void testGrokFail() throws Exception {
    Path path = new Path(GrokIngestMapperTest.class.getClassLoader()
        .getResource("grok" + File.separator + "IP-WORD.conf").getPath());
    jobConf.set(GrokIngestMapper.GROK_URI, path.toString());
    mapper.getFixture().init(jobConf);
    mapDriver.withConfiguration(jobConf);
    String splitFilePath = "/path/to/log";
    mapDriver.setMapInputPath(new Path(splitFilePath));

    LongWritable lineNumb = new LongWritable(10);

    String message = "non matching string";
    mapDriver.withInput(lineNumb, new Text(message));

    List<Pair<Text, LWDocumentWritable>> run = mapDriver.run();
    Assert.assertEquals(1, run.size());

    Pair<Text, LWDocumentWritable> pair = run.get(0);
    LWDocument doc = pair.getSecond().getLWDocument();
    // TODO: Check Fields
  }

  @Test
  public void testMonthDayYearGreedy() throws Exception {
    Path path = new Path(GrokIngestMapperTest.class.getClassLoader()
        .getResource("grok" + File.separator + "Month-Day-Year-Greedy.conf").getPath());
    jobConf.set(GrokIngestMapper.GROK_URI, path.toString());
    mapper.getFixture().init(jobConf);
    mapDriver.withConfiguration(jobConf);
    String splitFilePath = "/path/to/log";
    mapDriver.setMapInputPath(new Path(splitFilePath));

    LongWritable lineNumb = new LongWritable(10);

    String message = "Jan 05 2014 key1=value1 key2=value2 key3=value3";
    mapDriver.withInput(lineNumb, new Text(message));

    List<Pair<Text, LWDocumentWritable>> run = mapDriver.run();
    Assert.assertEquals(1, run.size());

    Pair<Text, LWDocumentWritable> pair = run.get(0);
    LWDocument doc = pair.getSecond().getLWDocument();
    Assert.assertNotNull(doc);
    // TODO: Check Fields
  }

  @Test
  public void testGrok2Fail() throws Exception {

    Path path = new Path(GrokIngestMapperTest.class.getClassLoader()
        .getResource("grok" + File.separator + "Month-Day-Year-Greedy.conf").getPath());
    jobConf.set(GrokIngestMapper.GROK_URI, path.toString());
    mapper.getFixture().init(jobConf);
    mapDriver.withConfiguration(jobConf);
    String splitFilePath = "/path/to/log";
    mapDriver.setMapInputPath(new Path(splitFilePath));

    LongWritable lineNumb = new LongWritable(10);

    String message = "non matching string";
    mapDriver.withInput(lineNumb, new Text(message));

    List<Pair<Text, LWDocumentWritable>> run = mapDriver.run();
    Assert.assertEquals(1, run.size());

    Pair<Text, LWDocumentWritable> pair = run.get(0);
    LWDocument doc = pair.getSecond().getLWDocument();
    // TODO: Check Fields
  }

  @Test
  public void testCISCOPattern() throws Exception {
    // Adding configuration file
    Path path = new Path(GrokIngestMapperTest.class.getClassLoader()
        .getResource("grok" + File.separator + "CISCO.conf").getPath());

    // Adding extra patterns file
    Ruby runtime = Ruby.newInstance();
    RubyArray rubyArray = RubyArray.newArray(runtime);
    rubyArray.add(GrokIngestMapperTest.class.getClassLoader()
        .getResource("grok" + File.separator + "extra_patterns.txt").getPath());
    jobConf.set(GrokIngestMapper.GROK_URI, path.toString());
    mapper.getFixture().init(jobConf);
    mapDriver.withConfiguration(jobConf);
    String splitFilePath = "/path/to/log";
    mapDriver.setMapInputPath(new Path(splitFilePath));

    LongWritable lineNumb = new LongWritable(10);

    String message = "Mar 20 2014 20:10:45 key1=value1 key2=value2 key3=value3";
    mapDriver.withInput(lineNumb, new Text(message));

    List<Pair<Text, LWDocumentWritable>> run = mapDriver.run();
    Assert.assertEquals(1, run.size());

    Pair<Text, LWDocumentWritable> pair = run.get(0);
    LWDocument doc = pair.getSecond().getLWDocument();
    Assert.assertNotNull(doc);
    // TODO: Check Fields
  }

  @Test
  public void testSyslog() throws Exception {

    // Adding configuration file
    Path path = new Path(GrokIngestMapperTest.class.getClassLoader()
        .getResource("grok" + File.separator + "Syslog.conf").getPath());

    jobConf.set(GrokIngestMapper.GROK_URI, path.toString());
    mapper.getFixture().init(jobConf);
    mapDriver.withConfiguration(jobConf);
    String splitFilePath = "/path/to/log";
    mapDriver.setMapInputPath(new Path(splitFilePath));

    LongWritable lineNumb = new LongWritable(10);

    String message = "<34>Oct 11 22:14:15 192.168.1.10 su: 'su root' failed for lonvick on /dev/pts/8";
    mapDriver.withInput(lineNumb, new Text(message));

    List<Pair<Text, LWDocumentWritable>> run = mapDriver.run();
    Assert.assertEquals(1, run.size());

    Pair<Text, LWDocumentWritable> pair = run.get(0);
    LWDocument doc = pair.getSecond().getLWDocument();
    Assert.assertNotNull(doc);
    // TODO: Check Fields
  }

  @Test
  public void testFirewall() throws Exception {

    // Adding configuration file
    Path path = new Path(GrokIngestMapperTest.class.getClassLoader()
        .getResource("grok" + File.separator + "firewall.conf").getPath());
    jobConf.set(GrokIngestMapper.GROK_URI, path.toString());
    mapper.getFixture().init(jobConf);
    mapDriver.withConfiguration(jobConf);
    String splitFilePath = "/path/to/log";
    mapDriver.setMapInputPath(new Path(splitFilePath));

    LongWritable lineNumb = new LongWritable(10);

    String message = "Mar 31 2014 18:02:36: %ASA-5-106100: access-list inbound denied tcp outside/128.241.220.82(3154) -> asp3/62.84.96.19(32005) hit-cnt 1 first hit [0x91c26a3, 0x0]";
    mapDriver.withInput(lineNumb, new Text(message));

    List<Pair<Text, LWDocumentWritable>> run = mapDriver.run();
    Assert.assertEquals(1, run.size());

    Pair<Text, LWDocumentWritable> pair = run.get(0);
    LWDocument doc = pair.getSecond().getLWDocument();
    Assert.assertNotNull(doc);
    // TODO: Check Fields
  }

  @Ignore
  @Test
  public void testAdditionalPattern() throws Exception {

    // Generate the HDFS hierarchy
    FileSystem fs = FileSystem.getLocal(jobConf);
    Path dir = new Path(fs.getWorkingDirectory(), "build");
    Path sub = new Path(dir, "GHT");
    Path tempDir = new Path(sub, "tmp-dir");
    Path base = new Path(sub, "tmp-dir-2");
    fs.mkdirs(tempDir);

    // Copy extra patterns file to HDFS
    Path dst = new Path(base, "extra_patterns.txt");
    Path src = new Path(GrokIngestMapperTest.class.getClassLoader()
        .getResource("grok" + File.separator + "extra_patterns.txt").getPath());
    fs.copyFromLocalFile(src, dst);

    // Adding configuration file
    Path confPath = new Path(GrokIngestMapperTest.class.getClassLoader()
        .getResource("grok" + File.separator + "customPattern.conf").toURI().getPath());

    // Adding extra patterns file
    Ruby runtime = Ruby.newInstance();
    RubyArray rubyArray = RubyArray.newArray(runtime);
    rubyArray.add(
        base.toUri().getPath() + File.separator + "extra_patterns.txt");
    GrokHelper.addPatternDirToDC(rubyArray, jobConf);
    jobConf.set(GrokIngestMapper.GROK_URI, confPath.toString());
    mapper.getFixture().init(jobConf);
    mapDriver.withConfiguration(jobConf);
    String splitFilePath = "/path/to/log";
    mapDriver.setMapInputPath(new Path(splitFilePath));

    LongWritable lineNumb = new LongWritable(10);

    String message = "192.168.1.1 123456 rest of the message";
    mapDriver.withInput(lineNumb, new Text(message));

    List<Pair<Text, LWDocumentWritable>> run = mapDriver.run();
    Assert.assertEquals(1, run.size());

    Pair<Text, LWDocumentWritable> pair = run.get(0);
    LWDocument doc = pair.getSecond().getLWDocument();
    Assert.assertNotNull(doc);
    // TODO: Check Fields
  }

  @Test
  public void testByteOffsetField() throws Exception {
    Path path = new Path(GrokIngestMapperTest.class.getClassLoader()
        .getResource("grok" + File.separator + "IP-WORD.conf").getPath());
    jobConf.set(GrokIngestMapper.GROK_URI, path.toString());
    mapper.getFixture().init(jobConf);
    mapDriver.withConfiguration(jobConf);
    String splitFilePath = "/path/to/log";
    mapDriver.setMapInputPath(new Path(splitFilePath));

    int offset = 0;
    LongWritable offset1 = new LongWritable(offset);
    String message1 = "192.168.1.1 WORD__1 This is the rest of the message";

    LongWritable offset2 = new LongWritable(offset + message1.length());
    String message2 = "112.37.117.33 WORD__2 This is the rest of the message";
    mapDriver.withInput(offset1, new Text(message1));
    mapDriver.withInput(offset2, new Text(message2));

    List<Pair<Text, LWDocumentWritable>> run = mapDriver.run();
    Assert.assertEquals(2, run.size());

    // Verifying first document
    Pair<Text, LWDocumentWritable> pair = run.get(0);
    LWDocument doc = pair.getSecond().getLWDocument();
    Assert.assertNotNull(doc);

    // TODO: Check Fields
  }
}
