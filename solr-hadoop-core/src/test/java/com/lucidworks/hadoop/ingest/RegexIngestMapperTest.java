package com.lucidworks.hadoop.ingest;

import com.lucidworks.hadoop.io.LWDocument;
import com.lucidworks.hadoop.io.LWDocumentWritable;
import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mrunit.MapDriver;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileIterator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 *
 *
 **/
public class RegexIngestMapperTest extends BaseIngestMapperTestCase {
  MapDriver<Writable, Writable, Text, LWDocumentWritable> mapDriver;

  @Before
  public void setUp() throws IOException {
    RegexIngestMapper mapper = new RegexIngestMapper();
    mapDriver = new MapDriver<Writable, Writable, Text, LWDocumentWritable>();
    mapDriver.setConfiguration(createConf());

    mapDriver.setMapper(mapper);
    setupCommonConf(mapDriver.getConfiguration());
    mapper.getFixture().init(new JobConf(mapDriver.getConfiguration()));
  }

  @Test
  public void test() throws Exception {
    Configuration conf = mapDriver.getConfiguration();
    conf.set(RegexIngestMapper.REGEX, "\\w+");
    conf.set(RegexIngestMapper.GROUPS_TO_FIELDS, "0=body");

    Path path = new Path(RegexIngestMapperTest.class.getClassLoader()
        .getResource("sequence" + File.separator + "frankenstein_text_text.seq").toURI());
    FileSystem fs = FileSystem.get(conf);
    Path dir = new Path(fs.getWorkingDirectory(), "build");
    Path sub = new Path(dir, "REGIMT");
    Path tempDir = new Path(sub, "tmp-dir");
    fs.mkdirs(tempDir);
    Path dst = new Path(tempDir, "frank_t_t.seq");
    fs.copyFromLocalFile(path, dst);
    SequenceFileIterator<Text, Text> iterator = new SequenceFileIterator<Text, Text>(dst, true,
        conf);
    int i = 0;
    Set<String> ids = new HashSet<String>();
    while (iterator.hasNext()) {
      Pair<Text, Text> pair = iterator.next();
      ids.add(pair.getFirst().toString());
      mapDriver.withInput(pair.getFirst(), pair.getSecond());
      i++;
    }

    List<org.apache.hadoop.mrunit.types.Pair<Text, LWDocumentWritable>> run = mapDriver.run();
    assertTrue(i > 1);
    assertEquals(i, run.size());
    // get one doc just to confirm:
    LWDocument doc = run.get(0).getSecond().getLWDocument();
    assertNotNull("document is null", doc);
    int runSize = run.size();
    assertEquals(776, runSize);
    assertTrue(doc.getId().contains("frank_seq_1"));
  }

  @Test
  public void testMatch() throws Exception {
    Configuration conf = mapDriver.getConfiguration();
    conf.set(RegexIngestMapper.REGEX, "(\\w+)\\s+(\\d+)\\s+(\\w+)\\s+(\\d+)");
    conf.set(RegexIngestMapper.GROUPS_TO_FIELDS, "0=body,1=text,2=number");
    conf.setBoolean(RegexIngestMapper.REGEX_MATCH, true);
    Pair<Text, Text> pair = new Pair<Text, Text>(new Text("id"), new Text("text 1 text 2"));
    mapDriver.withInput(pair.getFirst(), pair.getSecond());
    List<org.apache.hadoop.mrunit.types.Pair<Text, LWDocumentWritable>> run = mapDriver.run();
    Assert.assertEquals(1, run.size());
    LWDocument doc = run.get(0).getSecond().getLWDocument();
    assertNotNull("document is null", doc);
    // TODO: Check Fields

  }

  @Test
  public void testSimple() throws Exception {
    Configuration conf = mapDriver.getConfiguration();
    conf.set(RegexIngestMapper.REGEX, "(\\w+)\\s+(\\d+)");
    conf.set(RegexIngestMapper.GROUPS_TO_FIELDS, "0=body,1=text,2=number");
    Pair<Text, Text> pair = new Pair<Text, Text>(new Text("id"), new Text("text 1 text 2"));
    mapDriver.withInput(pair.getFirst(), pair.getSecond());
    List<org.apache.hadoop.mrunit.types.Pair<Text, LWDocumentWritable>> run = mapDriver.run();
    Assert.assertEquals(1, run.size());
    LWDocument doc = run.get(0).getSecond().getLWDocument();
    assertNotNull("document is null", doc);
    // TODO: Check Fields

  }

  @Test
  public void testBad() throws Exception {
    Configuration conf = mapDriver.getConfiguration();
    Pair<Text, Text> pair = new Pair<Text, Text>(new Text("id"), new Text("doesn't matter"));
    mapDriver.withInput(pair.getFirst(), pair.getSecond());

    try {
      List<org.apache.hadoop.mrunit.types.Pair<Text, LWDocumentWritable>> run = mapDriver.run();
      Assert.fail();
    } catch (RuntimeException e) {
      Assert.assertEquals(
          "com.lucidworks.hadoop.ingest.RegexIngestMapper.regex property must not be null or empty",
          e.getCause().getCause().getMessage());// two deep on the
      // exceptions
    }

    conf.set(RegexIngestMapper.REGEX, "");
    mapDriver.withInput(pair.getFirst(), pair.getSecond());

    try {
      List<org.apache.hadoop.mrunit.types.Pair<Text, LWDocumentWritable>> run = mapDriver.run();
      Assert.fail();
    } catch (RuntimeException e) {
      Assert.assertEquals(
          "com.lucidworks.hadoop.ingest.RegexIngestMapper.regex property must not be null or empty",
          e.getCause().getCause().getMessage());// two deep on the
      // exceptions
    }

    conf.set(RegexIngestMapper.REGEX, "\\w+");// good regex, bad group mapping
    conf.set(RegexIngestMapper.GROUPS_TO_FIELDS, "");
    try {
      List<org.apache.hadoop.mrunit.types.Pair<Text, LWDocumentWritable>> run = mapDriver.run();
      Assert.fail();
    } catch (RuntimeException e) {
      assertTrue(e.getCause().getCause().getMessage().startsWith(
          "com.lucidworks.hadoop.ingest.RegexIngestMapper.groups_to_fields property must not be null or empty"));// two
      // deep
      // on
      // the
      // exceptions
    }

    conf.set(RegexIngestMapper.REGEX, "\\w+");// good regex, bad group mapping
    conf.set(RegexIngestMapper.GROUPS_TO_FIELDS, "0");
    try {
      List<org.apache.hadoop.mrunit.types.Pair<Text, LWDocumentWritable>> run = mapDriver.run();
      Assert.fail();
    } catch (RuntimeException e) {
      assertTrue(e.getCause().getCause().getMessage().startsWith(
          "Malformed com.lucidworks.hadoop.ingest.RegexIngestMapper.groups_to_fields"));// two
      // deep
      // on
      // the
      // exceptions
    }

    conf.set(RegexIngestMapper.GROUPS_TO_FIELDS, "0=foo,1");
    try {
      List<org.apache.hadoop.mrunit.types.Pair<Text, LWDocumentWritable>> run = mapDriver.run();
      Assert.fail();
    } catch (RuntimeException e) {
      assertTrue(e.getCause().getCause().getMessage().startsWith(
          "Malformed com.lucidworks.hadoop.ingest.RegexIngestMapper.groups_to_fields"));// two
      // deep
      // on
      // the
      // exceptions
    }

    RegexIngestMapper mapper = new RegexIngestMapper();
    LWDocument[] docs = mapper.toDocuments(null, null, null, null);
    Assert.assertNull(docs);
  }

  @Test
  public void testDuplicatedId() throws Exception {
    Configuration conf = mapDriver.getConfiguration();
    conf.set(RegexIngestMapper.REGEX, "\\w+");
    conf.set(RegexIngestMapper.GROUPS_TO_FIELDS, "0=body");

    FileSystem fs = FileSystem.get(conf);
    Path dir = new Path(fs.getWorkingDirectory(), "build");
    Path sub = new Path(dir, "REGIMT");
    Path tempDir = new Path(sub, "tmp-dir");
    fs.mkdirs(tempDir);

    // First File
    mapDriver.withInput(new Text("0"), new Text("File content one"));
    List<org.apache.hadoop.mrunit.types.Pair<Text, LWDocumentWritable>> run1 = mapDriver.run();
    assertTrue(run1.size() == 1);
    LWDocument doc = run1.get(0).getSecond().getLWDocument();
    assertNotNull("document is null", doc);
    String file1Id = doc.getId();

    Thread.sleep(10);
    // Second File
    mapDriver.clearInput();
    mapDriver.withInput(new Text("0"), new Text("File content two"));
    List<org.apache.hadoop.mrunit.types.Pair<Text, LWDocumentWritable>> run2 = mapDriver.run();
    assertTrue(run2.size() == 1);
    doc = run2.get(0).getSecond().getLWDocument();
    assertNotNull("document is null", doc);
    String file2Id = doc.getId();

    assertTrue(!file1Id.equals(file2Id));
  }

  @Test
  public void testPathField() throws Exception {
    Configuration conf = mapDriver.getConfiguration();
    conf.set(RegexIngestMapper.REGEX, "\\w+");
    conf.set(RegexIngestMapper.GROUPS_TO_FIELDS, "0=body");

    FileSystem fs = FileSystem.get(conf);
    Path dir = new Path(fs.getWorkingDirectory(), "build");
    Path sub = new Path(dir, "REGIMT");
    Path tempDir = new Path(sub, "tmp-dir");
    fs.mkdirs(tempDir);

    // First File
    String splitFilePath = "/path/to/file1.txt";
    mapDriver.setMapInputPath(new Path(splitFilePath));
    mapDriver.withInput(new Text("0"), new Text("File content one"));
    List<org.apache.hadoop.mrunit.types.Pair<Text, LWDocumentWritable>> run1 = mapDriver.run();
    assertTrue(run1.size() == 1);
    LWDocument doc = run1.get(0).getSecond().getLWDocument();
    assertNotNull("document is null", doc);
    // TODO: Check Fields

  }
}
