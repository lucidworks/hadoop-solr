package com.lucidworks.hadoop.ingest;

import com.lucidworks.hadoop.io.LWDocument;
import com.lucidworks.hadoop.io.LWDocumentWritable;
import edu.cmu.lemurproject.WarcFileRecordReader;
import edu.cmu.lemurproject.WritableWarcRecord;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mrunit.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;

/**
 *
 *
 **/
public class WarcIngestMapperTest extends BaseIngestMapperTestCase {
  MapDriver<LongWritable, WritableWarcRecord, Text, LWDocumentWritable> mapDriver;

  @Before
  public void setUp() throws IOException {
    WarcIngestMapper mapper = new WarcIngestMapper();
    mapDriver = new MapDriver<LongWritable, WritableWarcRecord, Text, LWDocumentWritable>();
    mapDriver.setConfiguration(createConf());

    mapDriver.setMapper(mapper);
    setupCommonConf(mapDriver.getConfiguration());
    mapper.getFixture().init(new JobConf(mapDriver.getConfiguration()));
  }

  @Test
  public void testWarc() throws Exception {

    String[] the_ids = new String[] { "<urn:uuid:00fee1bb-1abc-45a6-af31-a164c2fdad88>",
        "<urn:uuid:6ee9accb-a284-47ef-8785-ed28aee2f79e>",
        "<urn:uuid:00fee1bb-1abc-45a6-af31-a164c2fdad88",
        "<urn:uuid:00fee1bb-1abc-45a6-af31-a164c2fdad88>",
        "<urn:uuid:b328f1fe-b2ee-45c0-9139-908850810b52>",
        "<urn:uuid:ccea02fa-a954-4c19-ace2-72a73b04d95d>" };
    Set<String> ids = new HashSet<String>(Arrays.asList(the_ids));//kind of ugly
    Path path = new Path(
        WarcIngestMapperTest.class.getClassLoader().getResource("warc/at.warc").toURI());
    FileSystem fs = FileSystem.get(mapDriver.getConfiguration());
    Path dir = new Path(fs.getWorkingDirectory(), "build");
    Path sub = new Path(dir, "WIMT");
    Path tempDir = new Path(sub, "tmp-dir");
    fs.mkdirs(tempDir);
    Path dst = new Path(tempDir, "warc/at.warc");
    fs.copyFromLocalFile(path, dst);
    InputSplit split = new FileSplit(dst, 0, fs.getFileStatus(dst).getLen(), (String[]) null);
    WarcFileRecordReader wfrr = new WarcFileRecordReader(mapDriver.getConfiguration(), split);
    LongWritable key = new LongWritable(0);
    WritableWarcRecord value = new WritableWarcRecord();
    int i = 0;
    while (wfrr.next(key, value)) {
      mapDriver.withInput(key, value);
      i++;
    }
    List<Pair<Text, LWDocumentWritable>> run = mapDriver.run();
    Assert.assertTrue(i > 1);
    assertEquals(i, run.size());
    //get one doc just to confirm:
    LWDocument doc = run.get(0).getSecond().getLWDocument();
    Assert.assertNotNull("document is null", doc);
    Assert.assertTrue(ids.contains(doc.getId()));
  }
}
