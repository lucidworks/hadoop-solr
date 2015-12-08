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
import org.junit.Before;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 *
 *
 **/
public class SequenceFileIngestMapperTest extends BaseIngestMapperTestCase {
  MapDriver<Writable, Writable, Text, LWDocumentWritable> mapDriver;

  @Before
  public void setUp() throws IOException {
    SequenceFileIngestMapper mapper = new SequenceFileIngestMapper();
    mapDriver = new MapDriver<Writable, Writable, Text, LWDocumentWritable>();
    mapDriver.setConfiguration(createConf());

    mapDriver.setMapper(mapper);
    setupCommonConf(mapDriver.getConfiguration());
    mapper.getFixture().init(new JobConf(mapDriver.getConfiguration()));
  }

  @Test
  public void test() throws Exception {
    Configuration conf = mapDriver.getConfiguration();

    Path path = new Path(SequenceFileIngestMapperTest.class.getClassLoader()
        .getResource("sequence" + File.separator + "frankenstein_text_text.seq").toURI());
    FileSystem fs = FileSystem.get(conf);
    Path dir = new Path(fs.getWorkingDirectory(), "build");
    Path sub = new Path(dir, "SFIMT");
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
    //get one doc just to confirm:
    LWDocument doc = run.get(0).getSecond().getLWDocument();
    assertNotNull("document is null", doc);
    assertTrue(ids.contains(doc.getId()));
  }
}
