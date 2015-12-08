package com.lucidworks.hadoop.ingest;

import com.lucidworks.hadoop.io.LWDocument;
import com.lucidworks.hadoop.io.LWDocumentProvider;
import com.lucidworks.hadoop.io.LWDocumentWritable;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mrunit.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.lucidworks.hadoop.utils.ConfigurationKeys.TEMP_DIR;
import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 *
 *
 **/
public class DirectoryIngestMapperTest extends BaseIngestMapperTestCase {
  MapDriver<Text, NullWritable, Text, LWDocumentWritable> mapDriver;
  private transient static Logger log = LoggerFactory.getLogger(DirectoryIngestMapperTest.class);
  private DirectoryIngestMapper mapper;
  private JobConf jobConf;
  private int tempFiles;

  @Before
  public void setUp() throws IOException, URISyntaxException {
    mapper = new DirectoryIngestMapper();
    mapDriver = new MapDriver<Text, NullWritable, Text, LWDocumentWritable>();
    mapDriver.setMapper(mapper);
    mapDriver.setConfiguration(createConf());

    Configuration conf = mapDriver.getConfiguration();
    setupCommonConf(conf);
    FileSystem fs = FileSystem.getLocal(conf);
    Path dir = new Path(fs.getWorkingDirectory(), "build");
    Path sub = new Path(dir, "DIMT");
    Path tempDir = new Path(sub, "tmp-dir");
    Path seqDir = new Path(sub, "seq-dir");// this is the location where the
    // fixture will write inputs.seq
    fs.mkdirs(tempDir);
    tempFiles = setupDir(fs, tempDir);
    conf.set(TEMP_DIR, seqDir.toString());
    jobConf = new JobConf(conf);
    org.apache.hadoop.mapred.FileInputFormat.setInputPaths(jobConf, new Path(tempDir, "*"));
    Path[] paths = org.apache.hadoop.mapred.FileInputFormat.getInputPaths(jobConf);
    assertEquals(1, paths.length);
    LWDocumentProvider.init(jobConf);

  }

  @Test
  public void testDir() throws Exception {
    mapper.getFixture().init(jobConf);
    Configuration conf = mapDriver.getConfiguration();
    FileSystem fs = FileSystem.getLocal(conf);
    Path[] paths = org.apache.hadoop.mapred.FileInputFormat.getInputPaths(jobConf);
    int i = 0;
    SequenceFile.Reader reader = new SequenceFile.Reader(fs, paths[0], conf);
    Text text = new Text();
    Set<String> ids = new HashSet<String>();
    while (reader.next(text, NullWritable.get())) {
      log.info("Path: " + text);
      ids.add(text.toString());
      mapDriver.withInput(text, NullWritable.get());
      i++;
    }
    mapDriver.withCounter(BaseHadoopIngest.Counters.DOCS_ADDED, i);
    List<Pair<Text, LWDocumentWritable>> run = mapDriver.run();
    assertEquals(i, tempFiles - 4); // -4 because the 4 files added as
    // subdirectories in SetupDir
    assertEquals(i, run.size());
    // get one doc just to confirm:
    for (Pair<Text, LWDocumentWritable> pair : run) {
      LWDocument doc = pair.getSecond().getLWDocument();
      assertNotNull("document is null", doc);
      Assert.assertNotNull(doc.getId());
      assertTrue(ids.contains(doc.getId()));

    }

    /*
     * Document foo = table.getDocument(dir + "/test1.doc", "foo",
     * DocumentTable.DocumentFields.TEXT | DocumentTable.DocumentFields.FIELD);
     * assertNotNull("document is null", foo);
     */
  }

  @Test
  public void testSubDir() throws Exception {
    Configuration conf = mapDriver.getConfiguration();
    conf.set(DirectoryIngestMapper.DIRECTORY_ADD_SUBDIRECTORIES, "true");
    FileSystem fs = FileSystem.getLocal(conf);
    Path dir = new Path(fs.getWorkingDirectory(), "build");
    Path sub = new Path(dir, "DIMT");
    Path tempDir = new Path(sub, "tmp-dir");
    jobConf = new JobConf(conf);
    org.apache.hadoop.mapred.FileInputFormat.setInputPaths(jobConf, new Path(tempDir, "*"));
    mapper.getFixture().init(jobConf);

    Path[] paths = org.apache.hadoop.mapred.FileInputFormat.getInputPaths(jobConf);
    int i = 0;
    SequenceFile.Reader reader = new SequenceFile.Reader(fs, paths[0], conf);
    Text text = new Text();
    Set<String> ids = new HashSet<String>();
    while (reader.next(text, NullWritable.get())) {
      log.info("Path: " + text);
      ids.add(text.toString());
      mapDriver.withInput(text, NullWritable.get());
      i++;
    }
    mapDriver.withCounter(BaseHadoopIngest.Counters.DOCS_ADDED, i);
    List<Pair<Text, LWDocumentWritable>> run = mapDriver.run();
    assertEquals(i, tempFiles);
    assertEquals(i, run.size());
    // get one doc just to confirm:
    for (Pair<Text, LWDocumentWritable> pair : run) {
      LWDocument doc = pair.getSecond().getLWDocument();
      assertNotNull("document is null", doc);
      Assert.assertNotNull(doc.getId());
      assertTrue(ids.contains(doc.getId()));
    }

  }

  private int setupDir(FileSystem fs, Path base) throws URISyntaxException, IOException {
    fs.mkdirs(base);
    int count = 0;
    List<Path> paths = new ArrayList<Path>();
    for (int i = 0; i < 6; i++, count++) {
      Path path = new Path(DirectoryIngestMapperTest.class.getClassLoader()
          .getResource("dir" + File.separator + "frank_txt_" + i + ".txt").toURI());
      Path dst = new Path(base, "frank_txt_" + i + ".txt");
      paths.add(dst);
      fs.copyFromLocalFile(path, dst);
    }
    for (int i = 0; i < 3; i++, count++) {
      Path path = new Path(DirectoryIngestMapperTest.class.getClassLoader()
          .getResource("dir" + File.separator + "test" + i + ".pdf").toURI());
      Path dst = new Path(base, "test" + i + ".pdf");
      paths.add(dst);
      fs.copyFromLocalFile(path, dst);
    }
    for (int i = 0; i < 2; i++, count++) {
      Path path = new Path(DirectoryIngestMapperTest.class.getClassLoader()
          .getResource("dir" + File.separator + "test" + i + ".doc").toURI());
      Path dst = new Path(base, "test" + i + ".doc");
      paths.add(dst);
      fs.copyFromLocalFile(path, dst);
    }
    Path subDirA = new Path(base, "subDirA");
    for (int i = 0; i < 2; i++, count++) {// Subdirectories A
      Path path = new Path(DirectoryIngestMapperTest.class.getClassLoader()
          .getResource("dir" + File.separator + "test" + i + ".doc").toURI());
      Path dst = new Path(subDirA, "test" + i + ".doc");
      paths.add(dst);
      fs.copyFromLocalFile(path, dst);
    }
    Path subDirB = new Path(base, "subDirB");
    for (int i = 0; i < 2; i++, count++) {// Subdirectories B
      Path path = new Path(DirectoryIngestMapperTest.class.getClassLoader()
          .getResource("dir" + File.separator + "test" + i + ".doc").toURI());
      Path dst = new Path(subDirB, "test" + i + ".pdf");
      paths.add(dst);
      fs.copyFromLocalFile(path, dst);
    }
    for (Path path : paths) {
      FileStatus fileStatus = fs.getFileStatus(path);
      System.out.println("Status: " + fileStatus.getPath());
    }
    return count;
  }

}
