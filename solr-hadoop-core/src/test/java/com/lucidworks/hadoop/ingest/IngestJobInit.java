package com.lucidworks.hadoop.ingest;

import com.lucidworks.hadoop.io.LWDocumentWritable;
import com.lucidworks.hadoop.utils.IngestJobMockMapRedOutFormat;
import com.lucidworks.hadoop.utils.MockRecordWriter;
import com.lucidworks.hadoop.utils.SolrCloudClusterSupport;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TestName;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class IngestJobInit extends SolrCloudClusterSupport {

  static final Logger log = Logger.getLogger(IngestJobInit.class);

  protected static Path tempDir;
  protected static Configuration conf;
  protected static FileSystem fs;

  protected static ByteArrayOutputStream outStream;
  protected static PrintStream outPrint;

  protected static String lineSep = System.getProperty("line.separator");

  @Rule
  public TestName testName = new TestName();

  @BeforeClass
  public static void initTest() throws Exception {
    conf = new Configuration();
    fs = FileSystem.get(conf);

    Path dir = new Path(fs.getWorkingDirectory(), "build");
    Path sub = new Path(dir, "IJT");
    tempDir = new Path(sub, "tmp-dir");
    fs.mkdirs(tempDir);
    // Set the system out for test
    outStream = new ByteArrayOutputStream();
    outPrint = new PrintStream(outStream, true);
    System.setOut(outPrint);
  }

  @Before
  public void setUp() throws Exception {
    removeAllDocs();
    clearOutput();
  }

  @AfterClass
  public static void tearDownTest() throws Exception {
    System.setOut(System.out);
    // because of junit.framework.AssertionFailedError: Clean up static fields
    // (in @AfterClass?):
    tempDir = null;
    conf = null;
    fs = null;
    outStream = null;
    outPrint = null;
  }

  @After
  public void printOut() throws Exception {
    // prints the outStream after each test
    System.err.println("Info: Testmethod[" + testName.getMethodName() + "] output[" + outStream + "]");
  }

  public static void clearOutput() {
    // needs to clear the output after every test
    outPrint.flush();
    outStream.reset();
  }

  public static void assertErrorMessage(String error) {
    // Assert that the error is the expected one.
    // XXX trim() ??
    String output = outStream.toString().replaceAll("\n", "").trim();
    if (!output.matches(".*" + error.trim() + ".*")) {
      fail("The error [" + error + "] not match with [" + outStream.toString() + "]");
    }
    clearOutput();
  }

  protected static void addContentToFS(Path input, byte[] content) throws IOException {
    FSDataOutputStream fsDataOutputStream = fs.create(input);
    fsDataOutputStream.write(content);
    IOUtils.closeQuietly(fsDataOutputStream);
  }

  protected static void addContentToFS(Path input, String... content) throws IOException {
    FSDataOutputStream fsDataOutputStream = fs.create(input);
    for (int i = 0; i < content.length; i++) {
      fsDataOutputStream.writeUTF(content[i]);
    }
    IOUtils.closeQuietly(fsDataOutputStream);
  }

  protected static void verifyJob(String jobName, int counter, String[] ids, String... id1Fields) {
    MockRecordWriter writer = IngestJobMockMapRedOutFormat.writers.get(jobName);
    assertNotNull(writer);

    assertEquals(counter, writer.map.size());

    if (ids == null) {
      return;
    }

    // Get the first ID
    LWDocumentWritable doc1 = writer.map.get(ids[0]);

    // Check related document related to doc1
    Assert.assertNotNull("No doc with " + ids[0], doc1);
    for (String field : id1Fields) {
      assertNotNull("No field " + field, doc1.getLWDocument().getFirstFieldValue(field));
    }

    for (Map.Entry<String, LWDocumentWritable> entry : writer.map.entrySet()) {
      if (log.isDebugEnabled()) {
        log.debug(entry.getKey() + "/" + entry.getValue());
      }
    }

    // Check ids array
    for (String id : ids) {
      Assert.assertNotNull("No doc with " + id, writer.map.get(id));
    }
  }

  protected static class TestIngestJob extends IngestJob {
    @Override
    public void processConf(String confAdds, JobConf conf) throws Exception {
      super.processConf(confAdds, conf);
    }
  }

}
