package com.lucidworks.hadoop.ingest;

import com.lucidworks.hadoop.io.LWDocument;
import com.lucidworks.hadoop.utils.IngestJobMockMapRedOutFormat;
import com.lucidworks.hadoop.utils.MockRecordWriter;
import com.lucidworks.hadoop.utils.TestUtils;
import java.io.File;
import java.net.URL;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class IngestJobCompressionTest extends IngestJobInit {

  @Test
  public void testGzipCompressionWithCSV() throws Exception {

    String compressedFileName = "csv" + File.separator + "frank.csv.gz";
    Path input = new Path(tempDir, compressedFileName);

    // Copy compressed file to HDFS
    URL url = IngestJobCompressionTest.class.getClassLoader().getResource(compressedFileName);
    assertTrue(url != null);
    Path localPath = new Path(url.toURI());
    fs.copyFromLocalFile(localPath, input);

    String jobName = "JobCompressionTest" + System.currentTimeMillis();
    String[] args = TestUtils
        .createHadoopJobArgsWithConf(jobName, CSVIngestMapper.class.getName(), DEFAULT_COLLECTION,
            getBaseUrl(), input.toUri().toString(),
            "csvFieldMapping[0=id,1=count,2=body,3=title,4=footer]");

    int val = ToolRunner.run(conf, new IngestJob(), args);

    // Verify job
    assertEquals(0, val);
    MockRecordWriter mockRecordWriter = IngestJobMockMapRedOutFormat.writers.get(jobName);
    assertTrue(mockRecordWriter != null);
    assertEquals(79, mockRecordWriter.map.size());

    // Verify some documents
    String id = "frankenstein_csv_1";
    LWDocument doc = mockRecordWriter.map.get(id).getLWDocument();
    assertTrue((doc != null));

    // TODO: check fields

    IngestJobMockMapRedOutFormat.writers.remove(jobName);
  }

  @Test
  public void testBzip2CompressionWithCSV() throws Exception {

    String compressedFileName = "csv" + File.separator + "frank.csv.bz2";
    Path input = new Path(tempDir, compressedFileName);

    // Copy compressed file to HDFS
    URL url = IngestJobCompressionTest.class.getClassLoader().getResource(compressedFileName);
    assertTrue(url != null);
    Path localPath = new Path(url.toURI());
    fs.copyFromLocalFile(localPath, input);

    String jobName = "JobCompressionTest" + System.currentTimeMillis();
    String[] args = TestUtils
        .createHadoopJobArgsWithConf(jobName, CSVIngestMapper.class.getName(), DEFAULT_COLLECTION,
            getBaseUrl(), input.toUri().toString(),
            "csvFieldMapping[0=id,1=count,2=body,3=title,4=footer]");

    int val = ToolRunner.run(conf, new IngestJob(), args);

    // Verify job
    assertEquals(0, val);
    MockRecordWriter mockRecordWriter = IngestJobMockMapRedOutFormat.writers.get(jobName);
    assertTrue(mockRecordWriter != null);
    assertEquals(79, mockRecordWriter.map.size());

    // Verify some documents
    String id = "frankenstein_csv_1";
    LWDocument doc = mockRecordWriter.map.get(id).getLWDocument();
    assertTrue((doc != null));

    // TODO: check fields

    // Remove the results from this test
    IngestJobMockMapRedOutFormat.writers.remove(jobName);
  }

  @Test
  public void testGzipCompressionWithDirectory() throws Exception {

    Path input = new Path(tempDir, "DirectoryIngestMapper");
    fs.mkdirs(input);
    for (int i = 0; i < 6; i++) {
      String compressedFileName = "dir" + File.separator + "frank_txt_" + i + ".txt.gz";
      Path currentInput = new Path(input, compressedFileName);

      // Copy compressed file to HDFS
      URL url = IngestJobCompressionTest.class.getClassLoader().getResource(compressedFileName);
      assertTrue(url != null);
      Path localPath = new Path(url.toURI());
      fs.copyFromLocalFile(localPath, currentInput);
    }

    String jobName = "JobCompressionTest" + System.currentTimeMillis();
    String[] args = TestUtils
        .createHadoopJobArgs(jobName, DirectoryIngestMapper.class.getName(), DEFAULT_COLLECTION,
            getBaseUrl(),
            input.toUri().toString() + File.separator + "dir" + File.separator + "frank_txt*.gz");

    int val = ToolRunner.run(conf, new IngestJob(), args);

    // Verify job
    assertEquals(0, val);
    MockRecordWriter mockRecordWriter = IngestJobMockMapRedOutFormat.writers.get(jobName);
    assertTrue(mockRecordWriter != null);
    assertEquals(6, mockRecordWriter.map.size());

    // Verify document fields
    String id = "dir/frank_txt_0.txt";
    for (String key : mockRecordWriter.map.keySet()) {
      if (key.contains(id)) {
        id = key;
      }
    }
    LWDocument doc = mockRecordWriter.map.get(id).getLWDocument();
    assertTrue((doc != null));

    // TODO: check fields

    // Remove the results from this test
    IngestJobMockMapRedOutFormat.writers.remove(jobName);
  }

  @Test
  public void testBzip2CompressionWithDirectory() throws Exception {

    Path input = new Path(tempDir, "DirectoryIngestMapper");
    fs.mkdirs(input);
    for (int i = 0; i < 6; i++) {
      String compressedFileName = "dir" + File.separator + "frank_txt_" + i + ".txt.bz2";
      Path currentInput = new Path(input, compressedFileName);

      // Copy compressed file to HDFS
      URL url = IngestJobCompressionTest.class.getClassLoader().getResource(compressedFileName);
      assertTrue(url != null);
      Path localPath = new Path(url.toURI());
      fs.copyFromLocalFile(localPath, currentInput);
    }

    String jobName = "JobCompressionTest" + System.currentTimeMillis();
    String[] args = TestUtils
        .createHadoopJobArgs(jobName, DirectoryIngestMapper.class.getName(), DEFAULT_COLLECTION,
            getBaseUrl(),
            input.toUri().toString() + File.separator + "dir" + File.separator + "frank_txt*.bz2");

    int val = ToolRunner.run(conf, new IngestJob(), args);

    // Verify job
    assertEquals(0, val);
    MockRecordWriter mockRecordWriter = IngestJobMockMapRedOutFormat.writers.get(jobName);
    assertTrue(mockRecordWriter != null);
    assertEquals(6, mockRecordWriter.map.size());

    // Verify document fields
    String id = "frank_txt_0.txt";
    for (String key : mockRecordWriter.map.keySet()) {
      if (key.contains(id)) {
        id = key;
      }
    }
    LWDocument doc = mockRecordWriter.map.get(id).getLWDocument();
    assertTrue((doc != null));

    // TODO: check fields

    // Remove the results from this test
    IngestJobMockMapRedOutFormat.writers.remove(jobName);
  }

  @Ignore("LWSHADOOP-120")
  @Test
  public void testGzipCompressionWithGrok() throws Exception {

    Path input = new Path(tempDir, "GrokIngestMapper");
    fs.mkdirs(input);
    String compressedFileName = "grok" + File.separator + "ip-word-small.log.gz";
    Path currentInput = new Path(input, compressedFileName);

    // Copy compressed file to HDFS
    URL url = IngestJobCompressionTest.class.getClassLoader().getResource(compressedFileName);
    assertTrue(url != null);
    Path localPath = new Path(url.toURI());
    fs.copyFromLocalFile(localPath, currentInput);

    String grokUri = "grok" + File.separator + "IP-WORD.conf";
    Path grokUriPath = new Path(input, grokUri);

    String jobName = "JobCompressionTest" + System.currentTimeMillis();
    String[] args = TestUtils
        .createHadoopOptionalArgs(jobName, GrokIngestMapper.class.getName(), DEFAULT_COLLECTION,
            getBaseUrl(), input.toUri().toString() + File.separator + compressedFileName,
            "-Dgrok.uri=" + grokUriPath);

    int val = ToolRunner.run(conf, new IngestJob(), args);

    // Verify job
    assertEquals(0, val);
    MockRecordWriter mockRecordWriter = IngestJobMockMapRedOutFormat.writers.get(jobName);
    assertTrue(mockRecordWriter != null);
    assertEquals(1, mockRecordWriter.map.size());

    // Verify document fields
    String id = mockRecordWriter.map.keySet().iterator().next();

    LWDocument doc = mockRecordWriter.map.get(id).getLWDocument();
    assertTrue((doc != null));

    // TODO: check fields

    // Remove the results from this test
    IngestJobMockMapRedOutFormat.writers.remove(jobName);
  }

  @Ignore("LWSHADOOP-120")
  @Test
  public void testBzip2CompressionWithGrok() throws Exception {

    Path input = new Path(tempDir, "GrokIngestMapper");
    fs.mkdirs(input);
    String compressedFileName = "grok" + File.separator + "ip-word-small.log.bz2";
    Path currentInput = new Path(input, compressedFileName);

    // Copy compressed file to HDFS
    URL url = IngestJobCompressionTest.class.getClassLoader().getResource(compressedFileName);
    assertTrue(url != null);
    Path localPath = new Path(url.toURI());
    fs.copyFromLocalFile(localPath, currentInput);

    // Copy LogStash configuartion
    url = IngestJobCompressionTest.class.getClassLoader()
        .getResource("grok" + File.separator + "IP-WORD.conf");
    assertTrue(url != null);
    localPath = new Path(url.toURI());
    Path logStashConfigurationDst = new Path(input, "IP-WORD-HDFS.conf");
    fs.copyFromLocalFile(localPath, logStashConfigurationDst);

    String jobName = "JobCompressionTest" + System.currentTimeMillis();
    String[] args = TestUtils
        .createHadoopOptionalArgs(jobName, GrokIngestMapper.class.getName(), DEFAULT_COLLECTION,
            getBaseUrl(), input.toUri().toString() + File.separator + compressedFileName,
            "-Dgrok.uri=" + logStashConfigurationDst.toString());

    int val = ToolRunner.run(conf, new IngestJob(), args);

    // Verify job
    assertEquals(0, val);
    MockRecordWriter mockRecordWriter = IngestJobMockMapRedOutFormat.writers.get(jobName);
    assertTrue(mockRecordWriter != null);
    assertEquals(1, mockRecordWriter.map.size());

    // Verify document fields
    String id = mockRecordWriter.map.keySet().iterator().next();

    LWDocument doc = mockRecordWriter.map.get(id).getLWDocument();
    assertTrue((doc != null));

    // TODO: check fields

    // Remove the results from this test
    IngestJobMockMapRedOutFormat.writers.remove(jobName);
  }

  @Test
  public void testGzipCompressionWithRegex() throws Exception {

    Path input = new Path(tempDir, "RegexIngestMapper");
    fs.mkdirs(input);
    String compressedFileName = "regex" + File.separator + "regex-small.txt.gz";
    Path currentInput = new Path(input, compressedFileName);

    // Copy compressed file to HDFS
    URL url = IngestJobCompressionTest.class.getClassLoader().getResource(compressedFileName);
    assertTrue(url != null);
    Path localPath = new Path(url.toURI());
    fs.copyFromLocalFile(localPath, currentInput);

    String jobName = "JobCompressionTest" + System.currentTimeMillis();
    String[] args = TestUtils
        .createHadoopOptionalArgs(jobName, RegexIngestMapper.class.getName(), DEFAULT_COLLECTION,
            getBaseUrl(), input.toUri().toString() + File.separator + compressedFileName,
            "-D" + RegexIngestMapper.REGEX + "=\\w+",
            "-D" + RegexIngestMapper.GROUPS_TO_FIELDS + "=0=matchFound");

    int val = ToolRunner.run(conf, new IngestJob(), args);

    // Verify job
    assertEquals(0, val);
    MockRecordWriter mockRecordWriter = IngestJobMockMapRedOutFormat.writers.get(jobName);
    assertTrue(mockRecordWriter != null);
    assertEquals(1, mockRecordWriter.map.size());

    // Verify document fields
    String id = mockRecordWriter.map.keySet().iterator().next();

    LWDocument doc = mockRecordWriter.map.get(id).getLWDocument();
    assertTrue((doc != null));

    // TODO: check fields

    // Remove the results from this test
    IngestJobMockMapRedOutFormat.writers.remove(jobName);
  }

  @Test
  public void testBzip2CompressionWithRegex() throws Exception {

    Path input = new Path(tempDir, "RegexIngestMapper");
    fs.mkdirs(input);
    String compressedFileName = "regex" + File.separator + "regex-small.txt.bz2";
    Path currentInput = new Path(input, compressedFileName);

    // Copy compressed file to HDFS
    URL url = IngestJobCompressionTest.class.getClassLoader().getResource(compressedFileName);
    assertTrue(url != null);
    Path localPath = new Path(url.toURI());
    fs.copyFromLocalFile(localPath, currentInput);

    String jobName = "JobCompressionTest" + System.currentTimeMillis();
    String[] args = TestUtils
        .createHadoopOptionalArgs(jobName, RegexIngestMapper.class.getName(), DEFAULT_COLLECTION,
            getBaseUrl(), input.toUri().toString() + File.separator + compressedFileName,
            "-D" + RegexIngestMapper.REGEX + "=\\w+",
            "-D" + RegexIngestMapper.GROUPS_TO_FIELDS + "=0=matchFound");

    int val = ToolRunner.run(conf, new IngestJob(), args);

    // Verify job
    assertEquals(0, val);
    MockRecordWriter mockRecordWriter = IngestJobMockMapRedOutFormat.writers.get(jobName);
    assertTrue(mockRecordWriter != null);
    assertEquals(1, mockRecordWriter.map.size());

    // Verify document fields
    String id = mockRecordWriter.map.keySet().iterator().next();

    LWDocument doc = mockRecordWriter.map.get(id).getLWDocument();
    assertTrue((doc != null));

    // TODO: check fields

    // Remove the results from this test
    IngestJobMockMapRedOutFormat.writers.remove(jobName);
  }

  @Test
  public void testGzipCompressionWithSequenceFile() throws Exception {

    Path input = new Path(tempDir, "SequenceFileIngestMapper");
    fs.mkdirs(input);
    String compressedFileName =
        "sequence" + File.separator + "frankestein_text_text_gzip_compressed.seq";
    Path currentInput = new Path(input, compressedFileName);

    // Copy compressed file to HDFS
    URL url = IngestJobCompressionTest.class.getClassLoader().getResource(compressedFileName);
    assertTrue(url != null);
    Path localPath = new Path(url.toURI());
    fs.copyFromLocalFile(localPath, currentInput);

    String jobName = "JobCompressionTest" + System.currentTimeMillis();
    String[] args = TestUtils
        .createHadoopJobArgs(jobName, SequenceFileIngestMapper.class.getName(), DEFAULT_COLLECTION,
            getBaseUrl(), input.toUri().toString() + File.separator + compressedFileName);

    int val = ToolRunner.run(conf, new IngestJob(), args);

    // Verify job
    assertEquals(0, val);
    MockRecordWriter mockRecordWriter = IngestJobMockMapRedOutFormat.writers.get(jobName);
    assertTrue(mockRecordWriter != null);
    assertEquals(776, mockRecordWriter.map.size());

    // Verify document fields
    String id = mockRecordWriter.map.keySet().iterator().next();

    LWDocument doc = mockRecordWriter.map.get(id).getLWDocument();
    assertTrue((doc != null));

    Assert.assertTrue(doc.getId().contains("frank_seq_"));

    // TODO: check fields

    // Remove the results from this test
    IngestJobMockMapRedOutFormat.writers.remove(jobName);
  }

  @Test
  public void testBzip2CompressionWithSequenceFile() throws Exception {

    Path input = new Path(tempDir, "SequenceFileIngestMapper");
    fs.mkdirs(input);
    String compressedFileName = "sequence" + File.separator + "frankestein_text_text_bzip2_compressed.seq";
    Path currentInput = new Path(input, compressedFileName);

    // Copy compressed file to HDFS
    URL url = IngestJobCompressionTest.class.getClassLoader().getResource(compressedFileName);
    assertTrue(url != null);
    Path localPath = new Path(url.toURI());
    fs.copyFromLocalFile(localPath, currentInput);

    String jobName = "JobCompressionTest" + System.currentTimeMillis();
    String[] args = TestUtils
        .createHadoopJobArgs(jobName, SequenceFileIngestMapper.class.getName(), DEFAULT_COLLECTION,
            getBaseUrl(), input.toUri().toString() + File.separator + compressedFileName);

    int val = ToolRunner.run(conf, new IngestJob(), args);

    // Verify job
    assertEquals(0, val);
    MockRecordWriter mockRecordWriter = IngestJobMockMapRedOutFormat.writers.get(jobName);
    assertTrue(mockRecordWriter != null);
    assertEquals(776, mockRecordWriter.map.size());

    // Verify document fields
    String id = mockRecordWriter.map.keySet().iterator().next();

    LWDocument doc = mockRecordWriter.map.get(id).getLWDocument();
    assertTrue((doc != null));

    Assert.assertTrue(doc.getId().contains("frank_seq_"));

    // TODO: check fields

    // Remove the results from this test
    IngestJobMockMapRedOutFormat.writers.remove(jobName);
  }

  @Test
  public void testGzipCompressionWithSolrXML() throws Exception {

    Path input = new Path(tempDir, "SolrXMLIngestMapper");
    fs.mkdirs(input);
    String compressedFileName = "sequence" + File.separator + "frankestein_text_solr_gzip_compressed.seq";
    Path currentInput = new Path(input, compressedFileName);

    // Copy compressed file to HDFS
    URL url = IngestJobCompressionTest.class.getClassLoader().getResource(compressedFileName);
    assertTrue(url != null);
    Path localPath = new Path(url.toURI());
    fs.copyFromLocalFile(localPath, currentInput);

    String jobName = "JobCompressionTest" + System.currentTimeMillis();
    String[] args = TestUtils
        .createHadoopJobArgs(jobName, SolrXMLIngestMapper.class.getName(), DEFAULT_COLLECTION,
            getBaseUrl(), input.toUri().toString() + File.separator + compressedFileName);

    int val = ToolRunner.run(conf, new IngestJob(), args);

    // Verify job
    assertEquals(0, val);
    MockRecordWriter mockRecordWriter = IngestJobMockMapRedOutFormat.writers.get(jobName);
    assertTrue(mockRecordWriter != null);
    assertEquals(776, mockRecordWriter.map.size());

    // Verify document fields
    String id = mockRecordWriter.map.keySet().iterator().next();

    LWDocument doc = mockRecordWriter.map.get(id).getLWDocument();
    assertTrue((doc != null));

    Assert.assertTrue(doc.getId().contains("solr_"));

    // TODO: check fields

    // Remove the results from this test
    IngestJobMockMapRedOutFormat.writers.remove(jobName);
  }

  @Test
  public void testBzip2CompressionWithSolrXML() throws Exception {

    Path input = new Path(tempDir, "SolrXMLIngestMapper");
    fs.mkdirs(input);
    String compressedFileName = "sequence" + File.separator + "frankestein_text_solr_bzip2_compressed.seq";
    Path currentInput = new Path(input, compressedFileName);

    // Copy compressed file to HDFS
    URL url = IngestJobCompressionTest.class.getClassLoader().getResource(compressedFileName);
    assertTrue(url != null);
    Path localPath = new Path(url.toURI());
    fs.copyFromLocalFile(localPath, currentInput);

    String jobName = "JobCompressionTest" + System.currentTimeMillis();
    String[] args = TestUtils
        .createHadoopJobArgs(jobName, SolrXMLIngestMapper.class.getName(), DEFAULT_COLLECTION,
            getBaseUrl(), input.toUri().toString() + File.separator + compressedFileName);

    int val = ToolRunner.run(conf, new IngestJob(), args);

    // Verify job
    assertEquals(0, val);
    MockRecordWriter mockRecordWriter = IngestJobMockMapRedOutFormat.writers.get(jobName);
    assertTrue(mockRecordWriter != null);
    assertEquals(776, mockRecordWriter.map.size());

    // Verify document fields
    String id = mockRecordWriter.map.keySet().iterator().next();

    LWDocument doc = mockRecordWriter.map.get(id).getLWDocument();
    assertTrue((doc != null));

    Assert.assertTrue(doc.getId().contains("solr_"));

    // TODO: check fields

    // Remove the results from this test
    IngestJobMockMapRedOutFormat.writers.remove(jobName);
  }

  private static final String WARC_FIELD = "warc.";

  @Ignore
  @Test
  public void testGzipCompressionWithWarc() throws Exception {

    Path input = new Path(tempDir, "WarcIngestMapper");
    fs.mkdirs(input);
    String compressedFileName = "warc/at.warc.gz";
    Path currentInput = new Path(input, compressedFileName);

    // Copy compressed file to HDFS
    URL url = IngestJobCompressionTest.class.getClassLoader().getResource(compressedFileName);
    assertTrue(url != null);
    Path localPath = new Path(url.toURI());
    fs.copyFromLocalFile(localPath, currentInput);

    String jobName = "JobCompressionTest" + System.currentTimeMillis();
    String[] args = TestUtils
        .createHadoopJobArgs(jobName, WarcIngestMapper.class.getName(), DEFAULT_COLLECTION,
            getBaseUrl(), input.toUri().toString() + File.separator + compressedFileName);

    int val = ToolRunner.run(conf, new IngestJob(), args);

    // Verify job
    assertEquals(0, val);
    MockRecordWriter mockRecordWriter = IngestJobMockMapRedOutFormat.writers.get(jobName);
    assertTrue(mockRecordWriter != null);
    assertEquals(5, mockRecordWriter.map.size());

    // Verify document fields
    String id = "<urn:uuid:00fee1bb-1abc-45a6-af31-a164c2fdad88>";

    LWDocument doc = mockRecordWriter.map.get(id).getLWDocument();
    assertTrue((doc != null));

    // TODO: check fields

    // Remove the results from this test
    IngestJobMockMapRedOutFormat.writers.remove(jobName);
  }

  @Ignore
  @Test
  public void testBzip2CompressionWithWarc() throws Exception {

    Path input = new Path(tempDir, "WarcIngestMapper");
    fs.mkdirs(input);
    String compressedFileName = "warc/at.warc.bz2";
    Path currentInput = new Path(input, compressedFileName);

    // Copy compressed file to HDFS
    URL url = IngestJobCompressionTest.class.getClassLoader().getResource(compressedFileName);
    assertTrue(url != null);
    Path localPath = new Path(url.toURI());
    fs.copyFromLocalFile(localPath, currentInput);

    String jobName = "JobCompressionTest" + System.currentTimeMillis();
    String[] args = TestUtils
        .createHadoopJobArgs(jobName, WarcIngestMapper.class.getName(), DEFAULT_COLLECTION,
            getBaseUrl(), input.toUri().toString() + File.separator + compressedFileName);

    int val = ToolRunner.run(conf, new IngestJob(), args);

    // Verify job
    assertEquals(0, val);
    MockRecordWriter mockRecordWriter = IngestJobMockMapRedOutFormat.writers.get(jobName);
    assertTrue(mockRecordWriter != null);
    assertEquals(5, mockRecordWriter.map.size());

    // Verify document fields
    String id = "<urn:uuid:00fee1bb-1abc-45a6-af31-a164c2fdad88>";

    LWDocument doc = mockRecordWriter.map.get(id).getLWDocument();
    assertTrue((doc != null));

    // TODO: check fields

    // Remove the results from this test
    IngestJobMockMapRedOutFormat.writers.remove(jobName);
  }

  @Test
  public void testGzipCompressionWithZip() throws Exception {

    Path input = new Path(tempDir, "ZipIngestMapper");
    fs.mkdirs(input);
    String compressedFileName = "zip/zipData.zip.gz";
    Path currentInput = new Path(input, compressedFileName);

    // Copy compressed file to HDFS
    URL url = IngestJobCompressionTest.class.getClassLoader().getResource(compressedFileName);
    assertTrue(url != null);
    Path localPath = new Path(url.toURI());
    fs.copyFromLocalFile(localPath, currentInput);

    String jobName = "JobCompressionTest" + System.currentTimeMillis();
    String[] args = TestUtils
        .createHadoopJobArgs(jobName, ZipIngestMapper.class.getName(), DEFAULT_COLLECTION,
            getBaseUrl(), input.toUri().toString() + File.separator + compressedFileName);

    int val = ToolRunner.run(conf, new IngestJob(), args);

    // Verify job
    assertEquals(0, val);
    MockRecordWriter mockRecordWriter = IngestJobMockMapRedOutFormat.writers.get(jobName);
    assertTrue(mockRecordWriter != null);
    assertEquals(6, mockRecordWriter.map.size());

    // Verify document fields
    String id = "test1.doc";

    LWDocument doc = mockRecordWriter.map.get(id).getLWDocument();
    assertTrue((doc != null));

    // TODO: check fields

    // Remove the results from this test
    IngestJobMockMapRedOutFormat.writers.remove(jobName);
  }

  @Test
  public void testBzip2CompressionWithZip() throws Exception {

    Path input = new Path(tempDir, "ZipIngestMapper");
    fs.mkdirs(input);
    String compressedFileName = "zip/zipData.zip.bz2";
    Path currentInput = new Path(input, compressedFileName);

    // Copy compressed file to HDFS
    URL url = IngestJobCompressionTest.class.getClassLoader().getResource(compressedFileName);
    assertTrue(url != null);
    Path localPath = new Path(url.toURI());
    fs.copyFromLocalFile(localPath, currentInput);

    String jobName = "JobCompressionTest" + System.currentTimeMillis();
    String[] args = TestUtils
        .createHadoopJobArgs(jobName, ZipIngestMapper.class.getName(), DEFAULT_COLLECTION,
            getBaseUrl(), input.toUri().toString() + File.separator + compressedFileName);

    int val = ToolRunner.run(conf, new IngestJob(), args);

    // Verify job
    assertEquals(0, val);
    MockRecordWriter mockRecordWriter = IngestJobMockMapRedOutFormat.writers.get(jobName);
    assertTrue(mockRecordWriter != null);
    assertEquals(6, mockRecordWriter.map.size());

    // Verify document fields
    String id = "test1.doc";

    LWDocument doc = mockRecordWriter.map.get(id).getLWDocument();
    assertTrue((doc != null));

    // TODO: check fields

    // Remove the results from this test
    IngestJobMockMapRedOutFormat.writers.remove(jobName);
  }
}
