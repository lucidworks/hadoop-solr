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

    String compressedFileName = "frank.csv.gz";
    Path input = new Path(tempDir, compressedFileName);

    // Copy compressed file to HDFS
    URL url = IngestJobCompressionTest.class.getClassLoader().getResource(compressedFileName);
    assertTrue(url != null);
    Path localPath = new Path(url.toURI());
    fs.copyFromLocalFile(localPath, input);

    String jobName = "JobCompressionTest" + System.currentTimeMillis();
    String[] args = TestUtils
        .createCSVArgs(jobName, DEFAULT_COLLECTION, getBaseUrl(), input.toUri().toString(),
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

    /* TODO
    // Count field
    List<PipelineField> pipeline = doc.getFields("count");
    Assert.assertEquals(1, pipeline.size());
    Assert.assertEquals("1", pipeline.get(0).getValue());

    // Body field
    pipeline = doc.getFields("body");
    Assert.assertEquals(1, pipeline.size());
    Assert.assertEquals(
            "Frankenstein or the Modern Prometheus by Mary Wollstonecraft (Godwin) Shelley Letter 1 St. Petersburgh Dec. 11th 17--",
            pipeline.get(0).getValue());

    pipeline = doc.getFields("title");
    Assert.assertEquals(1, pipeline.size());
    Assert.assertEquals("Frankenstein or the Modern Prometheus by Mary Woll",
            pipeline.get(0).getValue());

    pipeline = doc.getFields("footer");
    Assert.assertEquals(1, pipeline.size());
    Assert.assertEquals(
            "lstonecraft (Godwin) Shelley Letter 1 St. Petersburgh Dec. 11th 17--",
            pipeline.get(0).getValue()); */

    // Remove the results from this test
    IngestJobMockMapRedOutFormat.writers.remove(jobName);
  }

  @Test
  public void testBzip2CompressionWithCSV() throws Exception {

    String compressedFileName = "frank.csv.bz2";
    Path input = new Path(tempDir, compressedFileName);

    // Copy compressed file to HDFS
    URL url = IngestJobCompressionTest.class.getClassLoader().getResource(compressedFileName);
    assertTrue(url != null);
    Path localPath = new Path(url.toURI());
    fs.copyFromLocalFile(localPath, input);

    String jobName = "JobCompressionTest" + System.currentTimeMillis();
    String[] args = TestUtils
        .createCSVArgs(jobName, DEFAULT_COLLECTION, getBaseUrl(), input.toUri().toString(),
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

    /*
    // Count field
    List<PipelineField> pipeline = doc.getFields("count");
    Assert.assertEquals(1, pipeline.size());
    Assert.assertEquals("1", pipeline.get(0).getValue());

    // Body field
    pipeline = doc.getFields("body");
    Assert.assertEquals(1, pipeline.size());
    Assert.assertEquals(
            "Frankenstein or the Modern Prometheus by Mary Wollstonecraft (Godwin) Shelley Letter 1 St. Petersburgh Dec. 11th 17--",
            pipeline.get(0).getValue());

    pipeline = doc.getFields("title");
    Assert.assertEquals(1, pipeline.size());
    Assert.assertEquals("Frankenstein or the Modern Prometheus by Mary Woll",
            pipeline.get(0).getValue());

    pipeline = doc.getFields("footer");
    Assert.assertEquals(1, pipeline.size());
    Assert.assertEquals(
            "lstonecraft (Godwin) Shelley Letter 1 St. Petersburgh Dec. 11th 17--",
            pipeline.get(0).getValue());
*/
    // Remove the results from this test
    IngestJobMockMapRedOutFormat.writers.remove(jobName);
  }

  @Test
  public void testGzipCompressionWithDirectory() throws Exception {

    Path input = new Path(tempDir, "DirectoryIngestMapper");
    fs.mkdirs(input);
    for (int i = 0; i < 6; i++) {
      String compressedFileName = "frank_txt_" + i + ".txt.gz";
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
            getBaseUrl(), input.toUri().toString() + "/frank_txt*.gz");

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
/*
    // Body field
    List<PipelineField> pipeline = doc.getFields("body");
    Assert.assertEquals(1, pipeline.size());
    Assert.assertEquals(
            "Such were the professor's words--rather let me say such the words of the fate--enounced to destroy me. As he went on I felt as if my soul were grappling with a palpable enemy; one by one the various keys were touched which formed the mechanism of my being; chord after chord was sounded and soon my mind was filled with one thought one conception one purpose. So much has been done exclaimed the soul of Frankenstein--more far more will I achieve; treading in the steps already marked I will pioneer a new way explore unknown powers and unfold to the world the deepest mysteries of creation. \n",
            pipeline.get(0).getValue());

    pipeline = doc.getFields("Content-Type");
    Assert.assertEquals(1, pipeline.size());
    Assert.assertEquals("text/plain; charset=ISO-8859-1",
            pipeline.get(0).getValue());
*/
    // Remove the results from this test
    IngestJobMockMapRedOutFormat.writers.remove(jobName);
  }

  @Test
  public void testBzip2CompressionWithDirectory() throws Exception {

    Path input = new Path(tempDir, "DirectoryIngestMapper");
    fs.mkdirs(input);
    for (int i = 0; i < 6; i++) {
      String compressedFileName = "frank_txt_" + i + ".txt.bz2";
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
            getBaseUrl(), input.toUri().toString() + "/frank_txt*.bz2");

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
/*
    // Body field
    List<PipelineField> pipeline = doc.getFields("body");
    Assert.assertEquals(1, pipeline.size());
    Assert.assertEquals(
            "Such were the professor's words--rather let me say such the words of the fate--enounced to destroy me. As he went on I felt as if my soul were grappling with a palpable enemy; one by one the various keys were touched which formed the mechanism of my being; chord after chord was sounded and soon my mind was filled with one thought one conception one purpose. So much has been done exclaimed the soul of Frankenstein--more far more will I achieve; treading in the steps already marked I will pioneer a new way explore unknown powers and unfold to the world the deepest mysteries of creation. \n",
            pipeline.get(0).getValue());

    pipeline = doc.getFields("Content-Type");
    Assert.assertEquals(1, pipeline.size());
    Assert.assertEquals("text/plain; charset=ISO-8859-1",
            pipeline.get(0).getValue());*/

    // Remove the results from this test
    IngestJobMockMapRedOutFormat.writers.remove(jobName);
  }

  @Ignore("LWSHADOOP-120")
  @Test
  public void testGzipCompressionWithGrok() throws Exception {

    Path input = new Path(tempDir, "GrokIngestMapper");
    fs.mkdirs(input);
    String compressedFileName = "ip-word-small.log.gz";
    Path currentInput = new Path(input, compressedFileName);

    // Copy compressed file to HDFS
    URL url = IngestJobCompressionTest.class.getClassLoader().getResource(compressedFileName);
    assertTrue(url != null);
    Path localPath = new Path(url.toURI());
    fs.copyFromLocalFile(localPath, currentInput);

    String jobName = "JobCompressionTest" + System.currentTimeMillis();
    String[] args = TestUtils
        .createHadoopOptionalArgs(jobName, GrokIngestMapper.class.getName(), DEFAULT_COLLECTION,
            getBaseUrl(), input.toUri().toString() + File.separator + compressedFileName,
            "-Dgrok.uri=file://" + resourceDir + File.separator + "IP-WORD.conf");

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
/*
    List<PipelineField> pipeline = doc.getFields("message");
    Assert.assertEquals(1, pipeline.size());
    Assert.assertEquals("112.37.117.33 WORD__204623207",
            pipeline.get(0).getValue());

    pipeline = doc.getFields("received_from_field");
    Assert.assertEquals(1, pipeline.size());
    Assert.assertEquals("112.37.117.33", pipeline.get(0).getValue());

    pipeline = doc.getFields("message_field");
    Assert.assertEquals(1, pipeline.size());
    Assert.assertEquals("WORD__204623207", pipeline.get(0).getValue());

    pipeline = doc.getFields("path");
    Assert.assertEquals(1, pipeline.size());
    Assert.assertEquals(currentInput.toUri().getPath(),
            pipeline.get(0).getValue());

    pipeline = doc.getFields("log_message");
    Assert.assertEquals(1, pipeline.size());
    Assert.assertEquals("WORD__204623207", pipeline.get(0).getValue());

    pipeline = doc.getFields("ip");
    Assert.assertEquals(1, pipeline.size());
    Assert.assertEquals("112.37.117.33", pipeline.get(0).getValue());
*/
    // Remove the results from this test
    IngestJobMockMapRedOutFormat.writers.remove(jobName);
  }

  @Ignore("LWSHADOOP-120")
  @Test
  public void testBzip2CompressionWithGrok() throws Exception {

    Path input = new Path(tempDir, "GrokIngestMapper");
    fs.mkdirs(input);
    String compressedFileName = "ip-word-small.log.bz2";
    Path currentInput = new Path(input, compressedFileName);

    // Copy compressed file to HDFS
    URL url = IngestJobCompressionTest.class.getClassLoader().getResource(compressedFileName);
    assertTrue(url != null);
    Path localPath = new Path(url.toURI());
    fs.copyFromLocalFile(localPath, currentInput);

    // Copy LogStash configuartion
    url = IngestJobCompressionTest.class.getClassLoader().getResource("IP-WORD.conf");
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
/*
    List<PipelineField> pipeline = doc.getFields("message");
    Assert.assertEquals(1, pipeline.size());
    Assert.assertEquals("112.37.117.33 WORD__204623207",
            pipeline.get(0).getValue());

    pipeline = doc.getFields("received_from_field");
    Assert.assertEquals(1, pipeline.size());
    Assert.assertEquals("112.37.117.33", pipeline.get(0).getValue());

    pipeline = doc.getFields("message_field");
    Assert.assertEquals(1, pipeline.size());
    Assert.assertEquals("WORD__204623207", pipeline.get(0).getValue());

    pipeline = doc.getFields("path");
    Assert.assertEquals(1, pipeline.size());
    Assert.assertEquals(currentInput.toUri().getPath(),
            pipeline.get(0).getValue());

    pipeline = doc.getFields("log_message");
    Assert.assertEquals(1, pipeline.size());
    Assert.assertEquals("WORD__204623207", pipeline.get(0).getValue());

    pipeline = doc.getFields("ip");
    Assert.assertEquals(1, pipeline.size());
    Assert.assertEquals("112.37.117.33", pipeline.get(0).getValue());
*/
    // Remove the results from this test
    IngestJobMockMapRedOutFormat.writers.remove(jobName);
  }

  @Test
  public void testGzipCompressionWithRegex() throws Exception {

    Path input = new Path(tempDir, "RegexIngestMapper");
    fs.mkdirs(input);
    String compressedFileName = "regex-small.txt.gz";
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
/*
    List<PipelineField> pipeline = doc.getFields("matchFound");
    Assert.assertEquals(3, pipeline.size());
    Assert.assertEquals("wordA", pipeline.get(0).getValue());
    Assert.assertEquals("wordB", pipeline.get(1).getValue());
    Assert.assertEquals("wordC", pipeline.get(2).getValue());*/

    // Remove the results from this test
    IngestJobMockMapRedOutFormat.writers.remove(jobName);
  }

  @Test
  public void testBzip2CompressionWithRegex() throws Exception {

    Path input = new Path(tempDir, "RegexIngestMapper");
    fs.mkdirs(input);
    String compressedFileName = "regex-small.txt.bz2";
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
/*
    List<PipelineField> pipeline = doc.getFields("matchFound");
    Assert.assertEquals(3, pipeline.size());
    Assert.assertEquals("wordA", pipeline.get(0).getValue());
    Assert.assertEquals("wordB", pipeline.get(1).getValue());
    Assert.assertEquals("wordC", pipeline.get(2).getValue());*/

    // Remove the results from this test
    IngestJobMockMapRedOutFormat.writers.remove(jobName);
  }

  @Test
  public void testGzipCompressionWithSequenceFile() throws Exception {

    Path input = new Path(tempDir, "SequenceFileIngestMapper");
    fs.mkdirs(input);
    String compressedFileName = "frankestein_text_text_gzip_compressed.seq";
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

    // Remove the results from this test
    IngestJobMockMapRedOutFormat.writers.remove(jobName);
  }

  @Test
  public void testBzip2CompressionWithSequenceFile() throws Exception {

    Path input = new Path(tempDir, "SequenceFileIngestMapper");
    fs.mkdirs(input);
    String compressedFileName = "frankestein_text_text_bzip2_compressed.seq";
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

    // Remove the results from this test
    IngestJobMockMapRedOutFormat.writers.remove(jobName);
  }

  @Test
  public void testGzipCompressionWithSolrXML() throws Exception {

    Path input = new Path(tempDir, "SolrXMLIngestMapper");
    fs.mkdirs(input);
    String compressedFileName = "frankestein_text_solr_gzip_compressed.seq";
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

    // Remove the results from this test
    IngestJobMockMapRedOutFormat.writers.remove(jobName);
  }

  @Test
  public void testBzip2CompressionWithSolrXML() throws Exception {

    Path input = new Path(tempDir, "SolrXMLIngestMapper");
    fs.mkdirs(input);
    String compressedFileName = "frankestein_text_solr_bzip2_compressed.seq";
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

    // Remove the results from this test
    IngestJobMockMapRedOutFormat.writers.remove(jobName);
  }

  private static final String WARC_FIELD = "warc.";

  @Ignore
  @Test
  public void testGzipCompressionWithWarc() throws Exception {

    Path input = new Path(tempDir, "WarcIngestMapper");
    fs.mkdirs(input);
    String compressedFileName = "at.warc.gz";
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
/*
    List<PipelineField> pipeline = doc
            .getFields(WARC_FIELD + "WARC-Block-Digest" + "_s");
    Assert.assertEquals(1, pipeline.size());
    Assert.assertEquals("sha1:DOR77CIHLANEVHMQOWYV6IIWRQKAH2GA",
            pipeline.get(0).getValue());

    pipeline = doc.getFields(WARC_FIELD + "WARC-Filename" + "_s");
    Assert.assertEquals(1, pipeline.size());
    Assert.assertEquals("at.warc.gz", pipeline.get(0).getValue());
*/
    // Remove the results from this test
    IngestJobMockMapRedOutFormat.writers.remove(jobName);
  }

  @Ignore
  @Test
  public void testBzip2CompressionWithWarc() throws Exception {

    Path input = new Path(tempDir, "WarcIngestMapper");
    fs.mkdirs(input);
    String compressedFileName = "at.warc.bz2";
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
/*
    List<PipelineField> pipeline = doc
            .getFields(WARC_FIELD + "WARC-Block-Digest" + "_s");
    Assert.assertEquals(1, pipeline.size());
    Assert.assertEquals("sha1:DOR77CIHLANEVHMQOWYV6IIWRQKAH2GA",
            pipeline.get(0).getValue());

    pipeline = doc.getFields(WARC_FIELD + "WARC-Filename" + "_s");
    Assert.assertEquals(1, pipeline.size());
    Assert.assertEquals("at.warc.gz", pipeline.get(0).getValue());*/

    // Remove the results from this test
    IngestJobMockMapRedOutFormat.writers.remove(jobName);
  }

  @Test
  public void testGzipCompressionWithZip() throws Exception {

    Path input = new Path(tempDir, "ZipIngestMapper");
    fs.mkdirs(input);
    String compressedFileName = "zipData.zip.gz";
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
/*
    List<PipelineField> pipeline = doc.getFields("Content-Type");
    Assert.assertEquals(1, pipeline.size());
    Assert.assertEquals("application/msword", pipeline.get(0).getValue());

    pipeline = doc.getFields("Last-Author");
    Assert.assertEquals(1, pipeline.size());
    Assert.assertEquals("Michael McCandless", pipeline.get(0).getValue());

    pipeline = doc.getFields("Application-Name");
    Assert.assertEquals(1, pipeline.size());
    Assert.assertEquals("Microsoft Office Word", pipeline.get(0).getValue());

    pipeline = doc.getFields("Author");
    Assert.assertEquals(1, pipeline.size());
    Assert.assertEquals("Michael McCandless", pipeline.get(0).getValue());

    // pipeline = doc.getFields("Creation-Date");
    // Assert.assertEquals(1, pipeline.size());
    // Assert.assertEquals("2011-09-09T15:41:00Z", pipeline.get(0).getValue());*/

    // Remove the results from this test
    IngestJobMockMapRedOutFormat.writers.remove(jobName);
  }

  @Test
  public void testBzip2CompressionWithZip() throws Exception {

    Path input = new Path(tempDir, "ZipIngestMapper");
    fs.mkdirs(input);
    String compressedFileName = "zipData.zip.bz2";
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
/*
    List<PipelineField> pipeline = doc.getFields("Content-Type");
    Assert.assertEquals(1, pipeline.size());
    Assert.assertEquals("application/msword", pipeline.get(0).getValue());

    pipeline = doc.getFields("Last-Author");
    Assert.assertEquals(1, pipeline.size());
    Assert.assertEquals("Michael McCandless", pipeline.get(0).getValue());

    pipeline = doc.getFields("Application-Name");
    Assert.assertEquals(1, pipeline.size());
    Assert.assertEquals("Microsoft Office Word", pipeline.get(0).getValue());

    pipeline = doc.getFields("Author");
    Assert.assertEquals(1, pipeline.size());
    Assert.assertEquals("Michael McCandless", pipeline.get(0).getValue());

    // pipeline = doc.getFields("Creation-Date");
    // Assert.assertEquals(1, pipeline.size());
    // Assert.assertEquals("2011-09-09T15:41:00Z", pipeline.get(0).getValue());*/

    // Remove the results from this test
    IngestJobMockMapRedOutFormat.writers.remove(jobName);
  }
}
