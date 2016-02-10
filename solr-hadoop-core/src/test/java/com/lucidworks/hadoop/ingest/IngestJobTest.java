package com.lucidworks.hadoop.ingest;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import com.lucidworks.hadoop.io.FusionOutputFormat;
import com.lucidworks.hadoop.io.LWMapRedOutputFormat;
import com.lucidworks.hadoop.utils.IngestJobMockMapRedOutFormat;
import com.lucidworks.hadoop.utils.MockRecordWriter;
import com.lucidworks.hadoop.utils.TestUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 *
 *
 **/
public class IngestJobTest extends IngestJobInit {

  @Test
  public void testCSV() throws Exception {
    Path input = new Path(tempDir, "foo.csv");
    StringBuilder buffer = new StringBuilder("id,bar,junk,zen,hockey").append(lineSep)
        .append("id-1, The quick brown fox, jumped, head, gretzky, extra").append(lineSep)
        .append("id-2, The quick red fox, kicked, head, gretzky");

    addContentToFS(input, buffer.toString());

    String jobName = "testCsv";
    String[] args = TestUtils
        .createHadoopJobArgsWithConf(jobName, CSVIngestMapper.class.getName(), DEFAULT_COLLECTION,
            getBaseUrl(), input.toUri().toString(),
            "csvFieldMapping[0=id,1=bar, 2=junk , 3 = zen ,4 = hockey];idField[id];csvFirstLineComment[true]");

    // TODO: ToolRunner has some problems with exiting, so this may cause conflicts
    int val = ToolRunner.run(conf, new IngestJob(), args);
    assertEquals(0, val);
    verifyJob(jobName, 2, new String[]{"id-1", "id-2"}, "hockey", "field_5");

    jobName = "testCsv2";
    args = TestUtils
        .createHadoopJobArgsWithConf(jobName, CSVIngestMapper.class.getName(), DEFAULT_COLLECTION,
            getBaseUrl(), input.toUri().toString(),
            "csvFieldMapping[0=id,1=bar, 2=junk , 3 = zen ,4 = hockey];idField[id]");

    ToolRunner.run(conf, new IngestJob(), args);
    verifyJob(jobName, 3, new String[]{"id-1", "id-2"}, "hockey", "field_5");

    jobName = "testCsvFieldId";
    // id Field is the the field called "junk"
    args = TestUtils
        .createHadoopJobArgsWithConf(jobName, CSVIngestMapper.class.getName(), DEFAULT_COLLECTION,
            getBaseUrl(), input.toUri().toString(),
            "csvFieldMapping[0=bar, 1=id, 2=junk , 3 = zen ,4 = hockey];idField[junk]");
    ToolRunner.run(conf, new IngestJob(), args);
    verifyJob(jobName, 3, new String[]{"jumped", "kicked"}, "hockey", "field_5");
  }

  @Test
  public void testDirectoy() throws Exception {
    String dir = "dir" + File.separator + "docs";
    File dirFile = new File(ClassLoader.getSystemClassLoader().getResource(dir).getPath());
    assertTrue(dir + " does not exist: " + dirFile.getAbsolutePath(), dirFile.exists());
    Path input = new Path(tempDir, dir);
    // Upload each file to fs
    for (File file : dirFile.listFiles()) {
      if (!file.isDirectory()) {
        Path filePath = new Path(input, file.getName());
        addContentToFS(filePath, Files.toByteArray(file));
      }
    }
    String jobName = "testDirectoy";
    String[] args = TestUtils
        .createHadoopOptionalArgs(jobName, DirectoryIngestMapper.class.getName(),
            DEFAULT_COLLECTION, getBaseUrl(), input.toUri().toString() + "/*");

    int val = ToolRunner.run(conf, new IngestJob(), args);
    assertEquals(0, val);
    // verifyJob
    MockRecordWriter writer = IngestJobMockMapRedOutFormat.writers.get(jobName);
    assertNotNull(writer);
    assertEquals(7, writer.map.size());
  }

  @Test
  public void testZip() throws Exception {
    String zip = "zip/zipData.zip";
    File zipFile = new File(ClassLoader.getSystemClassLoader().getResource(zip).getPath());
    assertTrue(zip + " does not exist: " + zipFile.getAbsolutePath(), zipFile.exists());
    Path input = new Path(tempDir, zip);
    addContentToFS(input, Files.toByteArray(zipFile));

    String jobName = "testZip";
    String[] args = TestUtils
        .createHadoopOptionalArgs(jobName, ZipIngestMapper.class.getName(), DEFAULT_COLLECTION,
            getBaseUrl(), input.toUri().toString());

    int val = ToolRunner.run(conf, new IngestJob(), args);
    assertEquals(0, val);
    verifyJob(jobName, 6,
        new String[]{"test0.pdf", "test1.doc", "test0.doc", "test3.pdf", "test2.pdf",
            "test1.pdf"}, "body", "Content-Type");
  }

  @Test
  public void testWarc() throws Exception {
    String warc = "warc/at.warc";
    File warcFile = new File(ClassLoader.getSystemClassLoader().getResource(warc).getPath());
    assertTrue(warc + " does not exist: " + warcFile.getAbsolutePath(), warcFile.exists());
    Path input = new Path(tempDir, warc);
    addContentToFS(input, Files.toString(warcFile, Charsets.UTF_8));

    String jobName = "testWarc";
    String[] args = TestUtils
        .createHadoopOptionalArgs(jobName, WarcIngestMapper.class.getName(), DEFAULT_COLLECTION,
            getBaseUrl(), input.toUri().toString());

    int val = ToolRunner.run(conf, new IngestJob(), args);
    assertEquals(0, val);
    verifyJob(jobName, 3, new String[]{"<urn:uuid:b328f1fe-b2ee-45c0-9139-908850810b52>",
            "<urn:uuid:6ee9accb-a284-47ef-8785-ed28aee2f79e>"}, "warc.WARC-Target-URI",
        "warc.WARC-Warcinfo-ID");
  }

  @Test
  public void testSolrXML() throws Exception {
    String solr = "sequence" + File.separator + "frankenstein_text_solr.seq";
    File solrFile = new File(ClassLoader.getSystemClassLoader().getResource(solr).getPath());
    assertTrue(solr + " does not exist: " + solrFile.getAbsolutePath(), solrFile.exists());
    Path input = new Path(tempDir, solr);
    addContentToFS(input, Files.toByteArray(solrFile));

    String jobName = "testSolrXML";
    String[] args = TestUtils
        .createHadoopOptionalArgs(jobName, SolrXMLIngestMapper.class.getName(), DEFAULT_COLLECTION,
            getBaseUrl(), input.toUri().toString());

    int val = ToolRunner.run(conf, new IngestJob(), args);
    assertEquals(0, val);
    verifyJob(jobName, 776, new String[]{"solr_521", "solr_137", "solr_519"}, "body");
  }

  @Test
  public void testSequenceFile() throws Exception {
    String seq = "sequence" + File.separator + "frankenstein_text_text.seq";
    File seqFile = new File(ClassLoader.getSystemClassLoader().getResource(seq).getPath());
    assertTrue(seq + " does not exist: " + seqFile.getAbsolutePath(), seqFile.exists());
    Path input = new Path(tempDir, seq);
    addContentToFS(input, Files.toByteArray(seqFile));

    String jobName = "testSequenceFile";
    String[] args = TestUtils
        .createHadoopOptionalArgs(jobName, SequenceFileIngestMapper.class.getName(),
            DEFAULT_COLLECTION, getBaseUrl(), input.toUri().toString());

    int val = ToolRunner.run(conf, new IngestJob(), args);
    assertEquals(0, val);
    verifyJob(jobName, 776,
        new String[]{"frank_seq_558", "frank_seq_171", "frank_seq_554", "frank_seq_551"});
  }

  @Test
  public void testRegex() throws Exception {
    String regex1 = "regex" + File.separator + "regex-small.txt";
    File regexFile1 = new File(ClassLoader.getSystemClassLoader().getResource(regex1).getPath());
    assertTrue(regex1 + " does not exist: " + regexFile1.getAbsolutePath(), regexFile1.exists());
    Path input1 = new Path(tempDir, regex1);
    addContentToFS(input1, Files.toByteArray(regexFile1));

    String regex2 = "regex" + File.separator + "regex-small-2.txt";
    File regexFile2 = new File(ClassLoader.getSystemClassLoader().getResource(regex2).getPath());
    assertTrue(regex2 + " does not exist: " + regexFile2.getAbsolutePath(), regexFile2.exists());
    Path input2 = new Path(tempDir, regex2);
    addContentToFS(input2, Files.toByteArray(regexFile2));

    String jobName = "testRegex";
    String[] args = TestUtils
        .createHadoopOptionalArgs(jobName, RegexIngestMapper.class.getName(), DEFAULT_COLLECTION,
            getBaseUrl(),
            tempDir.toUri().toString() + File.separator + "regex" + File.separator + "regex-small*",
            "-D" + RegexIngestMapper.REGEX + "=\\w+",
            "-D" + RegexIngestMapper.GROUPS_TO_FIELDS + "=0=match");

    int val = ToolRunner.run(conf, new IngestJob(), args);
    assertEquals(0, val);
    MockRecordWriter mockRecordWriter = IngestJobMockMapRedOutFormat.writers.get(jobName);
    Assert.assertNotNull(mockRecordWriter);
    assertEquals(2, mockRecordWriter.map.size());
  }

  @Ignore // The distributed cache is not adding the conf file
  @Test
  public void testGrok() throws Exception {
    String grok = "grok" + File.separator + "ip-word.log";
    File grokFile = new File(ClassLoader.getSystemClassLoader().getResource(grok).getPath());
    assertTrue(grok + " does not exist: " + grokFile.getAbsolutePath(), grokFile.exists());
    Path input = new Path(tempDir, grok);
    addContentToFS(input, Files.toByteArray(grokFile));

    // Adding the grok-conf file
    String grokConf = "grok" + File.separator + "IP-WORD.conf";
    File grokConfFile = new File(
        ClassLoader.getSystemClassLoader().getResource(grokConf).getPath());
    assertTrue(grokConf + " does not exist: " + grokConfFile.getAbsolutePath(),
        grokConfFile.exists());

    String jobName = "testGrok";
    String[] args = TestUtils
        .createHadoopOptionalArgs(jobName, GrokIngestMapper.class.getName(), DEFAULT_COLLECTION,
            getBaseUrl(), input.toUri().toString(), "-Dgrok.uri=" + grokConfFile);

    int val = ToolRunner.run(conf, new IngestJob(), args);
    assertEquals(0, val);
    MockRecordWriter mockRecordWriter = IngestJobMockMapRedOutFormat.writers.get(jobName);
    Assert.assertNotNull(mockRecordWriter);
    assertEquals(4000, mockRecordWriter.map.size());
  }

  @Test
  public void testReducer() throws Exception {
    Path input = new Path(tempDir, "reducer.csv");
    StringBuilder buffer = new StringBuilder("id,bar,junk,zen,hockey").append(lineSep)
        .append("id-1, The quick brown fox, jumped, head, gretzky, extra").append(lineSep)
        .append("id-2, The quick red fox, kicked, head, gretzky");

    addContentToFS(input, buffer.toString());

    String jobName = "testCsvReducers";
    String[] args = TestUtils
        .createHadoopOptionalArgs(jobName, CSVIngestMapper.class.getName(), DEFAULT_COLLECTION,
            getBaseUrl(), input.toUri().toString(), "--" + IngestJob.REDUCER_OPTION,
            IngestReducer.class.getName(), "--" + IngestJob.NUM_REDUCERS_OPTION, "3",
            "--" + IngestJob.CONF_OPTION,
            "csvFieldMapping[0=id,1=bar, 2=junk , 3 = zen ,4 = hockey];idField[id];csvFirstLineComment[true]");
    conf.set("io.serializations", "com.lucidworks.hadoop.io.impl.LWMockSerealization");
    conf.set("io.sort.mb", "1");
    ToolRunner.run(conf, new IngestJob(), args);
    verifyJob(jobName, 2, new String[]{"id-1", "id-2"}, "hockey", "field_5");
  }

  @Test
  public void testBadArgs() throws Exception {
    String jobName = "testDidnotIngetAnyDocs";
    // tempDir is empty.
    String[] args = TestUtils
        .createHadoopJobArgs(jobName, DirectoryIngestMapper.class.getName(), DEFAULT_COLLECTION,
            getBaseUrl(), tempDir.toUri().toString());

    int val = ToolRunner.run(conf, new IngestJob(), args);
    assertEquals(1, val);
    assertErrorMessage("Didn't ingest any document");

    Path input = new Path(tempDir, "foo.csv");
    StringBuilder buffer = new StringBuilder("id,bar,junk,zen,hockey").append(lineSep)
        .append("id-1, The quick brown fox, jumped, head, gretzky, extra").append(lineSep)
        .append("id-2, The quick red fox, kicked, head, gretzky");

    addContentToFS(input, buffer.toString());

    jobName = "testBadMapper";
    // foo -> bad mapper option
    args = TestUtils.createHadoopJobArgs(jobName, "foo", DEFAULT_COLLECTION, getBaseUrl(),
        input.toUri().toString());

    val = ToolRunner.run(conf, new IngestJob(), args);
    assertEquals(1, val);
    assertErrorMessage("Unable to instantiate AbstractIngestMapper class");

    jobName = "testInvalidSolrConnection";
    // Plus one to the current jetty port to ensure this not exists
    String invalidSolrConnection = getBaseUrl() + "+1";

    args = TestUtils
        .createHadoopJobArgsWithConf(jobName, CSVIngestMapper.class.getName(), DEFAULT_COLLECTION,
            invalidSolrConnection, input.toUri().toString(),
            "csvFieldMapping[0=id,1=bar, 2=junk , 3 = zen ,4 = hockey];idField[id]", LWMapRedOutputFormat.class
                .getName());

    val = ToolRunner.run(conf, new IngestJob(), args);
    assertEquals(1, val);
    assertErrorMessage("server not available on");

    jobName = "testBadReducer";
    // foo -> bad reducer class
    args = TestUtils
        .createHadoopOptionalArgs(jobName, CSVIngestMapper.class.getName(), DEFAULT_COLLECTION,
            getBaseUrl(), input.toUri().toString(), "--" + IngestJob.REDUCER_OPTION, "foo",
            "--" + IngestJob.NUM_REDUCERS_OPTION, "3", "--" + IngestJob.CONF_OPTION,
            "csvFieldMapping[0=id,1=bar, 2=junk , 3 = zen ,4 = hockey];idField[id]");

    val = ToolRunner.run(conf, new IngestJob(), args);
    assertEquals(1, val);
    assertErrorMessage("Unable to instantiate IngestReducer class");

    jobName = "testNoZKorS";
    // zk/s -> null
    args = TestUtils
        .createHadoopJobArgs(jobName, CSVIngestMapper.class.getName(), DEFAULT_COLLECTION, null,
            input.toUri().toString(), LWMapRedOutputFormat.class.getName());

    val = ToolRunner.run(conf, new IngestJob(), args);
    assertEquals(1, val);
    assertErrorMessage("You must specify either the.*or the");

    // Missing options - will print the usage
    jobName = "testNullArgs";
    val = ToolRunner.run(conf, new IngestJob(), null);
    assertEquals(1, val);
    assertErrorMessage("Missing required option ");
  }

  @Test
  public void testPingWrongCollection() throws Exception {
    String jobName = "testInvalidSolrConnection";
    // Plus one to the current jetty port to ensure this not exists
    String invalidSolrConnection = getBaseUrl();

    Path input = new Path(tempDir, "foo.csv");
    StringBuilder buffer = new StringBuilder("id,bar,junk,zen,hockey").append(lineSep)
        .append("id-1, The quick brown fox, jumped, head, gretzky, extra").append(lineSep)
        .append("id-2, The quick red fox, kicked, head, gretzky");

    addContentToFS(input, buffer.toString());
    String[] args = TestUtils
        .createHadoopJobArgsWithConf(jobName, CSVIngestMapper.class.getName(), "INVALID-COLLECTION",
            invalidSolrConnection, input.toUri().toString(),
            "csvFieldMapping[0=id,1=bar, 2=junk , 3 = zen ,4 = hockey];idField[id]", LWMapRedOutputFormat.class
                .getName());

    int val = ToolRunner.run(conf, new IngestJob(), args);
    assertEquals(1, val);
    assertErrorMessage("Make sure that collection");
  }

  @Test
  public void testConfHandling() throws Exception {
    JobConf conf = new JobConf();
    TestIngestJob ij = new TestIngestJob();
    ij.processConf("foo[true];bar[1];junk[2.3];hockey[this is a string]", conf);
    assertTrue(conf.getBoolean("foo", false));
    assertEquals(1, conf.getInt("bar", 0));
    assertEquals(2.3, conf.getFloat("junk", 0), 0.1);
    assertEquals("this is a string", conf.get("hockey"));
    try {
      ij.processConf("foo", conf);// bad
      fail();
    } catch (Exception e) {
      assertTrue(e.getMessage().startsWith("Can't parse"));
    }
  }

  @Test
  public void testXML() throws Exception {
    String xsl = "xml" + File.separator + "xml_ingest_mapper.xsl";
    File xslFile = new File(ClassLoader.getSystemClassLoader().getResource(xsl).getPath());
    assertTrue(xsl + " does not exist: " + xslFile.getAbsolutePath(), xslFile.exists());
    Path inputXsl = new Path(tempDir, xsl);
    addContentToFS(inputXsl, Files.toByteArray(xslFile));

    String xml = "xml" + File.separator + "foo.xml";
    File xmlFile = new File(ClassLoader.getSystemClassLoader().getResource(xml).getPath());
    assertTrue(xml + " does not exist: " + xmlFile.getAbsolutePath(), xmlFile.exists());
    Path input = new Path(tempDir, xml);
    addContentToFS(input, Files.toByteArray(xmlFile));

    String jobName = "testXml";
    String[] args = TestUtils
        .createHadoopJobArgsWithConf(jobName, XMLIngestMapper.class.getName(), DEFAULT_COLLECTION,
            getBaseUrl(), input.toUri().toString(), "lww.xslt[" + inputXsl + "];lww.xml.start[root];" +
                "lww.xml.end[root];lww.xml.docXPathExpr[//doc];lww.xml.includeParentAttrsPrefix[p_]");

    int val = ToolRunner.run(conf, new IngestJob(), args);
    assertEquals(0, val);
    verifyJob(jobName, 2, new String[]{"1", "2"}, "text", "int");
  }
}
