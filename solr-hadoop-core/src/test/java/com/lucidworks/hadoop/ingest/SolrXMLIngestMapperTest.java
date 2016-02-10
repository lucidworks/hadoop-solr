package com.lucidworks.hadoop.ingest;

import com.lucidworks.hadoop.io.LWDocument;
import com.lucidworks.hadoop.io.LWDocumentWritable;
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

import javax.xml.stream.XMLStreamException;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.fail;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 *
 *
 **/
public class SolrXMLIngestMapperTest extends BaseIngestMapperTestCase {

  MapDriver<Writable, Text, Text, LWDocumentWritable> mapDriver;
  SolrXMLIngestMapper mapper = null;

  @Before
  public void setUp() throws IOException {
    mapper = new SolrXMLIngestMapper();
    mapDriver = new MapDriver<Writable, Text, Text, LWDocumentWritable>();
    mapDriver.setConfiguration(createConf());

    mapDriver.setMapper(mapper);
    setupCommonConf(mapDriver.getConfiguration());
    mapper.getFixture().init(new JobConf(mapDriver.getConfiguration()));
  }

  @Test
  public void test() throws Exception {
    Text key = new Text("blah");
    Text value = new Text(TEST_XML_1);
    mapDriver.withInput(key, value);
    mapDriver.withCounter(BaseHadoopIngest.Counters.DOCS_ADDED, 3);
    List<org.apache.hadoop.mrunit.types.Pair<Text, LWDocumentWritable>> run = mapDriver.run();
    assertEquals(3, run.size());
    LWDocument doc = run.get(0).getSecond().getLWDocument();
    assertNotNull("document is null", doc);
    //TODO: more checking of document here
  }

  @Test
  public void testSolr() throws Exception {
    Configuration conf = mapDriver.getConfiguration();

    Path path = new Path(SolrXMLIngestMapperTest.class.getClassLoader()
        .getResource("sequence" + File.separator + "frankenstein_text_solr.seq").toURI());
    FileSystem fs = FileSystem.get(conf);
    Path dir = new Path(fs.getWorkingDirectory(), "build");
    Path sub = new Path(dir, "SXIMT");
    Path tempDir = new Path(sub, "tmp-dir");
    fs.mkdirs(tempDir);
    Path dst = new Path(tempDir, "frankenstein_text_solr.seq");
    fs.copyFromLocalFile(path, dst);

    SequenceFileIterator<Text, Text> iterator = new SequenceFileIterator<Text, Text>(dst, true, conf);
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
    assertTrue("ID: " + doc.getId(), ids.contains(doc.getId()));
    assertTrue(i > 0);
  }

  @Test
  public void solrXMLLoaderTest() throws Exception {
    SolrXMLIngestMapper.SolrXMLLoader loader = mapper.createXmlLoader("foo", "id");

    Collection<LWDocument> docs = loader
        .readDocs(new ByteArrayInputStream(TEST_XML_1.getBytes("UTF-8")), "junk");
    assertNotNull("Didn't get docs", docs);
    assertEquals(3, docs.size());
    for (LWDocument doc : docs) {
      assertNotNull("doc.id is null", doc.getId());
      //      Object name = doc.getFirstField("name");
      //      assertNotNull("name is null", name);
      //      assertNull(doc.getFirstField("id"));
    }

    docs = loader
        .readDocs(new ByteArrayInputStream("<foo>this is junk</foo>".getBytes("UTF-8")), "junk");
    assertEquals(0, docs.size());
    //bad doc
    try {
      docs = loader
          .readDocs(new ByteArrayInputStream(TEST_XML_1.substring(0, 100).getBytes("UTF-8")),
              "junk");
      //Assert.
      fail();
    } catch (XMLStreamException e) {
      //expected.  Better way of doing this?
    }

  }

  public static final String TEST_XML_1 = "<update><add>\n" +
      "<doc>\n" +
      "  <field name=\"id\">SP2514N</field>\n" +
      "  <field name=\"name\">Samsung SpinPoint P120 SP2514N - hard drive - 250 GB - ATA-133</field>\n"
      +
      "  <field name=\"manu\">Samsung Electronics Co. Ltd.</field>\n" +
      "  <!-- Join -->\n" +
      "  <field name=\"manu_id_s\">samsung</field>\n" +
      "  <field name=\"cat\">electronics</field>\n" +
      "  <field name=\"cat\">hard drive</field>\n" +
      "  <field name=\"features\">7200RPM, 8MB cache, IDE Ultra ATA-133</field>\n" +
      "  <field name=\"features\">NoiseGuard, SilentSeek technology, Fluid Dynamic Bearing (FDB) motor</field>\n"
      +
      "  <field name=\"price\">92</field>\n" +
      "  <field name=\"popularity\">6</field>\n" +
      "  <field name=\"inStock\">true</field>\n" +
      "  <field name=\"manufacturedate_dt\">2006-02-13T15:26:37Z</field>\n" +
      "  <!-- Near Oklahoma city -->\n" +
      "  <field name=\"store\">35.0752,-97.032</field>\n" +
      "</doc>\n" +
      "\n" +
      "<doc>\n" +
      "  <field name=\"id\">6H500F0</field>\n" +
      "  <field name=\"name\">Maxtor DiamondMax 11 - hard drive - 500 GB - SATA-300</field>\n" +
      "  <field name=\"manu\">Maxtor Corp.</field>\n" +
      "  <!-- Join -->\n" +
      "  <field name=\"manu_id_s\">maxtor</field>\n" +
      "  <field name=\"cat\">electronics</field>\n" +
      "  <field name=\"cat\">hard drive</field>\n" +
      "  <field name=\"features\">SATA 3.0Gb/s, NCQ</field>\n" +
      "  <field name=\"features\">8.5ms seek</field>\n" +
      "  <field name=\"features\">16MB cache</field>\n" +
      "  <field name=\"price\">350</field>\n" +
      "  <field name=\"popularity\">6</field>\n" +
      "  <field name=\"inStock\">true</field>\n" +
      "  <!-- Buffalo store -->\n" +
      "  <field name=\"store\">45.17614,-93.87341</field>\n" +
      "  <field name=\"manufacturedate_dt\">2006-02-13T15:26:37Z</field>\n" +
      "</doc>\n" +
      //no id field
      "<doc>\n" +
      "  <field name=\"name\">no id</field>\n" +
      "  <field name=\"manufacturedate_dt\">2006-02-13T15:26:37Z</field>\n" +
      "</doc></add>\n" +
      "<delete><id>1234</id></delete></update>";

}

