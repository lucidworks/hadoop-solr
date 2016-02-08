package com.lucidworks.hadoop.ingest;

import com.lucidworks.hadoop.io.LWDocument;
import com.lucidworks.hadoop.io.LWDocumentWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mrunit.MapDriver;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class XMLIngestMapperTest extends BaseIngestMapperTestCase {

  MapDriver<Writable, Text, Text, LWDocumentWritable> mapDriver;

  private static final String TEST_XML = ""
      + "<root attr='yo'>"
      + "  <dok id='1'>"
      + "    <text>this is a test</text>"
      + "    <child1 foo='bar'/>"
      + "    <int>5150</int>"
      + "  </dok>"
      + "  <dok id='2'>"
      + "    <text>this is another test</text>"
      + "    <child1 foo='baz'/>"
      + "    <int>5151</int>"
      + "  </dok>"
      + "</root>";

  @Before
  public void setUp() throws Exception {
    XMLIngestMapper mapper = new XMLIngestMapper();
    mapDriver = new MapDriver<Writable, Text, Text, LWDocumentWritable>();
    mapDriver.setMapper(mapper);

    Configuration conf = mapDriver.getConfiguration();
    setupCommonConf(conf);
    conf.set("lww.xml.docXPathExpr", "//doc");
    conf.set("lww.xml.includeParentAttrsPrefix", "p_");

    conf.set("lww.xml.start", "");
    conf.set("lww.xml.end", "");

    // apply an XSLt to the source document as part of the mapper task
    String xmlXsl = "xml/xml_ingest_mapper.xsl";
    String xsltInDCache = XMLIngestMapperTest.class.getClassLoader().getResource(xmlXsl).getPath();

    conf.set("lww.xslt", xsltInDCache);
    mapDriver.withCacheFile(xsltInDCache);

    mapper.getFixture().init(new JobConf(mapDriver.getConfiguration()));
  }

  @Test
  public void test() throws Exception {
    Text key = new Text("");
    Text value = new Text(TEST_XML);
    mapDriver.withInput(key, value);
    mapDriver.withCounter(BaseHadoopIngest.Counters.DOCS_ADDED, 2);
    List<org.apache.hadoop.mrunit.types.Pair<Text, LWDocumentWritable>> run = mapDriver.run();
    assertEquals(2, run.size());
    LWDocument doc;

    doc = run.get(0).getSecond().getLWDocument();
    assertNotNull("document is null", doc);
    assertEquals("1", doc.getId());
    assertEquals("this is a test", doc.getFirstFieldValue("text"));
    assertEquals("bar", doc.getFirstFieldValue("child1.foo"));
    assertEquals("5150", doc.getFirstFieldValue("int"));
    assertEquals("yo", doc.getFirstFieldValue("p_attr"));

    doc = run.get(1).getSecond().getLWDocument();
    assertEquals("2", doc.getId());
    assertEquals("this is another test", doc.getFirstFieldValue("text"));
    assertEquals("baz", doc.getFirstFieldValue("child1.foo"));
    assertEquals("5151", doc.getFirstFieldValue("int"));
    assertEquals("yo", doc.getFirstFieldValue("p_attr"));
  }
}