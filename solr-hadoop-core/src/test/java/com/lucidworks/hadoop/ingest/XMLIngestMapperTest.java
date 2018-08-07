package com.lucidworks.hadoop.ingest;

import com.lucidworks.hadoop.cache.DistributedCacheHandler;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Test;

import java.util.List;

import static com.lucidworks.hadoop.utils.ConfigurationKeys.COLLECTION;
import static com.lucidworks.hadoop.utils.ConfigurationKeys.ZK_CONNECT;
import static junit.framework.TestCase.assertTrue;

public class XMLIngestMapperTest extends BaseMiniClusterTestCase {

    private static final String XSLT_FILE_LOCATION = XMLIngestMapperTest.class.getClassLoader()
            .getResource("xml/xml_ingest_mapper.xsl").getPath();
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

    @Test
    public void test() throws Exception {
        jobInput.add(TEST_XML);
        Configuration conf = getDefaultXmlMapperConfiguration();
        Job job = createJobBasedOnConfiguration(conf, XMLIngestMapper.class);
        DistributedCacheHandler.addFileToCache((org.apache.hadoop.mapred.JobConf)job.getConfiguration(),
                new Path(XSLT_FILE_LOCATION), "lww.xslt");

        final List<String> results = runJobSuccessfully(job, jobInput, 2);

        assertNumDocsProcessed(job, 2);
        final String doc1 = results.get(0);
        assertTrue("Failed to extract 'id' from first doc", doc1.contains("id=1"));
        assertTrue("Failed to extract 'text' from first doc", doc1.contains("text=this is a test"));
        assertTrue("Failed to extract 'child1.foo' from first doc", doc1.contains("child1.foo=bar"));
        assertTrue("Failed to extract 'int' from first doc", doc1.contains("int=5150"));
        assertTrue("Failed to extract 'p_attr' from first doc", doc1.contains("p_attr=yo"));
        final String doc2 = results.get(1);
        assertTrue("Failed to extract 'id' from second doc", doc2.contains("id=2"));
        assertTrue("Failed to extract 'text' from second doc", doc2.contains("text=this is another test"));
        assertTrue("Failed to extract 'child1.foo' from second doc", doc2.contains("child1.foo=baz"));
        assertTrue("Failed to extract 'int' from second doc", doc2.contains("int=5151"));
        assertTrue("Failed to extract 'p_attr' from second doc", doc2.contains("p_attr=yo"));
    }

    private Configuration getDefaultXmlMapperConfiguration() throws Exception {
        Configuration conf = getBaseConfiguration();
        conf.set(COLLECTION, "collection");
        conf.set(ZK_CONNECT, "localhost:0000");
        conf.set("idField", "id");
        conf.set("lww.xml.docXPathExpr", "//doc");
        conf.set("lww.xml.includeParentAttrsPrefix", "p_");
        conf.set("lww.xml.start", "");
        conf.set("lww.xml.end", "");

        // apply an XSLt to the source document as part of the mapper task
        Path remotePath = copyLocalResourceToHdfs(XSLT_FILE_LOCATION,"xml_ingest_mapper.xsl");
        conf.set("lww.xslt", remotePath.toUri().toString());
        jobCacheUris.add(remotePath.toUri());

        return conf;
    }
}
