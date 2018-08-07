package com.lucidworks.hadoop.ingest;

import com.lucidworks.hadoop.io.LWDocument;
import junit.framework.Assert;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileIterator;
import org.junit.Test;

import javax.xml.stream.XMLStreamException;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.net.URI;
import java.util.*;

import static com.lucidworks.hadoop.utils.ConfigurationKeys.COLLECTION;
import static com.lucidworks.hadoop.utils.ConfigurationKeys.ZK_CONNECT;
import static junit.framework.Assert.fail;
import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertNotNull;

public class SolrXMLIngestMapperTest extends BaseMiniClusterTestCase {

    @Test
    public void test() throws Exception {
        jobInput.add(TEST_XML_1);

        Configuration conf = getDefaultSolrXmlMapperConfiguration();
        Job job = createJobBasedOnConfiguration(conf, SolrXMLIngestMapper.class);
        final List<String> results = runJobSuccessfully(job, jobInput, 3);

        assertNumDocsProcessed(job, 3);
    }

    @Test
    public void testSolr() throws Exception {
        Configuration conf = getDefaultSolrXmlMapperConfiguration();
        loadFrankensteinDataFromSequenceFile(conf);

        Job job = createJobBasedOnConfiguration(conf, SolrXMLIngestMapper.class);
        final List<String> results = runJobSuccessfully(job, jobInput, jobInput.size());

        final int numDocsExpectedFromSeqFile = 776;
        assertNumDocsProcessed(job, numDocsExpectedFromSeqFile);
    }

    @Test
    public void solrXmlLoaderTest() throws Exception {
        SolrXMLIngestMapper.SolrXMLLoader loader = new SolrXMLIngestMapper().createXmlLoader("foo", "id");

        Collection<LWDocument> docs = loader
                .readDocs(new ByteArrayInputStream(TEST_XML_1.getBytes("UTF-8")), "junk");
        assertNotNull("Didn't get docs", docs);
        Assert.assertEquals(3, docs.size());
        for (LWDocument doc : docs) {
            assertNotNull("doc.id is null", doc.getId());
            //      Object name = doc.getFirstField("name");
            //      assertNotNull("name is null", name);
            //      assertNull(doc.getFirstField("id"));
        }

        docs = loader
                .readDocs(new ByteArrayInputStream("<foo>this is junk</foo>".getBytes("UTF-8")), "junk");
        Assert.assertEquals(0, docs.size());
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

    private Configuration getDefaultSolrXmlMapperConfiguration() {
        Configuration conf = getBaseConfiguration();
        conf.set(COLLECTION, "collection");
        conf.set(ZK_CONNECT, "localhost:0000");
        conf.set("idField", "id");

        return conf;
    }

    private void loadFrankensteinDataFromSequenceFile(Configuration conf) throws Exception {
        final String sequenceFilePathSubstring = "sequence" + File.separator + "frankenstein_text_solr.seq";
        final URI fullSeqFileUri = SolrXMLIngestMapperTest.class.getClassLoader().getResource(sequenceFilePathSubstring).toURI();
        Path seqFilePath = new Path(fullSeqFileUri);
        SequenceFileIterator<Text, Text> iterator = new SequenceFileIterator<Text, Text>(seqFilePath, true, conf);
        Set<String> ids = new HashSet<String>();
        while (iterator.hasNext()) {
            Pair<Text, Text> pair = iterator.next();
            jobInput.add(pair.getSecond().toString());
        }
    }

    public static final String TEST_XML_1 = "<update><add>" +
            "<doc>" +
            "  <field name=\"id\">SP2514N</field>" +
            "  <field name=\"name\">Samsung SpinPoint P120 SP2514N - hard drive - 250 GB - ATA-133</field>"
            +
            "  <field name=\"manu\">Samsung Electronics Co. Ltd.</field>" +
            "  <!-- Join -->" +
            "  <field name=\"manu_id_s\">samsung</field>" +
            "  <field name=\"cat\">electronics</field>" +
            "  <field name=\"cat\">hard drive</field>" +
            "  <field name=\"features\">7200RPM, 8MB cache, IDE Ultra ATA-133</field>" +
            "  <field name=\"features\">NoiseGuard, SilentSeek technology, Fluid Dynamic Bearing (FDB) motor</field>"
            +
            "  <field name=\"price\">92</field>" +
            "  <field name=\"popularity\">6</field>" +
            "  <field name=\"inStock\">true</field>" +
            "  <field name=\"manufacturedate_dt\">2006-02-13T15:26:37Z</field>" +
            "  <!-- Near Oklahoma city -->" +
            "  <field name=\"store\">35.0752,-97.032</field>" +
            "</doc>" +
            "" +
            "<doc>" +
            "  <field name=\"id\">6H500F0</field>" +
            "  <field name=\"name\">Maxtor DiamondMax 11 - hard drive - 500 GB - SATA-300</field>" +
            "  <field name=\"manu\">Maxtor Corp.</field>" +
            "  <!-- Join -->" +
            "  <field name=\"manu_id_s\">maxtor</field>" +
            "  <field name=\"cat\">electronics</field>" +
            "  <field name=\"cat\">hard drive</field>" +
            "  <field name=\"features\">SATA 3.0Gb/s, NCQ</field>" +
            "  <field name=\"features\">8.5ms seek</field>" +
            "  <field name=\"features\">16MB cache</field>" +
            "  <field name=\"price\">350</field>" +
            "  <field name=\"popularity\">6</field>" +
            "  <field name=\"inStock\">true</field>" +
            "  <!-- Buffalo store -->" +
            "  <field name=\"store\">45.17614,-93.87341</field>" +
            "  <field name=\"manufacturedate_dt\">2006-02-13T15:26:37Z</field>" +
            "</doc>" +
            //no id field
            "<doc>" +
            "  <field name=\"name\">no id</field>" +
            "  <field name=\"manufacturedate_dt\">2006-02-13T15:26:37Z</field>" +
            "</doc></add>" +
            "<delete><id>1234</id></delete></update>";
}
