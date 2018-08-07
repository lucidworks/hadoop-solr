package com.lucidworks.hadoop.ingest;

import com.lucidworks.hadoop.io.LWDocument;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileIterator;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.net.URI;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.lucidworks.hadoop.utils.ConfigurationKeys.COLLECTION;
import static com.lucidworks.hadoop.utils.ConfigurationKeys.ZK_CONNECT;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class RegexIngestMapperTest extends BaseMiniClusterTestCase {
    @Test
    public void test() throws Exception {
        Configuration conf = getDefaultRegexIngestMapperConfiguration();
        loadFrankensteinDataFromSequenceFile(conf); // Input is stored in 'sample.txt'
        Job job = createJobBasedOnConfiguration(conf, RegexIngestMapper.class);

        List<String> results = runJobSuccessfully(job, jobInput, 776);

        assertNumDocsProcessed(job, 776);
        assertTrue("Expected to parse ID for first input file", results.get(0).contains("sample.txt"));
    }

    @Test
    public void testMatch() throws Exception {
        jobInput.add("text 1 text 2");
        Configuration conf = getDefaultRegexIngestMapperConfiguration();
        conf.set(RegexIngestMapper.REGEX, "(\\w+)\\s+(\\d+)\\s+(\\w+)\\s+(\\d+)");
        conf.set(RegexIngestMapper.GROUPS_TO_FIELDS, "0=body,1=text,2=number");
        conf.setBoolean(RegexIngestMapper.REGEX_MATCH, true);
        Job job = createJobBasedOnConfiguration(conf, RegexIngestMapper.class);

        List<String> results = runJobSuccessfully(job, jobInput, 1);

        assertNumDocsProcessed(job, 1);
        assertNotNull("document is null", results.get(0));
    }

    @Test
    public void testSimple() throws Exception {
        jobInput.add("text 1 text 2");
        Configuration conf = getDefaultRegexIngestMapperConfiguration();
        conf.set(RegexIngestMapper.REGEX, "(\\w+)\\s+(\\d+)");
        conf.set(RegexIngestMapper.GROUPS_TO_FIELDS, "0=body,1=text,2=number");
        Job job = createJobBasedOnConfiguration(conf, RegexIngestMapper.class);

        List<String> results = runJobSuccessfully(job, jobInput, 1);

        assertNumDocsProcessed(job, 1);
        assertNotNull("document is null", results.get(0));
    }

    @Test
    public void testBad() throws Exception {
        Configuration conf = getDefaultRegexIngestMapperConfiguration();
        conf.unset(RegexIngestMapper.REGEX);
        assertMapperConfigurationFailsWithMessage(conf, "com.lucidworks.hadoop.ingest.RegexIngestMapper.regex property must not be null or empty");

        conf.set(RegexIngestMapper.REGEX, "");
        assertMapperConfigurationFailsWithMessage(conf, "com.lucidworks.hadoop.ingest.RegexIngestMapper.regex property must not be null or empty");

        conf.set(RegexIngestMapper.REGEX, "\\w+");// good regex, bad group mapping
        conf.set(RegexIngestMapper.GROUPS_TO_FIELDS, "");
        assertMapperConfigurationFailsWithMessage(conf, "com.lucidworks.hadoop.ingest.RegexIngestMapper.groups_to_fields property must not be null or empty");

        conf.set(RegexIngestMapper.REGEX, "\\w+");// good regex, bad group mapping
        conf.set(RegexIngestMapper.GROUPS_TO_FIELDS, "0");
        assertMapperConfigurationFailsWithMessage(conf, "Malformed com.lucidworks.hadoop.ingest.RegexIngestMapper.groups_to_fields");

        conf.set(RegexIngestMapper.GROUPS_TO_FIELDS, "0=foo,1");
        assertMapperConfigurationFailsWithMessage(conf, "Malformed com.lucidworks.hadoop.ingest.RegexIngestMapper.groups_to_fields");

        RegexIngestMapper mapper = new RegexIngestMapper();
        LWDocument[] docs = mapper.toDocuments(null, null, null, null);
        Assert.assertNull(docs);
    }

    @Test
    public void testPathField() throws Exception {
        jobInput.add("text 1 text 2");
        Configuration conf = getDefaultRegexIngestMapperConfiguration();
        conf.set(RegexIngestMapper.REGEX, "\\w+");
        conf.set(RegexIngestMapper.GROUPS_TO_FIELDS, "0=body");
        useAlternativeInputPath("testing/data/altInput");
        Job job = createJobBasedOnConfiguration(conf, RegexIngestMapper.class);

        List<String> results = runJobSuccessfully(job, jobInput, 1);

        assertNumDocsProcessed(job, 1);
        assertNotNull("document is null", results.get(0));
    }


    private void loadFrankensteinDataFromSequenceFile(Configuration conf) throws Exception {
        final String sequenceFilePathSubstring = "sequence" + File.separator + "frankenstein_text_text.seq";
        final URI fullSeqFileUri = RegexIngestMapperTest.class.getClassLoader().getResource(sequenceFilePathSubstring).toURI();
        Path seqFilePath = new Path(fullSeqFileUri);
        SequenceFileIterator<Text, Text> iterator = new SequenceFileIterator<Text, Text>(seqFilePath, true, conf);
        Set<String> ids = new HashSet<String>();
        while (iterator.hasNext()) {
            Pair<Text, Text> pair = iterator.next();
            jobInput.add(pair.getSecond().toString());
        }
    }

    private Configuration getDefaultRegexIngestMapperConfiguration() {
        Configuration conf = getBaseConfiguration();
        conf.set(COLLECTION, "collection");
        conf.set(ZK_CONNECT, "localhost:0000");
        conf.set("idField", "id");
        conf.set(RegexIngestMapper.REGEX, "\\w+");
        conf.set(RegexIngestMapper.GROUPS_TO_FIELDS, "0=body");

        return conf;
    }

    private void assertMapperConfigurationFailsWithMessage(Configuration conf, String expectedMessage) {
        try {
            new RegexIngestMapper().configure(new JobConf(conf));
            Assert.fail();
        } catch (RuntimeException e) {
            Assert.assertTrue("Actual exception message ["+e.getMessage()+"] did not match expected message ["
                    + expectedMessage+"].",
                    e.getMessage().startsWith(expectedMessage));
        }
    }
}
