package com.lucidworks.hadoop.ingest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Test;

import java.io.File;
import java.util.List;

import static com.lucidworks.hadoop.utils.ConfigurationKeys.COLLECTION;
import static com.lucidworks.hadoop.utils.ConfigurationKeys.ZK_CONNECT;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNotNull;

public class SequenceFileIngestMapperTest extends BaseMiniClusterTestCase {

    private static final Path LOCAL_FRANKENSTEIN_SEQ_FILE = new Path(SequenceFileIngestMapperTest.class.getClassLoader()
            .getResource("sequence" + File.separator + "frankenstein_text_text.seq").toString());

    @Test
    public void test() throws Exception {
        prepareFrankensteinSeqFileInput();
        Configuration conf = getDefaultSequenceFileIngestMapperConfiguration();
        Job job = createJobBasedOnConfiguration(conf, SequenceFileIngestMapper.class);
        ((JobConf)job.getConfiguration()).setInputFormat(SequenceFileInputFormat.class);

        List<String> results = runJobSuccessfully(job, 776);

        assertNumDocsProcessed(job, 776);
        assertEquals(776, results.size());
        for (String docStr : results) {
            assertNotNull(docStr);
        }

    }

    private void prepareFrankensteinSeqFileInput() throws Exception {
        copyLocalInputToHdfs(LOCAL_FRANKENSTEIN_SEQ_FILE.toUri().toString(), "frankenstein_text_text.seq");
    }

    private Configuration getDefaultSequenceFileIngestMapperConfiguration() {
        Configuration conf = getBaseConfiguration();
        conf.set("io.serializations", "com.lucidworks.hadoop.io.impl.LWMockSerealization");
        conf.set(COLLECTION, "collection");
        conf.set(ZK_CONNECT, "localhost:0000");
        conf.set("idField", "id");

        return conf;
    }
}
