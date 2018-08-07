package com.lucidworks.hadoop.ingest;

import edu.cmu.lemurproject.WarcFileInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Test;

import java.util.List;

import static com.lucidworks.hadoop.utils.ConfigurationKeys.COLLECTION;
import static com.lucidworks.hadoop.utils.ConfigurationKeys.ZK_CONNECT;

public class WarcIngestMapperTest extends BaseMiniClusterTestCase {

    private static final Path LOCAL_WARC_FILE = new Path(WarcIngestMapperTest.class.getClassLoader().getResource("warc/at.warc").toString());

    @Test
    public void testWarc() throws Exception {
        copyLocalInputToHdfs(LOCAL_WARC_FILE.toUri().toString(), "at.warc");
        Configuration conf = getDefaultWarcIngestMapperConfiguration();
        Job job = createJobBasedOnConfiguration(conf, WarcIngestMapper.class);
        ((JobConf)job.getConfiguration()).setInputFormat(WarcFileInputFormat.class);

        List<String> results = runJobSuccessfully(job, 4);

        assertNumDocsProcessed(job, 4);
        results.get(0).contains("id=<urn:uuid:00fee1bb-1abc-45a6-af31-a164c2fdad88>");
        results.get(1).contains("id=<urn:uuid:b328f1fe-b2ee-45c0-9139-908850810b52>");
        results.get(2).contains("id=<urn:uuid:ccea02fa-a954-4c19-ace2-72a73b04d95d>");
        results.get(3).contains("id=<urn:uuid:f584c023-8703-4551-8952-378427f0333d>");
    }

    private Configuration getDefaultWarcIngestMapperConfiguration() {
        Configuration conf = getBaseConfiguration();
        conf.set("io.serializations", "com.lucidworks.hadoop.io.impl.LWMockSerealization");
        conf.set(COLLECTION, "collection");
        conf.set(ZK_CONNECT, "localhost:0000");
        conf.set("idField", "id");

        return conf;
    }
}
