package com.lucidworks.hadoop.ingest;

import com.lucidworks.hadoop.io.ZipFileInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Test;

import java.util.List;

import static com.lucidworks.hadoop.utils.ConfigurationKeys.COLLECTION;
import static com.lucidworks.hadoop.utils.ConfigurationKeys.ZK_CONNECT;
import static junit.framework.TestCase.assertNotNull;

public class SipsIngestMapperTest extends BaseMiniClusterTestCase {
    @Test
    public void test() throws Exception {
        Configuration conf = getDefaultSipsIngestMapperConfiguration();
        create100EntrySequenceFile(conf);
        Job job = createJobBasedOnConfiguration(conf, SipsIngestMapper.class);
        ((JobConf)job.getConfiguration()).setInputFormat(SequenceFileInputFormat.class);

        final List<String> results = runJobSuccessfully(job,100);

        assertNumDocsProcessed(job, 100);
        for (String docString : results) {
            assertNotNull(docString);
        }
    }

    private void create100EntrySequenceFile(Configuration conf) throws Exception {
        try (SequenceFile.Writer writer = SequenceFile.createWriter(conf, SequenceFile.Writer.file(new Path(INPUT_DIRECTORY_PATH, "sip_info.txt")),
                    SequenceFile.Writer.keyClass(Text.class), SequenceFile.Writer.valueClass(DoubleWritable.class))) {
            for (int i = 0; i < 100; i++) {
                writer.append(new Text("sip_" + i), new DoubleWritable(i / 100.0));
            }
        }
    }

    private Configuration getDefaultSipsIngestMapperConfiguration() {
        Configuration conf = getBaseConfiguration();
        conf.set(COLLECTION, "collection");
        conf.set(ZK_CONNECT, "localhost:0000");
        conf.set("idField", "id");

        return conf;
    }
}
