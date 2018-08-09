package com.lucidworks.hadoop.ingest;

import com.lucidworks.hadoop.io.LWDocumentWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import static com.lucidworks.hadoop.utils.ConfigurationKeys.*;
import static junit.framework.Assert.assertEquals;
import static junit.framework.TestCase.assertNotNull;


public class DirectoryIngestMapperTest extends BaseMiniClusterTestCase {
    private transient static Logger log = LoggerFactory.getLogger(DirectoryIngestMapperTest.class);

    private Configuration conf;
    private JobConf jobConf;
    private int tempFiles;

    @Before
    public void setUp() throws Exception {
        conf = getDefaultDirectoryIngestMapperConfiguration();
        Path dir = new Path(fs.getWorkingDirectory(), "build");
        Path sub = new Path(dir, "DIMT");
        Path tempDir = new Path(sub, "tmp-dir");
        Path seqDir = new Path(sub, "seq-dir");// this is the location where the
        // fixture will write inputs.seq
        fs.mkdirs(tempDir);
        tempFiles = setupDir(fs, tempDir);
        conf.set(TEMP_DIR, seqDir.toString());
        jobConf = new JobConf(conf);
        jobConf.setMapperClass(DirectoryIngestMapper.class);
        jobConf.setInputFormat(SequenceFileInputFormat.class);
        FileOutputFormat.setOutputPath(jobConf, OUTPUT_DIRECTORY_PATH);
        org.apache.hadoop.mapred.FileInputFormat.setInputPaths(jobConf, new Path(tempDir, "*"));
        Path[] paths = org.apache.hadoop.mapred.FileInputFormat.getInputPaths(jobConf);
        assertEquals(1, paths.length);
    }

    @Test
    public void testDir() throws Exception {
        jobConf.set(DirectoryIngestMapper.DIRECTORY_ADD_SUBDIRECTORIES, "false");
        doTest(tempFiles - 4);  // The 4 subdirectories should be ignored
    }

    @Test
    public void testSubDir() throws Exception {
        jobConf.set(DirectoryIngestMapper.DIRECTORY_ADD_SUBDIRECTORIES, "true");
        doTest(tempFiles);
    }

    private Configuration getDefaultDirectoryIngestMapperConfiguration() {
        Configuration conf = getBaseConfiguration();
        conf.set(COLLECTION, "collection");
        conf.set(ZK_CONNECT, "localhost:0000");
        conf.set("idField", "id");

        return conf;
    }

    private void doTest(int expectedNumDocs) throws Exception {
        new DirectoryIngestMapper().getFixture().init(jobConf);
        Job job = Job.getInstance(jobConf);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LWDocumentWritable.class);

        List<String> results = runJobSuccessfully(job, expectedNumDocs);

        assertNumDocsProcessed(job, expectedNumDocs);
        for (String docStr : results) {
            assertNotNull(docStr);
        }
    }

    private int setupDir(FileSystem fs, Path base) throws URISyntaxException, IOException {
        fs.mkdirs(base);
        int count = 0;
        List<Path> paths = new ArrayList<Path>();
        for (int i = 0; i < 6; i++, count++) {
            Path path = new Path(DirectoryIngestMapperTest.class.getClassLoader()
                    .getResource("dir" + File.separator + "frank_txt_" + i + ".txt").toURI());
            Path dst = new Path(base, "frank_txt_" + i + ".txt");
            paths.add(dst);
            fs.copyFromLocalFile(path, dst);
        }
        for (int i = 0; i < 3; i++, count++) {
            Path path = new Path(DirectoryIngestMapperTest.class.getClassLoader()
                    .getResource("dir" + File.separator + "test" + i + ".pdf").toURI());
            Path dst = new Path(base, "test" + i + ".pdf");
            paths.add(dst);
            fs.copyFromLocalFile(path, dst);
        }
        for (int i = 0; i < 2; i++, count++) {
            Path path = new Path(DirectoryIngestMapperTest.class.getClassLoader()
                    .getResource("dir" + File.separator + "test" + i + ".doc").toURI());
            Path dst = new Path(base, "test" + i + ".doc");
            paths.add(dst);
            fs.copyFromLocalFile(path, dst);
        }
        Path subDirA = new Path(base, "subDirA");
        for (int i = 0; i < 2; i++, count++) {// Subdirectories A
            Path path = new Path(DirectoryIngestMapperTest.class.getClassLoader()
                    .getResource("dir" + File.separator + "test" + i + ".doc").toURI());
            Path dst = new Path(subDirA, "test" + i + ".doc");
            paths.add(dst);
            fs.copyFromLocalFile(path, dst);
        }
        Path subDirB = new Path(base, "subDirB");
        for (int i = 0; i < 2; i++, count++) {// Subdirectories B
            Path path = new Path(DirectoryIngestMapperTest.class.getClassLoader()
                    .getResource("dir" + File.separator + "test" + i + ".doc").toURI());
            Path dst = new Path(subDirB, "test" + i + ".pdf");
            paths.add(dst);
            fs.copyFromLocalFile(path, dst);
        }
        for (Path path : paths) {
            FileStatus fileStatus = fs.getFileStatus(path);
            System.out.println("Status: " + fileStatus.getPath());
        }
        return count;
    }
}
