package com.lucidworks.hadoop.ingest;

import com.lucidworks.hadoop.io.LWDocumentWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.test.PathUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class BaseMiniClusterTestCase {

    protected static final Path DEFAULT_INPUT_DIRECTORY_PATH = new Path("testing/data/input");
    protected static Path INPUT_DIRECTORY_PATH = DEFAULT_INPUT_DIRECTORY_PATH;
    protected static final Path OUTPUT_DIRECTORY_PATH = new Path("testing/data/output");
    protected static final Path RESOURCE_DIRECTORY_PATH = new Path("testing/data/resources");
    protected static final String CLUSTER_NAME = "cluster1";
    protected static final File testDataPath = new File(PathUtils.getTestDir(BaseMiniClusterTestCase.class), "miniclusters");

    protected static Configuration clusterConf;
    protected static MiniDFSCluster cluster;
    protected static FileSystem fs;

    protected List<String> jobInput;
    protected List<URI> jobCacheUris;

    @BeforeClass
    public static void setUpCluster() throws Exception {
        System.clearProperty(MiniDFSCluster.PROP_TEST_BUILD_DATA);
        clusterConf = new HdfsConfiguration();

        File testDataCluster1 = new File(testDataPath, CLUSTER_NAME);
        String c1Path = testDataCluster1.getAbsolutePath();
        clusterConf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, c1Path);
        cluster = new MiniDFSCluster.Builder(clusterConf).build();
        cluster.waitClusterUp();

        fs = FileSystem.get(clusterConf);
    }

    @AfterClass
    public static void tearDownCluster() throws Exception {
        Path dataDir = new Path(testDataPath.getParentFile().getParentFile().getParent());
        fs.delete(dataDir, true);
        File rootTestFile = new File(testDataPath.getParentFile().getParentFile().getParent());
        String rootTestDir = rootTestFile.getAbsolutePath();
        Path rootTestPath = new Path(rootTestDir);
        LocalFileSystem localFileSystem = FileSystem.getLocal(clusterConf);
        localFileSystem.delete(rootTestPath, true);
        cluster.shutdown();
    }


    @Before
    public void setUp() throws Exception {
        fs.delete(DEFAULT_INPUT_DIRECTORY_PATH, true);
        fs.delete(INPUT_DIRECTORY_PATH, true);
        fs.delete(OUTPUT_DIRECTORY_PATH, true);

        jobInput = new ArrayList<>();
        jobCacheUris = new ArrayList<>();
    }

    @After
    public void tearDown() throws Exception {
        fs.delete(DEFAULT_INPUT_DIRECTORY_PATH, true);
        fs.delete(INPUT_DIRECTORY_PATH, true);
        fs.delete(OUTPUT_DIRECTORY_PATH, true);

        INPUT_DIRECTORY_PATH = DEFAULT_INPUT_DIRECTORY_PATH;
    }

    protected Configuration getBaseConfiguration() {
        return new HdfsConfiguration(clusterConf);
    }

    protected Job createJobBasedOnConfiguration(Configuration baseConfiguration, Class mapperClazz) throws Exception {
        JobConf jobConf = new JobConf(baseConfiguration);
        jobConf.setMapperClass(mapperClazz);
        FileInputFormat.addInputPath(jobConf, INPUT_DIRECTORY_PATH);
        FileOutputFormat.setOutputPath(jobConf, OUTPUT_DIRECTORY_PATH);
        Job job = Job.getInstance(jobConf);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LWDocumentWritable.class);

        return job;
    }

    protected void withJobInput(List<String> jobInput) throws IOException {
        writeHDFSContent(fs, INPUT_DIRECTORY_PATH, "sample.txt", jobInput);
    }

    protected List<String> runJobSuccessfully(Job job, List<String> input, int expectedLines) throws Exception {
        fs.delete(INPUT_DIRECTORY_PATH, true);
        fs.delete(OUTPUT_DIRECTORY_PATH, true);

        withJobInput(input);
        if (! jobCacheUris.isEmpty()) {
            job.setCacheFiles(cloneCacheUriList());
        }
        job.waitForCompletion(true);
        assertTrue(job.isSuccessful());

        return getJobResults(expectedLines);
    }

    protected List<String> runJobSuccessfully(Job job, int expectedLines) throws Exception {
        job.waitForCompletion(true);
        assertTrue(job.isSuccessful());
        return getJobResults(expectedLines);
    }

    private void writeHDFSContent(FileSystem fs, Path dir, String fileName, List<String> content) throws IOException {
        Path newFilePath = new Path(dir, fileName);
        FSDataOutputStream out = fs.create(newFilePath);
        for (String line : content){
            out.writeBytes(line);
            out.writeBytes("\n");
        }
        out.close();
    }

    protected Path copyLocalResourceToHdfs(String localPath, String remoteFilename) throws Exception {
        return copyLocalFileToHdfs(localPath, RESOURCE_DIRECTORY_PATH, remoteFilename);
    }

    protected Path copyLocalInputToHdfs(String localPath, String remoteFilename) throws Exception {
        return copyLocalFileToHdfs(localPath, INPUT_DIRECTORY_PATH, remoteFilename);
    }

    protected Path copyLocalFileToHdfs(String localPath, Path remotePath, String remoteFilename) throws Exception {
        fs.mkdirs(RESOURCE_DIRECTORY_PATH);

        final Path remotePathWithFilename = new Path(remotePath, remoteFilename);
        fs.copyFromLocalFile(new Path(localPath), remotePathWithFilename);

        return remotePathWithFilename;
    }

    protected List<String> getJobResults(int expectedLines) throws Exception {
        List<String> results = new ArrayList<String>();
        FileStatus[] fileStatus = fs.listStatus(OUTPUT_DIRECTORY_PATH);
        for (FileStatus file : fileStatus) {
            String name = file.getPath().getName();
            if (name.contains("part-r-00000")){
                Path filePath = new Path(OUTPUT_DIRECTORY_PATH + "/" + name);
                BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(filePath)));
                for (int i=0; i < expectedLines; i++){
                    String line = reader.readLine();
                    if (line == null){
                        fail("Expected [" + expectedLines + "] of output but only found [" + i + "]");
                    }
                    results.add(line);
                }
                assertNull(reader.readLine());
                reader.close();
            }
        }
        return results;
    }

    protected String createExpectedDocStrWithFields(String idValue, String ... fields) {
        if (fields.length % 2 != 0) {
            fail("Expected field-value String pairs, but received an odd number of String inputs");
        }

        final StringBuilder sb = new StringBuilder(idValue + "\tLWDocumentWritable{document=SolrInputDocument(fields: [");
        for (int i = 0; i < fields.length; i+=2) {
            sb.append(fields[i] + "=" + fields[i+1]);
            if (i < fields.length - 2) { // if we're not the last field-value pair
                sb.append(", ");
            }
        }
        sb.append("])}");
        return sb.toString();
    }

    protected void assertNumDocsProcessed(Job job, int expectedNumDocs) throws Exception {
        assertEquals("Expected [" + expectedNumDocs + "]", expectedNumDocs,
                job.getCounters().findCounter(BaseHadoopIngest.Counters.DOCS_ADDED).getValue());
    }

    private URI[] cloneCacheUriList() {
        final URI[] uris = new URI[jobCacheUris.size()];
        for(int i = 0; i < jobCacheUris.size(); i++) {
            uris[i] = jobCacheUris.get(i);
        }

        return uris;
    }
}
