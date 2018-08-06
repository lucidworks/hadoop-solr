package com.lucidworks.hadoop.ingest;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;

import static com.lucidworks.hadoop.utils.ConfigurationKeys.COLLECTION;
import static com.lucidworks.hadoop.utils.ConfigurationKeys.ZK_CONNECT;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNotNull;
import static org.junit.Assert.assertFalse;

public class CSVIngestMapperTest extends BaseMiniClusterTestCase {

    @Test
    public void testParsesLinesAsIndividualDocuments() throws Exception {
        jobInput.add("id,bar,junk,zen,hockey"); // Skipped because of first line ignore setting
        jobInput.add("id-1, The quick brown fox, jumped, head, gretzky, extra");

        Configuration conf = getDefaultCSVMapperConfiguration();
        Job job = createJobBasedOnConfiguration(conf, CSVIngestMapper.class);

        runJobSuccessfully(job, jobInput, 1);
        assertEquals("Should be 1 documents", 1, job.getCounters().findCounter(BaseHadoopIngest.Counters.DOCS_ADDED).getValue());
        final List<String> documentLines = getJobResults(1);
        final String expectedDocStr = createExpectedDocStrWithFields("id-1", "id", "id-1",
                "bar", "The quick brown fox",
                "junk", "jumped",
                "zen", "head",
                "hockey", "gretzky",
                "field_5", "extra");
        assertEquals(expectedDocStr, documentLines.get(0));
    }

    @Test
    public void testLWS592() throws Exception {
        // Input has ctrl+A delimiters
        jobInput.add("idbarjunk"); // Skipped because of first-line-ignore setting
        jobInput.add("id-1The quick brown foxhead");

        Configuration conf = getDefaultCSVMapperConfiguration();
        byte[] delimiterBase64 = Base64.encodeBase64("\u0001".getBytes());
        conf.set(CSVIngestMapper.CSV_DELIMITER, new String(delimiterBase64));
        conf.set(CSVIngestMapper.CSV_FIELD_MAPPING, "0=id,1=bar,2=junk");

        Job job = createJobBasedOnConfiguration(conf, CSVIngestMapper.class);
        final List<String> results = runJobSuccessfully(job, jobInput, 1);

        assertEquals("Should be 1 document", 1, job.getCounters().findCounter(BaseHadoopIngest.Counters.DOCS_ADDED).getValue());

        final String expectedDocStr = createExpectedDocStrWithFields("id-1", "id", "id-1", "bar", "The quick brown fox", "junk", "head");
        assertEquals(expectedDocStr, results.get(0));
    }

    @Test
    public void testDefaultFieldId() throws Exception {
        jobInput.add("bar, id,junk,zen,hockey"); // Skipped because of first-line-ignore setting
        jobInput.add("The quick brown fox, id-1, jumped, head, gretzky, extra");


        Configuration conf = getDefaultCSVMapperConfiguration();
        conf.set(CSVIngestMapper.CSV_FIELD_MAPPING, "0=bar, 1=id, 2=junk, 3=zen, 4 = hockey");
        Job job = createJobBasedOnConfiguration(conf, CSVIngestMapper.class);
        final List<String> results = runJobSuccessfully(job, jobInput, 1);

        assertEquals("Should be 1 document", 1, job.getCounters().findCounter(BaseHadoopIngest.Counters.DOCS_ADDED).getValue());
        final String expectedDocStr = createExpectedDocStrWithFields("id-1", "id", "id-1",
                "bar", "The quick brown fox",
                "junk", "jumped",
                "zen", "head",
                "hockey", "gretzky",
                "field_5", "extra");
        assertEquals(expectedDocStr, results.get(0));
    }

    @Test
    public void testFieldId() throws Exception {
        jobInput.add("bar, id,junk,zen,hockey"); // Skipped because of first line ignore
        jobInput.add("The quick brown fox, jumped, id-1, head, gretzky, extra");

        Configuration conf = getDefaultCSVMapperConfiguration();
        // The "id" is in a different position and has a diferent name my-id
        conf.set(CSVIngestMapper.CSV_FIELD_MAPPING, "0=bar, 1=junk, 2=my-id, 3=zen, 4 = hockey");
        conf.set("idField", "my-id");

        Job job = createJobBasedOnConfiguration(conf, CSVIngestMapper.class);
        final List<String> results = runJobSuccessfully(job, jobInput, 1);

        assertEquals("Should be 1 document", 1, job.getCounters().findCounter(BaseHadoopIngest.Counters.DOCS_ADDED).getValue());
        final String doc = results.get(0);
        final String expectedDocStr = createExpectedDocStrWithFields("id-1","id", "id-1",
                "bar", "The quick brown fox",
                "junk", "jumped",
                "zen", "head",
                "hockey", "gretzky",
                "field_5", "extra");
        assertEquals(expectedDocStr, doc);
    }

    @Test
    public void testWithoutFieldId() throws Exception {
        jobInput.add("bar, id,junk,zen,hockey");// we skip the 0th line
        jobInput.add("The quick brown fox, id-1, jumped, head, gretzky, extra");

        Configuration conf = getDefaultCSVMapperConfiguration();
        // The "id" is in a different position
        conf.set(CSVIngestMapper.CSV_FIELD_MAPPING, "0=bar, 1=id, 2=junk, 3=zen, 4 = hockey");
        // Set another field to be the id
        conf.set("idField", "junk");

        Job job = createJobBasedOnConfiguration(conf, CSVIngestMapper.class);
        final List<String> results = runJobSuccessfully(job, jobInput, 1);

        assertEquals("Should be 1 document", 1, job.getCounters().findCounter(BaseHadoopIngest.Counters.DOCS_ADDED).getValue());
        final String doc = results.get(0);
        final String expectedDocStr = createExpectedDocStrWithFields("jumped","id", "jumped",
                "bar", "The quick brown fox",
                "zen", "head",
                "hockey", "gretzky",
                "field_5", "extra");
        assertEquals(expectedDocStr, doc);
    }

    @Test
    public void testOptions() throws Exception {
        jobInput.add("id-0|bar|junk|zen|hockey");
        jobInput.add("id-1|The quick brown fox|jumped|head|gretzky|extra");

        Configuration conf = getDefaultCSVMapperConfiguration();
        byte[] delimiterBase64 = Base64.encodeBase64("|".getBytes());
        conf.set(CSVIngestMapper.CSV_DELIMITER, new String(delimiterBase64));
        conf.set(CSVIngestMapper.CSV_IGNORE_FIRST_LINE_COMMENT, "false");
        Job job = createJobBasedOnConfiguration(conf, CSVIngestMapper.class);
        final List<String> results = runJobSuccessfully(job, jobInput, 2);

        assertEquals("Should be 2 documents", 2, job.getCounters().findCounter(BaseHadoopIngest.Counters.DOCS_ADDED).getValue());
        final String doc1 = results.get(0);
        final String doc2 = results.get(1);

        final String expectedDoc1Str = createExpectedDocStrWithFields("id-0", "id", "id-0", "bar", "bar", "junk", "junk", "zen", "zen", "hockey",
                "hockey");
        assertEquals(expectedDoc1Str, doc1);

        final String expectedDoc2Str = createExpectedDocStrWithFields("id-1", "id", "id-1", "bar", "The quick brown fox", "junk", "jumped", "zen",
                "head", "hockey", "gretzky", "field_5", "extra");
        assertEquals(expectedDoc2Str, doc2);
    }

    @Test
    public void testJobFailsWithoutCollection() throws Exception {
        jobInput.add("id,bar,junk,zen,hockey"); // Skipped because of first line ignore setting
        jobInput.add("id-1, The quick brown fox, jumped, head, gretzky, extra");
        withJobInput(jobInput);

        Configuration conf = getDefaultCSVMapperConfiguration();
        conf.unset(COLLECTION);
        Job job = createJobBasedOnConfiguration(conf, CSVIngestMapper.class);
        job.waitForCompletion(true);

        assertFalse(job.isSuccessful());
    }

    @Test
    public void testStrategy() throws Exception {
        jobInput.add("id-0,bar,junk,zen,hockey");

        Configuration conf = getDefaultCSVMapperConfiguration();
        conf.set(CSVIngestMapper.CSV_IGNORE_FIRST_LINE_COMMENT, "false");
        conf.set(CSVIngestMapper.CSV_DELIMITER, ",");
        conf.set(CSVIngestMapper.CSV_FIELD_MAPPING, "0=id,1=count,2=body, 3=title,4=footer");
        conf.set(CSVIngestMapper.CSV_STRATEGY, CSVIngestMapper.EXCEL_STRATEGY);

        Job job = createJobBasedOnConfiguration(conf, CSVIngestMapper.class);
        final List<String> results = runJobSuccessfully(job, jobInput, 1);

        final String expectedDocStr = createExpectedDocStrWithFields("id-0", "id", "id-0", "count", "bar", "body", "junk", "title", "zen", "footer", "hockey");
        assertEquals(expectedDocStr, results.get(0));
    }

    @Test
    public void testFrankenstein() throws Exception {
        final int numLines = addFrankensteinDataToJobInput();
        final int expectedDocuments = numLines - 1;  // We ignore the first line as a comment

        Configuration conf = getDefaultCSVMapperConfiguration();
        conf.set(CSVIngestMapper.CSV_FIELD_MAPPING, "0=id,1=count,2=body, 3=title,4=footer");

        Job job = createJobBasedOnConfiguration(conf, CSVIngestMapper.class);
        final List<String> results = runJobSuccessfully(job, jobInput, expectedDocuments);

        assertEquals("Should be " + expectedDocuments + " documents", expectedDocuments,
                job.getCounters().findCounter(BaseHadoopIngest.Counters.DOCS_ADDED).getValue());
    }

    private int addFrankensteinDataToJobInput() throws Exception {
        InputStream frank = CSVIngestMapperTest.class.getClassLoader()
                .getResourceAsStream("csv" + File.separator + "frank.csv");
        assertNotNull("frank is null", frank);
        BufferedReader reader = new BufferedReader(new InputStreamReader(frank));
        String line = null;
        int i = 0;
        while ((line = reader.readLine()) != null) {
            String[] splits = line.split(",");
            String id = splits[0];
            LongWritable lineNumb = new LongWritable(i);
            jobInput.add(line);
            i++;
        }

        return i;
    }

    private Configuration getDefaultCSVMapperConfiguration() {
        Configuration conf = getBaseConfiguration();
        conf.set(CSVIngestMapper.CSV_IGNORE_FIRST_LINE_COMMENT, "true");
        byte[] delimiterBase64 = Base64.encodeBase64(",".getBytes());
        conf.set(CSVIngestMapper.CSV_DELIMITER, new String(delimiterBase64));
        conf.set(CSVIngestMapper.CSV_FIELD_MAPPING, "0=id,1=bar, 2=junk , 3 = zen ,   4 = hockey");
        conf.set(COLLECTION, "collection");
        conf.set(ZK_CONNECT, "localhost:0000");
        conf.set("idField", "id");

        return conf;
    }
}
