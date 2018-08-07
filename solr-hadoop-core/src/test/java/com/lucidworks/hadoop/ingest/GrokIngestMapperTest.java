package com.lucidworks.hadoop.ingest;

import com.lucidworks.hadoop.cache.DistributedCacheHandler;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Test;

import java.io.File;
import java.util.List;

import static com.lucidworks.hadoop.utils.ConfigurationKeys.COLLECTION;
import static com.lucidworks.hadoop.utils.ConfigurationKeys.ZK_CONNECT;
import static junit.framework.TestCase.assertNotNull;
import static junit.framework.TestCase.assertTrue;

public class GrokIngestMapperTest extends BaseMiniClusterTestCase {

    private static final String LOCAL_IP_WORD_CONF_LOCATION = GrokIngestMapperTest.class.getClassLoader()
            .getResource("grok" + File.separator + "IP-WORD.conf").getPath();
    private static final String LOCAL_DATE_CONF_LOCATION = GrokIngestMapperTest.class.getClassLoader()
            .getResource("grok" + File.separator + "Month-Day-Year-Greedy.conf").getPath();
    private static final String LOCAL_CISCO_CONF_LOCATION = GrokIngestMapperTest.class.getClassLoader()
            .getResource("grok" + File.separator + "CISCO.conf").getPath();
    private static final String LOCAL_SYSLOG_CONF_LOCATION = GrokIngestMapperTest.class.getClassLoader()
            .getResource("grok" + File.separator + "Syslog.conf").getPath();
    private static final String LOCAL_FIREWALL_CONF_LOCATION = GrokIngestMapperTest.class.getClassLoader()
            .getResource("grok" + File.separator + "firewall.conf").getPath();
    private static final String LOCAL_CUSTOM_CONF_LOCATION = GrokIngestMapperTest.class.getClassLoader()
            .getResource("grok" + File.separator + "customPattern.conf").getPath();
    private static final String LOCAL_EXTRA_PATTERNS_LOCATION = GrokIngestMapperTest.class.getClassLoader()
            .getResource("grok" + File.separator + "extra_patterns.txt").getPath();

    @Test
    public void testIPWORDpattern() throws Exception {
        jobInput.add("192.168.1.1 WORD__1 This is the rest of the message");

        Configuration conf = getDefaultGrokIngestMapperConfiguration();
        Path remotePath = copyLocalResourceToHdfs(LOCAL_IP_WORD_CONF_LOCATION,"IP-WORD.conf");
        conf.set(GrokIngestMapper.GROK_CONFIG_PATH, remotePath.toUri().toString());
        jobCacheUris.add(remotePath.toUri());
        Job job = createJobBasedOnConfiguration(conf, GrokIngestMapper.class);
        DistributedCacheHandler.addFileToCache((org.apache.hadoop.mapred.JobConf)job.getConfiguration(),
                new Path(LOCAL_IP_WORD_CONF_LOCATION), GrokIngestMapper.GROK_CONFIG_PATH);
        ((JobConf)job.getConfiguration()).set(GrokIngestMapper.GROK_URI, new Path(LOCAL_IP_WORD_CONF_LOCATION).toUri().toString());

        List<String> results = runJobSuccessfully(job, jobInput, 1);

        assertNumDocsProcessed(job, 1);
    }

    @Test
    public void testGrokFail() throws Exception {
        jobInput.add("some non-matching string");

        Configuration conf = getDefaultGrokIngestMapperConfiguration();
        Path remotePath = copyLocalResourceToHdfs(LOCAL_IP_WORD_CONF_LOCATION,"IP-WORD.conf");
        conf.set(GrokIngestMapper.GROK_CONFIG_PATH, remotePath.toUri().toString());
        jobCacheUris.add(remotePath.toUri());
        Job job = createJobBasedOnConfiguration(conf, GrokIngestMapper.class);
        DistributedCacheHandler.addFileToCache((org.apache.hadoop.mapred.JobConf)job.getConfiguration(),
                new Path(LOCAL_IP_WORD_CONF_LOCATION), GrokIngestMapper.GROK_CONFIG_PATH);
        ((JobConf)job.getConfiguration()).set(GrokIngestMapper.GROK_URI, new Path(LOCAL_IP_WORD_CONF_LOCATION).toUri().toString());

        List<String> results = runJobSuccessfully(job, jobInput, 1);

        assertNumDocsProcessed(job, 1);
        assertTrue(results.get(0).contains("tags=_grokparsefailure"));
    }

    @Test
    public void testMonthDayYearGreedy() throws Exception {
        jobInput.add("Jan 05 2014 key1=value1 key2=value2 key3=value3");

        Configuration conf = getDefaultGrokIngestMapperConfiguration();
        Path remotePath = copyLocalResourceToHdfs(LOCAL_DATE_CONF_LOCATION,"Month-Day-Year-Greedy.conf");
        conf.set(GrokIngestMapper.GROK_CONFIG_PATH, remotePath.toUri().toString());
        jobCacheUris.add(remotePath.toUri());
        Job job = createJobBasedOnConfiguration(conf, GrokIngestMapper.class);
        DistributedCacheHandler.addFileToCache((org.apache.hadoop.mapred.JobConf)job.getConfiguration(),
                new Path(LOCAL_DATE_CONF_LOCATION), GrokIngestMapper.GROK_CONFIG_PATH);
        ((JobConf)job.getConfiguration()).set(GrokIngestMapper.GROK_URI, new Path(LOCAL_DATE_CONF_LOCATION).toUri().toString());

        List<String> results = runJobSuccessfully(job, jobInput, 1);

        assertNumDocsProcessed(job, 1);
        assertNotNull(results.get(0));
    }

    @Test
    public void testGrokFail2() throws Exception {
        jobInput.add("some non-matching string");

        Configuration conf = getDefaultGrokIngestMapperConfiguration();
        Path remotePath = copyLocalResourceToHdfs(LOCAL_DATE_CONF_LOCATION,"Month-Day-Year-Greedy.conf");
        conf.set(GrokIngestMapper.GROK_CONFIG_PATH, remotePath.toUri().toString());
        jobCacheUris.add(remotePath.toUri());
        Job job = createJobBasedOnConfiguration(conf, GrokIngestMapper.class);
        DistributedCacheHandler.addFileToCache((org.apache.hadoop.mapred.JobConf)job.getConfiguration(),
                new Path(LOCAL_DATE_CONF_LOCATION), GrokIngestMapper.GROK_CONFIG_PATH);
        ((JobConf)job.getConfiguration()).set(GrokIngestMapper.GROK_URI, new Path(LOCAL_DATE_CONF_LOCATION).toUri().toString());

        List<String> results = runJobSuccessfully(job, jobInput, 1);

        assertNumDocsProcessed(job, 1);
        assertTrue(results.get(0).contains("tags=_grokparsefailure"));
    }

    @Test
    public void testCISCOPattern() throws Exception {
        jobInput.add("Mar 20 2014 20:10:45 key1=value1 key2=value2 key3=value3");

        Configuration conf = getDefaultGrokIngestMapperConfiguration();
        Path remotePath = copyLocalResourceToHdfs(LOCAL_CISCO_CONF_LOCATION,"CISCO.conf");
        conf.set(GrokIngestMapper.GROK_CONFIG_PATH, remotePath.toUri().toString());
        jobCacheUris.add(remotePath.toUri());
        Job job = createJobBasedOnConfiguration(conf, GrokIngestMapper.class);
        DistributedCacheHandler.addFileToCache((org.apache.hadoop.mapred.JobConf)job.getConfiguration(),
                new Path(LOCAL_CISCO_CONF_LOCATION), GrokIngestMapper.GROK_CONFIG_PATH);
        ((JobConf)job.getConfiguration()).set(GrokIngestMapper.GROK_URI, new Path(LOCAL_CISCO_CONF_LOCATION).toUri().toString());

        List<String> results = runJobSuccessfully(job, jobInput, 1);

        assertNumDocsProcessed(job, 1);
        assertNotNull(results.get(0));
    }

    @Test
    public void testSyslog() throws Exception {
        jobInput.add("<34>Oct 11 22:14:15 192.168.1.10 su: 'su root' failed for lonvick on /dev/pts/8");

        Configuration conf = getDefaultGrokIngestMapperConfiguration();
        Path remotePath = copyLocalResourceToHdfs(LOCAL_SYSLOG_CONF_LOCATION,"Syslog.conf");
        conf.set(GrokIngestMapper.GROK_CONFIG_PATH, remotePath.toUri().toString());
        jobCacheUris.add(remotePath.toUri());
        Job job = createJobBasedOnConfiguration(conf, GrokIngestMapper.class);
        DistributedCacheHandler.addFileToCache((org.apache.hadoop.mapred.JobConf)job.getConfiguration(),
                new Path(LOCAL_SYSLOG_CONF_LOCATION), GrokIngestMapper.GROK_CONFIG_PATH);
        ((JobConf)job.getConfiguration()).set(GrokIngestMapper.GROK_URI, new Path(LOCAL_SYSLOG_CONF_LOCATION).toUri().toString());

        List<String> results = runJobSuccessfully(job, jobInput, 1);

        assertNumDocsProcessed(job, 1);
        assertNotNull(results.get(0));
    }

    @Test
    public void testFirewall() throws Exception {
        jobInput.add("Mar 31 2014 18:02:36: %ASA-5-106100: access-list inbound denied tcp outside/128.241.220.82(3154) -> asp3/62.84.96.19(32005) hit-cnt 1 first hit [0x91c26a3, 0x0]");

        Configuration conf = getDefaultGrokIngestMapperConfiguration();
        Path remotePath = copyLocalResourceToHdfs(LOCAL_FIREWALL_CONF_LOCATION,"firewall.conf");
        conf.set(GrokIngestMapper.GROK_CONFIG_PATH, remotePath.toUri().toString());
        jobCacheUris.add(remotePath.toUri());
        Job job = createJobBasedOnConfiguration(conf, GrokIngestMapper.class);
        DistributedCacheHandler.addFileToCache((org.apache.hadoop.mapred.JobConf)job.getConfiguration(),
                new Path(LOCAL_FIREWALL_CONF_LOCATION), GrokIngestMapper.GROK_CONFIG_PATH);
        ((JobConf)job.getConfiguration()).set(GrokIngestMapper.GROK_URI, new Path(LOCAL_FIREWALL_CONF_LOCATION).toUri().toString());

        List<String> results = runJobSuccessfully(job, jobInput, 1);

        assertNumDocsProcessed(job, 1);
        assertNotNull(results.get(0));
    }

    @Test
    public void testAdditionalPattern() throws Exception {
        //TODO This test was temporarily removed when switching away from mrunit, as it's hard to simulate without the
        // direct control that mrunit afforded us.  We need to come up with an alternative way to test
        // this functionality
    }

    @Test
    public void testByteOffsetField() {
        //TODO This test was temporarily removed when switching away from mrunit, as it's hard to simulate without the
        // direct control that mrunit afforded us.  We need to come up with an alternative way to test
        // this functionality
    }

    private Configuration getDefaultGrokIngestMapperConfiguration() throws Exception {
        Configuration conf = getBaseConfiguration();
        conf.set("io.serializations", "com.lucidworks.hadoop.io.impl.LWMockSerealization");
        conf.set(COLLECTION, "collection");
        conf.set(ZK_CONNECT, "localhost:0000");
        conf.set("idField", "id");

        return conf;
    }
}
