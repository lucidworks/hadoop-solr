package com.lucidworks.hadoop.ingest;

import com.lucidworks.hadoop.io.LWDocumentWritable;
import com.lucidworks.hadoop.io.LWMapRedOutputFormat;
import com.lucidworks.hadoop.io.LucidWorksWriter;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.AbstractJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import static com.lucidworks.hadoop.utils.ConfigurationKeys.COLLECTION;
import static com.lucidworks.hadoop.utils.ConfigurationKeys.MIME_TYPE;
import static com.lucidworks.hadoop.utils.ConfigurationKeys.OVERWRITE;
import static com.lucidworks.hadoop.utils.ConfigurationKeys.SOLR_SERVER_URL;
import static com.lucidworks.hadoop.utils.ConfigurationKeys.TEMP_DIR;
import static com.lucidworks.hadoop.utils.ConfigurationKeys.ZK_CONNECT;

public class IngestJob extends AbstractJob {
  private static final Logger log = LoggerFactory.getLogger(IngestJob.class);

  public static final String COLLECTION_OPTION = "collection";
  public static final String MIME_TYPE_OPTION = "mimeType";
  public static final String ZK_CONNECT_OPTION = "zkConnect";
  public static final String MAPPER_OPTION = "mapperClass";
  public static final String REDUCER_OPTION = "reducerClass";
  public static final String OVERWRITE_OPTION = "overwrite";
  public static final String INPUT_FORMAT = "inputFormat";
  public static final String SOLR_SERVER_OPTION = "solrServer";
  public static final String NUM_REDUCERS_OPTION = "numReducers";

  public static final String OUTPUT_FORMAT = "outputFormat";
  public static final String DEFAULT_MAPPER_CLASS = DirectoryIngestMapper.class.getCanonicalName();
  public static final String INPUT_FORMAT_OVERRIDE = "inputFormatOverride";
  public static final String OUTPUT_FORMAT_OVERRIDE = "outputFormatOverride";
  public static final String CONF_OPTION = "conf";
  public static final String JOB_NAME_OPTION = "name";

  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new IngestJob(), args));
  }

  @Override
  public int run(String[] args) throws Exception {
    addInputOption();
    addOutputOption();
    addOption(JOB_NAME_OPTION, "jn", "Optional name to give the Hadoop Job.", false);
    addOption(ZK_CONNECT_OPTION, "zk",
        "Connection string for ZooKeeper. Either Solr Server or ZK must be specified.", false);
    addOption(SOLR_SERVER_OPTION, "s",
        "The URL for Solr, when not in SolrCloud.  Either Solr Server or ZK must be specified.",
        false);
    addOption(COLLECTION_OPTION, "c", "Collection name", true);
    addOption(MIME_TYPE_OPTION, "mt", "a mimeType known to Tika", false);
    addOption(CONF_OPTION, "con",
        "Key[value] pairs, separated by ; to be added to the Conf.  The value is set as a string.",
        false);
    addOption(MAPPER_OPTION, "cls",
        "Fully qualified class name for the IngestMapper to use (derived from AbstractIngestMapper)",
        false);
    addOption(REDUCER_OPTION, "redcls",
        "Fully qualified class name for your custom IngestReducer.  You must also set "
            + NUM_REDUCERS_OPTION + " > 0 for this to be invoked.", false);
    addOption(OVERWRITE_OPTION, "ov", "Overwrite any existing documents", "true");
    addOption(INPUT_FORMAT, "if",
        "The Hadoop InputFormat type to use.  Must be a fully qualified class name and the class must be available on the classpath. "
            + " Note: Some Mapper Classes may ignore this.  Also note, your Input Format must be able to produce the correct kind of records for the mapper.");
    addOption(OUTPUT_FORMAT, "of",
        "The Hadoop OutputFormat type to use.  Must be a fully qualified class name and the class must be available on the classpath. "
            + " Note: Some Mapper Classes may ignore this.  Also note, your Output Format must be able to handle <Text, DocumentWritable>.");
    addOption(NUM_REDUCERS_OPTION, "ur",
        "An Integer >= 0 indicating the number of Reducers to use when outputing to the OutputFormat.  "
            + "Depending on the OutputFormat and your system resources, you may wish to have Hadoop do a reduce step first so as to not open too many connections to the output resource.  Default is to not use any reducers.  Note, ");

    Map<String, List<String>> map = parseArguments(args, false, true);
    if (map == null) {
      return 1;
    }

    Path input = getInputPath();
    Path output = getOutputPath();
    JobConf conf = new JobConf(getConf(), getClass());
    conf.setJarByClass(IngestJob.class);
    conf.setJobName(getOption(JOB_NAME_OPTION, IngestJob.class.getName()));
    String zk = getOption(ZK_CONNECT_OPTION);
    String solr = getOption(SOLR_SERVER_OPTION);

    // Pass through CLI args to JobConf
    conf.set(COLLECTION, getOption(COLLECTION_OPTION));
    conf.set(LucidWorksWriter.SOLR_COLLECTION,
        getOption(COLLECTION_OPTION));//TODO: consolidate this redundancy
    conf.set(MIME_TYPE, getOption(MIME_TYPE_OPTION, ""));
    conf.set(TEMP_DIR, getTempPath().toString());
    boolean overwrite = Boolean.parseBoolean(getOption(OVERWRITE_OPTION, "true"));
    conf.set(OVERWRITE, overwrite ? "true" : "false");

    //Handle additional conf parameters
    String confAdds = getOption(CONF_OPTION);
    if (confAdds != null && !confAdds.isEmpty()) {
      processConf(confAdds, conf);
    }

    // Basic M/R config
    conf.setMapOutputKeyClass(Text.class);
    conf.setMapOutputValueClass(LWDocumentWritable.class);
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(LWDocumentWritable.class);
    int numReducers = Integer.parseInt(getOption(NUM_REDUCERS_OPTION, "0"));
    conf.setNumReduceTasks(numReducers);
    //Do not change this without a very good reason, otherwise you will end up indexing content multiple times
    conf.set("mapred.map.tasks.speculative.execution", "false");
    conf.set("mapred.reduce.tasks.speculative.execution", "false");

    conf.set("mapreduce.reduce.speculative", "false");
    conf.set("mapreduce.map.speculative", "false");

    FileInputFormat.setInputPaths(conf, input);
    if (output != null) {
      FileOutputFormat.setOutputPath(conf, output);
    }

    // Instantiate the Mapper class
    String mapperClass = getOption(MAPPER_OPTION, DEFAULT_MAPPER_CLASS);
    Class<AbstractIngestMapper> mapperClazz;
    AbstractIngestMapper mapper;
    try {
      mapperClazz = (Class<AbstractIngestMapper>) Class.forName(mapperClass);
      mapper = mapperClazz.newInstance();
    } catch (Exception e) {
      System.out.println("Unable to instantiate AbstractIngestMapper class");
      log.error("Unable to instantiate AbstractIngestMapper class " + mapperClass, e);
      return 1;
    }
    conf.setMapperClass(mapperClazz);

    // Let the mapper configure itself
    String inputFormatName = getOption(INPUT_FORMAT);
    if (inputFormatName != null) {
      //if the user passed in an InputFormat, than override what the mapper set.
      Class<? extends InputFormat> ifClass = Class.forName(inputFormatName)
          .asSubclass(InputFormat.class);
      conf.setInputFormat(ifClass);
      conf.setBoolean(INPUT_FORMAT_OVERRIDE, true);
    }

    // Instantiate the Reducer class
    String reducerClass = getOption(REDUCER_OPTION, IngestReducer.class.getName());
    IngestReducer reducer = null;
    //Only worry about reducers if the user actually wants them
    if (numReducers > 0 && reducerClass != null) {
      Class<IngestReducer> reducerClazz;
      try {
        reducerClazz = (Class<IngestReducer>) Class.forName(reducerClass);
        reducer = reducerClazz.newInstance();//just make sure it can be created now
      } catch (Exception e) {
        System.out.println("Unable to instantiate IngestReducer class");
        log.error("Unable to instantiate IngestReducer class " + reducerClass, e);
        return 1;
      }
      conf.setReducerClass(reducerClazz);
    }

    String outputFormatName = getOption(OUTPUT_FORMAT);
    if (outputFormatName == null || outputFormatName.equals(LWMapRedOutputFormat.class.getName())) {
      log.info("Using default OutputFormat: {}", LWMapRedOutputFormat.class.getName());
      conf.setOutputFormat(LWMapRedOutputFormat.class);
      if (zk != null && !zk.isEmpty()) {
        conf.set(ZK_CONNECT, zk);
        conf.set(LucidWorksWriter.SOLR_ZKHOST, zk);
      } else if (solr != null && !solr.isEmpty()) {
        conf.set(SOLR_SERVER_URL, solr);
        conf.set(LucidWorksWriter.SOLR_SERVER_URL, solr);
      } else {
        System.out.println("You must specify either the " + ZK_CONNECT_OPTION + " or the " + SOLR_SERVER_OPTION);
        return 1;
      }
      // Checking Solr server before job execution
      if (!checkSolrServer(conf)) {
        System.out.println("Solr server not available on: " + (zk == null || zk.isEmpty() ? solr : zk));
        System.out.println("Make sure that collection [" + getOption(COLLECTION_OPTION) + "] exists");
        return 1;
      }
    } else {
      //if the user passed in an OutputFormat, than override what the mapper set.
      log.info("OutputFormat is {}", outputFormatName);
      Class<? extends OutputFormat> ofClass = Class.forName(outputFormatName).asSubclass(OutputFormat.class);
      conf.setOutputFormat(ofClass);
      conf.setBoolean(OUTPUT_FORMAT_OVERRIDE, true);
    }

    //Configure the mappers and reducers after we have done everything else in case they want to override anything
    mapper.getFixture().init(conf);
    if (reducer != null) {
      reducer.getFixture().init(conf);
    }

    boolean debugAll = conf.getBoolean("lww.debug.all", false);
    if (debugAll) {
      conf.set("yarn.app.mapreduce.am.log.level", "DEBUG");
    }

    // Run the job and wait
    RunningJob job = JobClient.runJob(conf);
    job.waitForCompletion();

    // Allow the mapper/reducer to cleanup after itself
    mapper.getFixture().close();
    if (reducer != null) {
      reducer.getFixture().close();
    }

    // Final job commit
    doFinalCommit(conf, job);

    // Determine the return code
    if (job.isSuccessful()) {
      long added = job.getCounters().getCounter(BaseHadoopIngest.Counters.DOCS_ADDED);
      if (added == 0) {
        System.out.println("Didn't ingest any documents, failing");
        log.warn("Didn't ingest any documents, failing");
        return 1;
      } else {
        // Added some docs
        return 0;
      }
    } else {
      log.warn("Something went wrong, failing");
      return 1;
    }
  }

  protected void processConf(String confAdds, JobConf conf) throws Exception {
    String[] splits = confAdds.split(";");
    if (splits != null && splits.length > 0) {
      for (String split : splits) {
        //split on =, LHS is the key, RHS is the value, as a single value
        String[] keyVal = split.split("\\[");
        if (keyVal != null && keyVal.length == 2) {
          keyVal[1] = keyVal[1].replace(']', ' ').trim();//remove the trailing ]
          //try to intelligently handle the values
          if (keyVal[1].equalsIgnoreCase("true") || keyVal[1].equalsIgnoreCase("false")) {
            log.info("Setting {} as a boolean: {}", keyVal[0], keyVal[1]);
            conf.setBoolean(keyVal[0], Boolean.parseBoolean(keyVal[1]));
          } else {
            try {
              Integer num = Integer.parseInt(keyVal[1]);
              if (num != null) {
                log.info("Setting {} as an int: {}", keyVal[0], keyVal[1]);
                conf.setInt(keyVal[0], num);
                continue;
              }
            } catch (NumberFormatException e) {
            }
            try {
              Float num = Float.parseFloat(keyVal[1]);
              if (num != null) {
                log.info("Setting {} as a float: {}", keyVal[0], keyVal[1]);
                conf.setFloat(keyVal[0], num);
                continue;
              }
            } catch (NumberFormatException e) {
            }
            log.info("Setting {} as a String: {}", keyVal[0], keyVal[1]);
            conf.set(keyVal[0], keyVal[1]);
          }
        } else {
          log.error("Can't parse split {}", split);
          throw new Exception("Can't parse split: " + split);
        }
      }
    }
  }

  public void doFinalCommit(JobConf conf, RunningJob job) {
    if (conf.getBoolean("lww.commit.on.close", false)) {
      String jobName = job.getJobName();
      log.info("Performing final commit for job " + jobName);
      // Progress can be null here, because no write operation is performed.
      LucidWorksWriter lww = new LucidWorksWriter(null);
      try {
        lww.open(conf, jobName);
        lww.commit();
      } catch (Exception e) {
        log.error("Error in final job commit", e);
      }
    }
  }

  public boolean checkSolrServer(JobConf conf) {
    String jobName = "defaultjobName";
    // Progress can be null here, because no write operation is performed.
    LucidWorksWriter lww = new LucidWorksWriter(null);
    try {
      lww.open(conf, jobName);
      lww.ping();
      return true;
    } catch (Exception e) {
      log.error("Error while checking Solr Server", e);
      return false;
    }
  }
}
