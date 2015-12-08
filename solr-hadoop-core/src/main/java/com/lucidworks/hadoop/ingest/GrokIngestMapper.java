package com.lucidworks.hadoop.ingest;

import com.lucidworks.hadoop.cache.DistributedCacheHandler;
import com.lucidworks.hadoop.ingest.util.GrokHelper;
import com.lucidworks.hadoop.io.LWDocument;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.jruby.RubyHash;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GrokIngestMapper extends AbstractIngestMapper<LongWritable, Text> {
  private transient static Logger log = LoggerFactory.getLogger(GrokIngestMapper.class);

  public static final String GROK_URI = "grok.uri";
  public static final String GROK_CONFIG_PATH = "grok.config.path";
  public static final String ADDITIONAL_PATTERNS = "grok.additional.patterns";

  public static final String LOG_RUBY_PARAM = "log";
  public static final String CONFIG_STRING_RUBY_PARAM = "config_string_param";
  public static final String ADDITIONAL_PATTERNS_RUBY_PARAM = "additional_patterns";
  public static final String FILTERS_ARRAY_RUBY_PARAM = "filters";
  public static final String LOADER_RUBY_CLASS = "logStash-grok/loader.rb";
  public static final String MATCHER_RUBY_CLASS = "logStash-grok/matcher.rb";
  public static final String PATTERN_HANDLER_RUBY_CLASS = "logStash-grok/patternHandler.rb";

  public static final String PATH_FIELD_NAME = "path";
  public static final String BYTE_OFFSET_FIELD_NAME = "byte_offset";

  private Object filters;

  private final AbstractJobFixture fixture = new AbstractJobFixture() {
    @Override
    public void init(JobConf conf) throws IOException {
      super.init(conf);
      fillDistributeCache(conf);
    }
  };

  @Override
  public void configure(JobConf conf) {
    super.configure(conf);
    String configurationString = DistributedCacheHandler.getFileFromCache(conf, GROK_CONFIG_PATH);

    String additionalPatternsPaths = conf.get(ADDITIONAL_PATTERNS);

    StringBuilder additionalPatternsParam = new StringBuilder();
    if (additionalPatternsPaths != null) {
      String[] paths = additionalPatternsPaths.split(";");
      for (String currentPath : paths) {
        String content = DistributedCacheHandler.getFileFromCache(conf, currentPath);
        additionalPatternsParam.append(content);
      }
    }

    if (configurationString == null || configurationString.isEmpty()) {
      throw new RuntimeException(
          "Grok configuration not found at: " + conf.get(GROK_CONFIG_PATH) + " and URI: " + conf
              .get(GROK_URI));
    }

    Map<String, Object> params = new HashMap<String, Object>();
    params.put(CONFIG_STRING_RUBY_PARAM, configurationString);
    params.put(ADDITIONAL_PATTERNS_RUBY_PARAM, additionalPatternsParam.toString().trim());

    List<String> toRemove = new ArrayList<String>();
    toRemove.add(CONFIG_STRING_RUBY_PARAM);
    toRemove.add(ADDITIONAL_PATTERNS_RUBY_PARAM);
    Object response = executeScript(LOADER_RUBY_CLASS, params, toRemove);

    if (response != null) {
      filters = response;
    } else {
      throw new RuntimeException("Filters are null");
    }

  }

  @Override
  public AbstractJobFixture getFixture() {
    return fixture;
  }

  @Override
  protected LWDocument[] toDocuments(LongWritable key, Text value, Reporter reporter,
      Configuration conf) throws IOException {

    Map<String, Object> params = new HashMap<String, Object>();
    params.put(LOG_RUBY_PARAM, value.toString());
    params.put(FILTERS_ARRAY_RUBY_PARAM, filters);

    List<String> toRemoveList = new ArrayList<String>();
    toRemoveList.add(LOG_RUBY_PARAM);
    toRemoveList.add(FILTERS_ARRAY_RUBY_PARAM);
    Object response = executeScript(MATCHER_RUBY_CLASS, params, toRemoveList);

    try {
      RubyHash hash = (RubyHash) response;
      if (response != null) {
        Set<String> keys = hash.keySet();
        LWDocument document = createDocument();
        for (String currentKey : keys) {
          document.addField(currentKey, hash.get(currentKey));
        }

        // Adding the file where this log was taken
        FileSplit fileSplit = (FileSplit) reporter.getInputSplit();
        String originalLogFilePath = fileSplit.getPath().toUri().getPath();
        document.addField(PATH_FIELD_NAME, originalLogFilePath);

        // Adding offset value
        document.addField(BYTE_OFFSET_FIELD_NAME, key.toString());

        // Set ID
        document
            .setId(originalLogFilePath + "-" + key.toString() + "-" + System.currentTimeMillis());

        return document.process();
      } else {
        return null;
      }
    } catch (Exception e) {
      log.error("Error: " + e.getMessage());
      throw new RuntimeException("Error executing ruby script");
    }
  }

  public static Object executeScript(String resourcePath, Map<String, Object> params,
      List<String> attributesToRemove) {
    ScriptEngineManager manager = new ScriptEngineManager();
    ScriptEngine engine = manager.getEngineByName("ruby");
    InputStream resource = GrokIngestMapper.class.getClassLoader()
        .getResourceAsStream(resourcePath);

    for (String toRemove : attributesToRemove) {
      engine.getContext().setAttribute(toRemove, params.get(toRemove),
          ScriptContext.ENGINE_SCOPE);// necessary limit the scope to just
      // engine
    }

    for (Map.Entry<String, Object> entry : params.entrySet()) {
      manager.put(entry.getKey(), entry.getValue());
    }

    if (resource == null) {
      throw new RuntimeException("Resource not found " + resourcePath);
    }
    if (engine == null) {
      throw new RuntimeException("Script engine can not be created");
    }
    InputStreamReader is = new InputStreamReader(resource);

    try {
      Object response = engine.eval(is);
      return response;
    } catch (Exception e) {
      log.error("Error executing script: " + e.getMessage(), e);
      throw new RuntimeException("Error executing ruby script", e);
    }
  }

  private static void fillDistributeCache(JobConf conf) throws RuntimeException {
    String grokURI = conf.get(GROK_URI, null);
    if (grokURI == null) {
      throw new RuntimeException("You must specify the -D" + GROK_URI);
    }

    try {
      DistributedCacheHandler.addFileToCache(conf, new Path(grokURI), GROK_CONFIG_PATH);

    } catch (Exception e) {
      System.out.println("Error caching grok configuration file: " + e.getMessage());
    }
    // To find patterns_dir
    String configuration = GrokHelper.readConfiguration(grokURI, conf);
    handlePatternDir(conf, configuration);
  }

  private static void handlePatternDir(JobConf conf, String configuration) {
    Map<String, Object> params = new HashMap<String, Object>();
    params.put(CONFIG_STRING_RUBY_PARAM, configuration);
    Object response = executeScript(PATTERN_HANDLER_RUBY_CLASS, params, new ArrayList<String>());

    try {
      GrokHelper.addPatternDirToDC(response, conf);
    } catch (Exception e) {
      System.out.println("Error caching grok additonal patterns: " + e.getMessage());
    }
  }
}
