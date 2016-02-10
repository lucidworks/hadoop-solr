package com.lucidworks.hadoop.utils;

import com.google.common.collect.ObjectArrays;
import com.lucidworks.hadoop.io.LWDocumentWritable;
import com.lucidworks.hadoop.io.impl.LWMockDocument;
import java.util.HashMap;
import java.util.Map;
import org.junit.Ignore;

@Ignore
public class TestUtils {
  // From Ingest Job
  private static String CONF_OPTION = "conf";

  // mandatory job args
  public static String HADOOP_ARGS = "-jn %s -cls %s -c %s -i %s";


  public static String[] createHadoopJobArgsWithConf(String jn, String cls, String c, String s, String i, String conf) {
    String csvArgs = "";
    if (conf != null) {
      csvArgs = csvArgs + "--" + CONF_OPTION + "&s" + conf;
    }
    return ObjectArrays.concat(createHadoopJobArgs(jn, cls, c, s, i),
        csvArgs.split("&s"), String.class);
  }

  public static String[] createHadoopJobArgsWithConf(String jn, String cls, String c, String s, String i, String conf, String of) {
    String csvArgs = "";
    if (conf != null) {
      csvArgs = csvArgs + "--" + CONF_OPTION + "&s" + conf;
    }
    return ObjectArrays.concat(createHadoopJobArgs(jn, cls, c, s, i, of),
        csvArgs.split("&s"), String.class);
  }

  /**
   * Creates a Hadoop job args with optional arguments
   *
   * @param jn       jobname
   * @param cls      classname
   * @param c        collection
   * @param s        solr url
   * @param i        input
   * @param optional list of optional args
   * @return
   */
  public static String[] createHadoopOptionalArgs(String jn, String cls, String c, String s,
      String i, String... optional) {
    String[] mandatoryArgs = createHadoopJobArgs(jn, cls, c, s, i);
    return ObjectArrays.concat(optional, mandatoryArgs, String.class);
  }

  /**
   * Creates a Hadoop job with mandatory args + jobname
   *
   * @param jn   jobname
   * @param cls  classname
   * @param c    collection
   * @param s    solr url (zk when isZK true)
   * @param i    input
   * @param isZk when true zookepper (Solr cloud)
   * @return
   */
  public static String[] createHadoopJobArgs(String jn, String cls, String c, String s, String i, String of,
      boolean isZk) {
    String args = HADOOP_ARGS;
    if (s != null) {
      if (isZk) {
        args = args + " -zk " + s;
      } else {
        args = args + " -s " + s;
      }
    }
    if (of == null) {
      args = args + " -of " + IngestJobMockMapRedOutFormat.class.getName();
    } else {
      args = args + " -of " + of;
    }
    // change the split: spaces could generate a problem
    return String.format(args, jn, cls, c, i).split(" ");
  }

  public static String[] createHadoopJobArgs(String jn, String cls, String c, String s, String i) {
    return createHadoopJobArgs(jn, cls, c, s, i, null, true);
  }

  public static String[] createHadoopJobArgs(String jn, String cls, String c, String s, String i, String of) {
    return createHadoopJobArgs(jn, cls, c, s, i, of, true);
  }


  public static LWDocumentWritable createLWDocumentWritable(String id, String... keyValues) {
    Map<String, String> fields = new HashMap<>();
    for (int i = 0; i < keyValues.length; i += 2) {
      fields.put(keyValues[i], keyValues[i + 1]);
    }
    LWMockDocument doc = new LWMockDocument(id, fields);
    return new LWDocumentWritable(doc);
  }
}
