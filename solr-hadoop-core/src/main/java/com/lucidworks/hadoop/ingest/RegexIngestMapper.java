package com.lucidworks.hadoop.ingest;

import com.lucidworks.hadoop.io.LWDocument;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;

public class RegexIngestMapper extends AbstractIngestMapper<Writable, Writable> {

  public static final String REGEX = RegexIngestMapper.class.getName() + ".regex";
  public static final String GROUPS_TO_FIELDS =
      RegexIngestMapper.class.getName() + ".groups_to_fields";
  private static final Pattern GROUP_SEPARATOR = Pattern.compile(",");
  private static final Pattern GROUP_FIELD_SEPARATOR = Pattern.compile("=");
  /**
   * If true, then use {@link java.util.regex.Matcher#matches()} instead of find
   */
  public static final String REGEX_MATCH = RegexIngestMapper.class.getName() + "." + "match";
  public static final String FIELD_PATH = "path";

  private final AbstractJobFixture fixture = new AbstractJobFixture() {
    @Override
    public void init(JobConf conf) throws IOException {
    }
  };

  @Override
  public AbstractJobFixture getFixture() {
    return fixture;
  }

  protected Pattern regex;
  protected Map<Integer, String> groupToFields;
  protected boolean match;

  @Override
  public void configure(JobConf conf) {
    super.configure(conf);
    String regexStr = conf.get(REGEX);
    if (regexStr != null && regexStr.isEmpty() == false) {
      regex = Pattern.compile(regexStr);
    } else {
      throw new RuntimeException(REGEX + " property must not be null or empty");
    }
    String groupToFieldsStr = conf.get(GROUPS_TO_FIELDS);
    if (groupToFieldsStr != null && groupToFieldsStr.isEmpty() == false) {
      //format is: groupNumber=fieldName,groupNumber=fieldName as in: 1=id,2=title,3=dog
      groupToFields = new HashMap<Integer, String>();
      String[] splits = GROUP_SEPARATOR.split(groupToFieldsStr);
      for (String split : splits) {
        String[] groupFieldSplit = GROUP_FIELD_SEPARATOR.split(split);
        if (groupFieldSplit != null && groupFieldSplit.length == 2) {
          groupToFields.put(Integer.parseInt(groupFieldSplit[0]), groupFieldSplit[1]);
        } else {
          throw new RuntimeException(
              "Malformed " + GROUPS_TO_FIELDS + " property: " + groupToFieldsStr
                  + ".  Format is: groupNumber=fieldName,groupNumber=fieldName as in: 1=id,2=title,3=dog");
        }
      }
    } else {
      throw new RuntimeException(GROUPS_TO_FIELDS + " property must not be null or empty");
    }
    match = conf.getBoolean(REGEX_MATCH, false);
  }

  @Override
  public LWDocument[] toDocuments(Writable key, Writable value, Reporter reporter,
      Configuration conf) throws IOException {
    if (key != null && value != null) {
      LWDocument doc = createDocument(key.toString() + "-" + System.currentTimeMillis(), null);
      Matcher matcher = regex.matcher(value.toString());
      if (matcher != null) {
        if (match) {
          if (matcher.matches()) {
            processMatch(doc, matcher);
          }
        } else {//
          while (matcher.find()) {
            processMatch(doc, matcher);
            reporter.progress();//do we really even need this?
          }
        }
      }
      // Adding the file path where this record was taken
      FileSplit fileSplit = (FileSplit) reporter.getInputSplit();
      String originalLogFilePath = fileSplit.getPath().toUri().getPath();
      doc.addField(FIELD_PATH, originalLogFilePath);
      String docId = originalLogFilePath + "-" + doc.getId();
      doc.setId(docId);
      return new LWDocument[] {doc};
    }
    return null;
  }

  protected void processMatch(LWDocument doc, Matcher matcher) {
    int groupCount = matcher.groupCount();
    if (groupCount >= 0) {
      for (int i = 0; i < groupCount + 1; i++) {//include the "0" group
        String field = groupToFields.get(i);
        if (field != null) {
          doc.addField(field, matcher.group(i));
        } //else: nothing to do, as we don't have a mapping
      }
    }
  }

}
