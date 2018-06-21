package com.lucidworks.hadoop.ingest;

import com.lucidworks.hadoop.io.LWDocument;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVStrategy;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.solr.common.util.StrUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.lucidworks.hadoop.utils.ConfigurationKeys.COLLECTION;

/**
 *
 *
 **/
public class CSVIngestMapper extends AbstractIngestMapper<LongWritable, Text> {

  public static final String CSV_FIELD_MAPPING = "csvFieldMapping";
  public static final String CSV_DELIMITER = "csvDelimiter";
  public static final String CSV_IGNORE_FIRST_LINE_COMMENT = "csvFirstLineComment";
  public static final String CSV_STRATEGY = "csvStrategy";
  public static final String DEFAULT_STRATEGY = "default";
  public static final String EXCEL_STRATEGY = "excel";
  public static final String TAB_DELIM_STRATEGY = "tdf";

  protected Map<Integer, String> fieldMap = null;
  protected CSVStrategy strategy = null;

  private static final String DEFAULT_FIELD_NAME = "field_";
  private final AbstractJobFixture fixture = new AbstractJobFixture() {
    @Override
    public void init(JobConf conf) throws IOException {
      boolean override = conf.getBoolean(IngestJob.INPUT_FORMAT_OVERRIDE, false);
      if (!override) {
        conf.setInputFormat(TextInputFormat.class);
      }// else the user has overridden the input format and we assume it is OK.
      byte[] delimiterBase64 = Base64.encodeBase64(conf.get(CSV_DELIMITER, "").getBytes());
      conf.set(CSV_DELIMITER, new String(delimiterBase64));
    }
  };

  private String collection;
  private String idField;
  private boolean ignoreFirstLine = true;
  // If the fieldId is not set
  private boolean useDefaultId = true;

  @Override
  public void configure(JobConf conf) {
    super.configure(conf);
    idField = conf.get("idField", "id");
    collection = conf.get(COLLECTION);
    if (collection == null) {
      throw new RuntimeException("No collection specified, aborting");
    }
    ignoreFirstLine = Boolean.parseBoolean(conf.get(CSV_IGNORE_FIRST_LINE_COMMENT, "false"));
    String delimiterStr = conf.get(CSV_DELIMITER);
    if (delimiterStr != null) {
      byte[] delimiterBase64 = Base64.decodeBase64(delimiterStr.getBytes());
      delimiterStr = new String(delimiterBase64);
    }

    // we get a string, but we only use the first character, as delimiters must be a 'char'
    String fieldMapStr = conf.get(CSV_FIELD_MAPPING);

    if (fieldMapStr == null) {
      log.warn("No field mapping specified, mapping to generic names, i.e. field_1, field_2, etc");
      fieldMap = Collections.emptyMap();
    } else {
      fieldMap = parseFieldMapStr(fieldMapStr);
    }
    String stratStr = conf.get(CSV_STRATEGY, DEFAULT_STRATEGY);
    if (stratStr.equalsIgnoreCase(DEFAULT_STRATEGY)) {
      strategy = copyStrategy(CSVStrategy.DEFAULT_STRATEGY);
      // Only change the delimiter for DEFAULT and EXCEL
      if (delimiterStr != null && !delimiterStr.isEmpty() && !delimiterStr
          .equals(strategy.getDelimiter() + "")) {
        strategy.setDelimiter(delimiterStr.charAt(0));
      }
    } else if (stratStr.equalsIgnoreCase(EXCEL_STRATEGY)) {
      strategy = copyStrategy(CSVStrategy.EXCEL_STRATEGY);
      if (delimiterStr != null && !delimiterStr.isEmpty() && !delimiterStr
          .equals(strategy.getDelimiter() + "")) {
        strategy.setDelimiter(delimiterStr.charAt(0));
      }
    } else if (stratStr.equalsIgnoreCase(TAB_DELIM_STRATEGY)) {
      strategy = copyStrategy(CSVStrategy.TDF_STRATEGY);
    } else {
      try {
        Class<? extends CSVStrategy> stratClass = Class.forName(stratStr)
            .asSubclass(CSVStrategy.class);
        strategy = stratClass.newInstance();
      } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
        log.error("Exception", e);
        throw new RuntimeException("Couldn't load CSVStrategy class, aborting");
      }
    }
  }

  private Map<Integer, String> parseFieldMapStr(String fieldMapStr) {
    Map<Integer, String> result = new HashMap<Integer, String>();
    // looks like: number=name,number=name, ....
    List<String> keyValues = StrUtils.splitSmart(fieldMapStr, ',');
    for (String keyValue : keyValues) {
      String[] splits = keyValue.split("=");
      if (splits != null && splits.length == 2) {
        result.put(Integer.parseInt(splits[0].trim()), splits[1].trim());
      } else {
        throw new RuntimeException("Invalid Field mapping passed in: " + fieldMapStr);
      }
    }
    // check whether or not the fieldId is on the field map
    if (result.containsValue(idField)) {
      useDefaultId = false;
    }
    return result;
  }

  @Override
  protected LWDocument[] toDocuments(LongWritable key, Text value, Reporter reporter,
                                     Configuration conf) throws IOException {

    if (ignoreFirstLine && key.get() == 0) {
      // Ignoring the First Line
      return null;
    }

    CSVParser parser = new CSVParser(
      new InputStreamReader(new ByteArrayInputStream(value.getBytes(), 0, value.getLength()), "UTF-8"), strategy);

    try {
      String[] row = parser.getLine();
      if (row == null) {
        log.warn("No values for document with key: {}, skipping", key.get());
        return null;
      }

      LWDocument document = createDocument();
      for (int i = 0; i < row.length; i++) {
        String rowValue = row[i];
        if (null == rowValue || rowValue.trim().isEmpty()) {
          continue;
        }
        rowValue = rowValue.trim();
        String name = fieldMap.get(i);
        if (name != null) {
          if (i == 0 && useDefaultId) {
            // by default, the first string in vals will be the document id
            document.setId(rowValue);
          } else {
            if (!useDefaultId && name.equals(idField)) {
              document.setId(rowValue);
            } else {
              document.addField(name, rowValue);
            }
          }
        } else {
          if (i == 0) {
            document.setId(rowValue);
          } else {
            document.addField(DEFAULT_FIELD_NAME + i, rowValue);
          }
        }
      }
      return new LWDocument[] {document};
    } catch (IOException e) {
      log.error("Unable to parse document with key: {} value: {}", key.get(), value, e);
    }
    return null;
  }

  @Override
  public AbstractJobFixture getFixture() {
    return fixture;
  }

  /*
   * CSVStrategy objects have several mutators that allow their behavior to be customize by consumers.  These are
   * convenient in the general case, but introduce trappy behavior when used with static/predefined strategies such as
   * {@link CSVStrategy#DEFAULT}.
   *
   * This method copies the provided CSVStrategy, so that mutators can be safely used with the static CSVStrategy
   * constants.
   */
  private CSVStrategy copyStrategy(CSVStrategy strategy) {
    return new CSVStrategy(strategy.getDelimiter(), strategy.getEncapsulator(), strategy.getCommentStart(),
            strategy.getEscape(), strategy.getIgnoreLeadingWhitespaces(), strategy.getIgnoreTrailingWhitespaces(),
            strategy.getUnicodeEscapeInterpretation(), strategy.getIgnoreEmptyLines());
  }
}
