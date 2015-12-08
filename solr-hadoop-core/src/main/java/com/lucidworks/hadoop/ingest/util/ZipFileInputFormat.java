package com.lucidworks.hadoop.ingest.util;

import java.io.IOException;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

/**
 * Extends the basic FileInputFormat class provided by Apache Hadoop to accept
 * ZIP files. It should be noted that ZIP files are not 'splittable' and each
 * ZIP file will be processed by a single Mapper.
 */
public class ZipFileInputFormat extends FileInputFormat<Text, BytesWritable> {
  /**
   * See the comments on the setLenient() method
   */
  private static boolean isLenient = false;

  /**
   * @param lenient
   */
  public static void setLenient(boolean lenient) {
    isLenient = lenient;
  }

  public static boolean getLenient() {
    return isLenient;
  }

  @Override
  public RecordReader<Text, BytesWritable> getRecordReader(InputSplit arg0, JobConf arg1,
      Reporter arg2) throws IOException {
    return new ZipFileRecordReader((FileSplit) arg0, arg1);
  }
}