package com.lucidworks.hadoop.ingest.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.Decompressor;

import java.io.InputStream;

public class CompressionHelper {

  //FIXME: use MIME types
  public static final String GZIP_EXTGENSION = ".gz";
  public static final String BZIP2_EXTGENSION = ".bz2";
  public static final String LZO_EXTGENSION = ".lzo";
  public static final String SNAPPY_EXTGENSION = ".snappy";

  public static boolean isCompressed(Path filePath) {
    String stringPath = filePath.toString();
    if (stringPath.endsWith(GZIP_EXTGENSION)
            || stringPath.endsWith(BZIP2_EXTGENSION)
            || stringPath.endsWith(LZO_EXTGENSION)
            || stringPath.endsWith(SNAPPY_EXTGENSION)) {
      System.out.println("File: " + filePath.toString() + " is compressed");
      return true;
    }
    return false;
  }

  /**
   * This function opens a stream to read a compressed file. Stream is not
   * closed, the user has to close it when read is finished.
   *
   * @param filePath
   * @return
   */
  public static InputStream openCompressedFile(Path filePath, Configuration conf) {
    CompressionCodecFactory factory = new CompressionCodecFactory(conf);
    CompressionCodec codec = factory.getCodec(filePath);

    if (codec == null) {
      // FIXME: not throw, just log
      throw new RuntimeException("No codec found for file "
              + filePath.toString());
    }

    try {
      FileSystem fs = filePath.getFileSystem(conf);
      Decompressor decompressor = codec.createDecompressor();
      CompressionInputStream in = codec.createInputStream(fs.open(filePath),
              decompressor);
      return in;
    } catch (Exception e) {
      System.out.println("Error opening compressed file: " + e.getMessage());
      e.printStackTrace();
    }
    return null;
  }

  public static CompressionCodec createCompressionCodec(Path filePath, Configuration conf) {
    CompressionCodecFactory factory = new CompressionCodecFactory(conf);
    CompressionCodec codec = factory.getCodec(filePath);

    if (codec == null) {
      // :(
      throw new RuntimeException("No codec found for file "
              + filePath.toString());
    }
    return codec;
  }
}
