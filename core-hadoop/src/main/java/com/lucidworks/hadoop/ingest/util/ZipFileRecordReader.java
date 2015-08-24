package com.lucidworks.hadoop.ingest.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This RecordReader implementation extracts individual files from a ZIP file
 * and hands them over to the Mapper. The "key" is the decompressed file name,
 * the "value" is the file contents.
 */
public class ZipFileRecordReader implements RecordReader<Text, BytesWritable> {
  private transient static Logger log = LoggerFactory.getLogger(ZipFileRecordReader.class);
  /**
   * InputStream used to read the ZIP file from the FileSystem
   */
  private FSDataInputStream fsin;

  /**
   * ZIP file parser/decompresser
   */
  private ZipInputStream zip;

  private InputStream compressedInputStream;

  private FileSplit fileSplit;
  private boolean processed = false;

  public ZipFileRecordReader(FileSplit fileSplit, Configuration conf) throws IOException {
    this.fileSplit = fileSplit;

    Path path = fileSplit.getPath();
    FileSystem fs = path.getFileSystem(conf);

    //Open the stream
    if (CompressionHelper.isCompressed(path)) {
      compressedInputStream = CompressionHelper.openCompressedFile(path, conf);
      zip = new ZipInputStream(compressedInputStream);
    } else {
      fsin = fs.open(path);
      zip = new ZipInputStream(fsin);
    }
  }

  /**
   * Close quietly, ignoring any exceptions
   */
  @Override
  public void close() throws IOException {
    try {
      zip.close();
    } catch (Exception ignore) {
    }
    try {
      fsin.close();
      compressedInputStream.close();
    } catch (Exception ignore) {
    }
  }

  @Override
  public Text createKey() {
    return new Text();
  }

  @Override
  public BytesWritable createValue() {
    return new BytesWritable();

  }

  @Override
  public long getPos() throws IOException {
    return processed ? fileSplit.getLength() : 0;
  }

  @Override
  public float getProgress() throws IOException {
    return processed ? 1.0f : 0.0f;
  }

  /**
   * Each ZipEntry is decompressed and readied for the Mapper. If the
   * ZipFileInputFormat has been set to Lenient (not the default), certain
   * exceptions will be gracefully ignored to prevent a larger job from
   * failing.
   */

  @Override
  public boolean next(Text key, BytesWritable value) throws IOException {
    {
      ZipEntry entry = null;
      try {
        entry = zip.getNextEntry();
      } catch (Throwable e) {
        if (!ZipFileInputFormat.getLenient()) {
          throw new RuntimeException(e);
        }
      }

      // Sanity check
      if (entry == null) {
        processed = true;
        return false;
      }

      // Filename
      key.set(new Text(entry.getName()));

      byte[] bufferOut = null;
      int cummulativeBytesRead = 0;
      while (true) {
        int bytesRead = 0;
        byte[] bufferIn = new byte[8192];
        try {
          bytesRead = zip.read(bufferIn, 0, bufferIn.length);
        } catch (Throwable e) {
          if (!ZipFileInputFormat.getLenient()) {
            throw new RuntimeException(e);
          }
          return false;
        }
        if (bytesRead > 0) {
          byte[] tmp = head(bufferIn, bytesRead);
          if (cummulativeBytesRead == 0) {
            bufferOut = tmp;
          } else {
            bufferOut = add(bufferOut, tmp);
          }
          cummulativeBytesRead += bytesRead;
        } else {
          break;
        }
      }
      try {
        zip.closeEntry();
      } catch (IOException e) {
        if (!ZipFileInputFormat.getLenient()) {
          throw new RuntimeException(e);
        }
      }
      // Uncompressed contents
      if (bufferOut != null) {
        value.setCapacity(bufferOut.length);
        value.set(bufferOut, 0, bufferOut.length);
      } else {
        log.warn("bufferOut is null for "
            + key);//should we return false here?  I don't think so, since I think that would mean we can't process any more records
      }
      return true;
    }
  }
  //from HBase, Apache licensed

  /**
   * @param a      array
   * @param length amount of bytes to grab
   * @return First <code>length</code> bytes from <code>a</code>
   */
  public static byte[] head(final byte[] a, final int length) {
    if (a.length < length) {
      return null;
    }
    byte[] result = new byte[length];
    System.arraycopy(a, 0, result, 0, length);
    return result;
  }

  /**
   * An empty instance.
   */
  public static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

  /**
   * @param a lower half
   * @param b upper half
   * @return New array that has a in lower half and b in upper half.
   */
  public static byte[] add(final byte[] a, final byte[] b) {
    return add(a, b, EMPTY_BYTE_ARRAY);
  }

  /**
   * @param a first third
   * @param b second third
   * @param c third third
   * @return New array made from a, b and c
   */
  public static byte[] add(final byte[] a, final byte[] b, final byte[] c) {
    byte[] result = new byte[a.length + b.length + c.length];
    System.arraycopy(a, 0, result, 0, a.length);
    System.arraycopy(b, 0, result, a.length, b.length);
    System.arraycopy(c, 0, result, a.length + b.length, c.length);
    return result;
  }
}