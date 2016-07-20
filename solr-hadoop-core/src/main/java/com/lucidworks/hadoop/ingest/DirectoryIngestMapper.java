package com.lucidworks.hadoop.ingest;

import com.lucidworks.hadoop.io.LWDocument;
import com.lucidworks.hadoop.utils.CompressionHelper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import static com.lucidworks.hadoop.utils.ConfigurationKeys.MIME_TYPE;
import static com.lucidworks.hadoop.utils.ConfigurationKeys.TEMP_DIR;

public class DirectoryIngestMapper extends AbstractIngestMapper<Text, NullWritable> {
  private transient static Logger log = LoggerFactory.getLogger(DirectoryIngestMapper.class);

  public static final String DIRECTORY_ADD_SUBDIRECTORIES = "add.subdirectories";

  private final AbstractJobFixture fixture = new AbstractJobFixture() {
    @Override
    public void init(JobConf conf) throws IOException {
      // Expand the input path glob into a sequence file of inputs
      Path actualInput = new Path(conf.get(TEMP_DIR), "inputs.seq");
      expandGlob(conf, actualInput, FileInputFormat.getInputPaths(conf));

      // Configure the real M/R job
      conf.setInputFormat(SequenceFileInputFormat.class);
      FileInputFormat.setInputPaths(conf, actualInput);
      conf.setMapperClass(DirectoryIngestMapper.class);
    }
  };

  @Override
  public void configure(JobConf conf) {
    super.configure(conf);
  }

  @Override
  public AbstractJobFixture getFixture() {
    return fixture;
  }

  public static void expandGlob(
    Configuration conf,
    Path output,
    Path... pathsToExpand) throws IOException {
    log.info("Expanding glob to a sequence file of inputs");
    SequenceFile.Writer writer = SequenceFile
      .createWriter(output.getFileSystem(conf), conf, output, Text.class, NullWritable.class);
    boolean addSubdirectories = conf.getBoolean(DIRECTORY_ADD_SUBDIRECTORIES, false);
    long i;
    try {
      i = processPaths(conf, addSubdirectories, writer, pathsToExpand);
    } finally {
      writer.sync();
      writer.close();
    }

    log.info("Wrote {} values to {}", i, output.toString());
  }

  private static long processPaths(
    Configuration conf,
    boolean addSubdirectories,
    SequenceFile.Writer writer,
    Path... pathsToExpand) throws IOException {
    long counter = 0;
    for (Path path : pathsToExpand) {
      FileSystem fileSystem = path.getFileSystem(conf);
      for (FileStatus fstat : fileSystem.globStatus(path)) {
        if (fstat.isDir()) {
          if (addSubdirectories == true) {
            counter += processPaths(conf, addSubdirectories, writer, new Path(fstat.getPath().toUri()
                                                                                   .toString() + "/*"));
          }// TODO: should we log that we skipped the sub dir?
        } else {
          writer.append(new Text(fstat.getPath().toUri().toString()), NullWritable.get());
          counter++;
        }
      }
    }
    return counter;

  }

  @Override
  public LWDocument[] toDocuments(
    Text uri,
    NullWritable nullWritable,
    Reporter reporter,
    Configuration conf) throws IOException {
    Path file;
    try {
      file = new Path(new URI(uri.toString()));
    } catch (URISyntaxException e) {
      throw new IOException(e);
    }
    log.debug("Processing: {} conf: {}", file, conf);
    FileSystem fs = file.getFileSystem(conf);
    byte[] ba = null;

    // Checking if decompression is needed
    if (CompressionHelper.isCompressed(file)) {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      InputStream is = CompressionHelper.openCompressedFile(file, conf);
      IOUtils.copyBytes(is, baos, 4096, true);
      ba = baos.toByteArray();

    } else {
      ba = new byte[(int) fs.getFileStatus(file).getLen()];
      FSDataInputStream fis = fs.open(file);
      fis.readFully(ba);
      fis.close();
    }

    Map<String, String> metadata = new HashMap<String, String>();
    String mimeType = conf.get(MIME_TYPE, null);
    if (mimeType != null) {
      metadata.put(MIME_TYPE, mimeType);
    }
    LWDocument doc = createDocument(uri.toString(), metadata);
    doc.setContent(ba);
    return new LWDocument[]{doc};
  }
}

