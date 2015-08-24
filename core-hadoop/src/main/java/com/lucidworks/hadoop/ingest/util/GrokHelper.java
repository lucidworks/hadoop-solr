package com.lucidworks.hadoop.ingest.util;

import com.lucidworks.hadoop.cache.DistributedCacheHandler;
import com.lucidworks.hadoop.ingest.GrokIngestMapper;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapred.JobConf;
import org.jruby.RubyArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GrokHelper {
  private static transient Logger log = LoggerFactory.getLogger(GrokHelper.class);

  public static String readConfiguration(String path, JobConf conf) {
    String response = "";
    Path p = new Path(path);
    try {
      FileSystem fs = p.getFileSystem(conf);
      FSDataInputStream inputStream = fs.open(p);
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      IOUtils.copyBytes(inputStream, out, conf);
      response = out.toString();
      fs.close();
    } catch (IOException e) {
      log.error("Unable to read " + path + " from HDFS", e);
    }
    return response;
  }

  public static List<String> getAllSubPaths(String path) {
    List<String> response = new ArrayList<String>();
    getFiles(new File(path), response);
    return response;
  }

  public static List<Path> getAllSubPaths(Path path, JobConf conf) {
    List<Path> response = new ArrayList<Path>();
    try {
      FileSystem fs = FileSystem.get(conf);
      FileStatus[] status = fs.listStatus(path);
      for (int i = 0; i < status.length; i++) {
        response.add(status[i].getPath());
      }
    } catch (Exception e) {
      log.error("Error while getting subpaths", e);
    }

    return response;
  }

  private static void getFiles(File f, List<String> response) {
    File files[];
    if (f.isFile()) {
      response.add(f.getAbsolutePath());
    } else {
      files = f.listFiles();
      for (int i = 0; i < files.length; i++) {
        getFiles(files[i], response);
      }
    }
  }

  public static void addPatternDirToDC(Object array, JobConf conf) throws Exception {
    if (!array.getClass().getName().equals("org.jruby.RubyArray")) {
      throw new RuntimeException("Param is not RubyArray");
    }

    RubyArray rubyArray = (RubyArray) array;
    int size = rubyArray.size();
    for (int counter = 0; counter < size; counter++) {
      // Adding the prefix "file://" because the user will use the same LogStash
      // configuration file, and this configuration assumes that the additional
      // patterns are in local file system
      String path = "file://" + rubyArray.get(counter).toString();

      List<Path> filesToExplore = new ArrayList<Path>();
      try {
        Path currentPath = new Path(path);
        FileSystem fs = currentPath.getFileSystem(conf);
        if (fs.isDirectory(currentPath)) {
          filesToExplore = getAllSubPaths(currentPath, conf);
        } else {
          filesToExplore.add(currentPath);
        }
      } catch (Exception e) {
        log.error("Error while reading path " + path, e);
      }

      for (Path cPath : filesToExplore) {
        DistributedCacheHandler.addFileToCache(conf, cPath, cPath.toString());
        String currentPaths = conf.get(GrokIngestMapper.ADDITIONAL_PATTERNS);
        if (currentPaths != null) {
          currentPaths += ";" + cPath;
        } else {
          currentPaths = cPath.toString();
        }
        conf.set(GrokIngestMapper.ADDITIONAL_PATTERNS, currentPaths);
      }
    }
  }
}

