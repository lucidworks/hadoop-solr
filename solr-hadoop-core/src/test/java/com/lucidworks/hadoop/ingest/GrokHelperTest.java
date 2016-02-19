package com.lucidworks.hadoop.ingest;

import com.lucidworks.hadoop.ingest.util.GrokHelper;
import com.lucidworks.hadoop.io.LWDocumentWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mrunit.MapDriver;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GrokHelperTest {

  private MapDriver<LongWritable, Text, Text, LWDocumentWritable> mapDriver;
  private JobConf jobConf;
  private Path base;
  private FileSystem fs;

  private final String lineSep = System.getProperty("line.separator");

  @Before
  public void setUp() throws IOException, URISyntaxException {
    mapDriver = new MapDriver<LongWritable, Text, Text, LWDocumentWritable>();

    Configuration configuration = new Configuration();
    configuration.set("io.serializations", "com.lucidworks.hadoop.io.impl.LWMockSerealization");
    mapDriver.setConfiguration(configuration);

    Configuration conf = mapDriver.getConfiguration();
    fs = FileSystem.getLocal(conf);
    Path dir = new Path(fs.getWorkingDirectory(), "build");
    Path sub = new Path(dir, "GHT");
    Path tempDir = new Path(sub, "tmp-dir");
    base = new Path(sub, "tmp-dir");
    fs.mkdirs(tempDir);
    jobConf = new JobConf(conf);

  }

  public Path copyToHDFS(String srcPath, String fileName) throws Exception {

    Path dst = new Path(base, fileName);
    Path src = new Path(srcPath);
    File tmp = new File(srcPath);
    if (tmp.isDirectory()) {
      fs.mkdirs(src);
    } else if (tmp.isFile()) {
      fs.copyFromLocalFile(src, dst);
    }
    return dst;
  }

  public String readLocaFile(String path) throws Exception {

    File f = new File(path);

    BufferedReader br = null;
    StringBuilder expected = new StringBuilder();

    String sCurrentLine;

    br = new BufferedReader(new FileReader(f));

    while ((sCurrentLine = br.readLine()) != null) {
      expected.append(sCurrentLine + lineSep);
    }
    br.close();
    String response = expected.toString();
    if (response.endsWith(lineSep)) {
      response = response.substring(0, response.length() - 1);
    }

    return response;
  }

  @Test
  public void testReadLocal() throws Exception {
    String configurationFileName = "grok" + File.separator + "IP-WORD.conf";
    String path = GrokHelperTest.class.getClassLoader().getResource(configurationFileName)
        .getPath();

    String config = GrokHelper.readConfiguration(path, new JobConf());

    URL url = GrokHelperTest.class.getClassLoader().getResource(configurationFileName);

    String expected = readLocaFile(url.getPath());
    Assert.assertEquals(expected, config);
  }

  @Test
  public void testReadHDFS() throws Exception {

    String configurationFileName = "grok" + File.separator + "IP-WORD.conf";
    // Create a new file in HDFS
    URL url = GrokHelperTest.class.getClassLoader().getResource(configurationFileName);

    Path dst = copyToHDFS(url.getPath(), "conf.conf");

    JobConf jobConf = new JobConf();
    String conf = GrokHelper.readConfiguration(dst.toString(), jobConf);

    String expected = readLocaFile(url.getPath());

    Assert.assertEquals(expected, conf);
  }

  public void addContent(String content, File file) {
    try {

      if (!file.exists()) {
        file.createNewFile();
      }

      FileWriter fw = new FileWriter(file.getAbsoluteFile());
      BufferedWriter bw = new BufferedWriter(fw);
      bw.write(content);
      bw.close();

    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public void addHDFSContent(String content, Path path) {
    try {
      FSDataOutputStream fsDataOutputStream = fs.create(path);
      fsDataOutputStream.writeUTF(content);
      fsDataOutputStream.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testGetAllSubPaths() {

    // Creating tmp hierarchy
    String wd =
        fs.getWorkingDirectory().toUri().getPath() + File.separator + "build" + File.separator
            + "hierarchy";

    File father = new File(wd);
    father.mkdir();

    File sonFolder = new File(father, "son");
    sonFolder.mkdir();
    File son = new File(sonFolder, "son.txt");
    addContent("This is the content of son", son);

    File grandSonFolder = new File(sonFolder, "grandSon");
    grandSonFolder.mkdir();
    File grandson = new File(grandSonFolder, "grandson.txt");
    addContent("This is the content of grandson", grandson);

    String configurationFileName = "IP-WORD.conf";
    URL url = GrokHelperTest.class.getClassLoader().getResource(configurationFileName);

    List<String> paths = GrokHelper.getAllSubPaths(wd);

    Assert.assertTrue(paths.contains(
        wd + File.separator + "son" + File.separator + "grandSon" + File.separator
            + "grandson.txt"));
    Assert.assertTrue(paths.contains(wd + File.separator + "son" + File.separator + "son.txt"));
  }

  @Test
  public void testGetAllSubPathsFromHDFS() throws Exception {

    fs = FileSystem.get(jobConf);

    String mainBasePath = base.toUri().getPath() + "/HDFS-hierarchy";

    Path mainFolder = new Path(mainBasePath);
    fs.mkdirs(mainFolder);

    Path father = new Path(mainFolder.toUri().getPath());
    fs.mkdirs(father);

    Path sonFolder = new Path(father, new Path("son"));
    fs.mkdirs(sonFolder);

    Path son = new Path(sonFolder, new Path("son.txt"));
    fs.create(son);
    addHDFSContent("This is the content of son", son);

    Path grandSonFolder = new Path(sonFolder, new Path("grandSon"));
    fs.mkdirs(grandSonFolder);

    Path grandson = new Path(grandSonFolder, new Path("grandson.txt"));
    fs.create(grandson);
    addHDFSContent("This is the content of grandson", grandson);

    List<Path> subPaths = GrokHelper.getAllSubPaths(mainFolder, jobConf);

    boolean response = false;
    for (Path currentPath : subPaths) {
      if (sonFolder.toString().equals(currentPath.toUri().getPath())) {
        response = true;
        break;
      }
    }
    Assert.assertTrue(response);
  }

  @Ignore
  @Test
  public void addPatternsToHDFS() throws Exception {
    String conf = "filter {\n" +
        "  grok {\n" +
        "    match => [\"message\", \"%{IP:ip} %{WORD:log_message}\"]\n" +
        "    add_field => [\"received_from_field\", \"%{ip}\"]\n" +
        "    patterns_dir => [\"/home/user/patterns/extra1.txt\", \"/home/user/patterns/extra2.txt\"]\n" +
        "  }\n" +
        "}";

    Map<String, Object> params = new HashMap<String, Object>();
    params.put(GrokIngestMapper.CONFIG_STRING_RUBY_PARAM, conf);
    Object response = GrokIngestMapper
        .executeScript(GrokIngestMapper.PATTERN_HANDLER_RUBY_CLASS, params,
            new ArrayList<String>());

    // Expected an array of paths
    String expected = "[\"/home/user/patterns/extra1.txt\", \"/home/user/patterns/extra2.txt\"]";

    Assert.assertEquals(expected, response.toString());

  }
}
