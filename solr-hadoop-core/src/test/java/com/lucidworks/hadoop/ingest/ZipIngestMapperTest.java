package com.lucidworks.hadoop.ingest;

import com.lucidworks.hadoop.io.LWDocument;
import com.lucidworks.hadoop.io.LWDocumentWritable;
import com.lucidworks.hadoop.utils.ZipFileRecordReader;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mrunit.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.Enumeration;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;

public class ZipIngestMapperTest extends BaseIngestMapperTestCase {
  private transient static Logger log = LoggerFactory.getLogger(ZipIngestMapperTest.class);
  private MapDriver<Text, BytesWritable, Text, LWDocumentWritable> mapDriver;

  @Before
  public void setUp() throws IOException {
    mapDriver = new MapDriver<Text, BytesWritable, Text, LWDocumentWritable>();
    ZipIngestMapper mapper = new ZipIngestMapper();
    mapDriver.setConfiguration(createConf());

    mapDriver.setMapper(mapper);
    Configuration conf = mapDriver.getConfiguration();
    setupCommonConf(conf);
    mapper.getFixture().init(new JobConf(conf));
  }

  @Test
  public void testIngestFull() throws IOException {
    String testStringOne = "test document one which has a few junk terms";
    String testStringTwo = "test document two of different length";
    Configuration conf;
    conf = mapDriver.getConfiguration();
    FileSystem fileSystem = FileSystem.getLocal(conf);
    Path dir = new Path(fileSystem.getWorkingDirectory(), "build");
    Path sub = new Path(dir, "ZIMT");
    Path tempDir = new Path(sub, "tmp-dir");
    fileSystem.mkdirs(tempDir);
    Path path = new Path(tempDir, "zip_test");
    fileSystem.mkdirs(path);

    Path zipFile = new Path(path, "zip" + File.separator + "test.zip");
    ZipOutputStream zipOut = new ZipOutputStream(fileSystem.create(zipFile, true));
    ZipEntry e = new ZipEntry("/tmp/20020901/mytext1.txt");
    zipOut.putNextEntry(e);

    byte[] data1 = testStringOne.getBytes(Charset.forName("US-ASCII"));
    zipOut.write(data1, 0, data1.length);
    zipOut.closeEntry();

    ZipEntry e2 = new ZipEntry("/tmp/20020901/mytext2.txt");
    zipOut.putNextEntry(e2);

    byte[] data2 = testStringTwo.getBytes(Charset.forName("US-ASCII"));
    zipOut.write(data2, 0, data2.length);

    zipOut.closeEntry();
    // put in a PDF
    URL resource = ZipIngestMapperTest.class.getClassLoader().getResource("dir" + File.separator + "test0.pdf");
    Assert.assertNotNull(resource);
    ZipEntry e3 = new ZipEntry("/tmp/20020901/test0.pdf");
    zipOut.putNextEntry(e3);
    byte[] data3 = IOUtils.toByteArray(resource);
    zipOut.write(data3, 0, data3.length);

    zipOut.close();

    FileSplit fileSplit = new FileSplit(zipFile, 0, data1.length + data2.length, new JobConf(conf));
    ZipFileRecordReader zipFileRecordReader = new ZipFileRecordReader(fileSplit, conf);
    Text key = new Text();
    LWDocument doc;
    BytesWritable value = new BytesWritable();
    zipFileRecordReader.next(key, value);
    Assert.assertEquals("/tmp/20020901/mytext1.txt", key.toString());
    Assert.assertEquals(testStringOne, new String(value.getBytes(), Charset.forName("UTF-8")));
    mapDriver.withInput(key, value);
    List<Pair<Text, LWDocumentWritable>> run = mapDriver.run();
    Assert.assertEquals(1, run.size());
    checkDoc(run.get(0).getSecond().getLWDocument(), testStringOne);

    zipFileRecordReader.next(key, value);
    Assert.assertEquals("/tmp/20020901/mytext2.txt", key.toString());
    Assert.assertEquals(testStringTwo, new String(value.getBytes(), Charset.forName("UTF-8")));
    mapDriver.withInput(key, value);
    run = mapDriver.run();
    Assert.assertEquals(2, run.size());
    checkDoc(run.get(1).getSecond().getLWDocument(), testStringTwo);

    zipFileRecordReader.next(key, value);
    Assert.assertEquals("/tmp/20020901/test0.pdf", key.toString());
    mapDriver.resetExpectedCounters();
    mapDriver.withInput(key, value);
    run = mapDriver.run(true);
    Assert.assertEquals(3, run.size());
    // TODO: something wrong here
    checkDoc(run.get(2).getSecond().getLWDocument(), null);

  }

  protected void checkDoc(LWDocument doc, String expectedText) {
    Assert.assertNotNull("document is null", doc);
    Assert.assertNotNull(doc.getId());
    // TODO: Check Fields

  }

  @Test
  public void testZipData() {

    final StringBuilder sb = new StringBuilder();
    sb.append("Test String");

    String zipFilePath = "/tmp/empty_dirs.zip";

    try {
      FileOutputStream fileOutputStream = new FileOutputStream(zipFilePath);
      ZipOutputStream zipOut = new ZipOutputStream(fileOutputStream);

      ZipEntry e = new ZipEntry("/tmp/20020901/mytext.txt");
      zipOut.putNextEntry(e);

      byte[] data = sb.toString().getBytes();
      zipOut.write(data, 0, data.length);

      zipOut.closeEntry();
      zipOut.close();
      fileOutputStream.close();
      log.info("Done..");
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }

    try {
      ZipFile zipFile = new ZipFile("/tmp/empty_dirs.zip");
      Enumeration<?> enu = zipFile.entries();
      while (enu.hasMoreElements()) {
        ZipEntry zipEntry = (ZipEntry) enu.nextElement();

        String name = zipEntry.getName();
        long size = zipEntry.getSize();

        long compressedSize = zipEntry.getCompressedSize();
        System.out
            .printf("name: %-20s | size: %6d | compressed size: %6d\n", name, size, compressedSize);

        File file = new File(name);
        if (name.endsWith("/")) {
          file.mkdirs();
          continue;
        }

        File parent = file.getParentFile();
        if (parent != null) {
          parent.mkdirs();
        }

        InputStream is = zipFile.getInputStream(zipEntry);
        FileOutputStream fos = new FileOutputStream(file);
        byte[] bytes = new byte[1024];
        int length;
        while ((length = is.read(bytes)) >= 0) {
          // System.out.println(Bytes.toString(bytes));
          fos.write(bytes, 0, length);
        }
        is.close();
        fos.close();

      }
      zipFile.close();

      FileInputStream fis = new FileInputStream("/tmp/20020901/mytext.txt");
      InputStreamReader in = new InputStreamReader(fis, "UTF-8");
      BufferedReader bf = new BufferedReader(in);
      String line = null;
      while ((line = bf.readLine()) != null) {
        Assert.assertEquals("Test String", line);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
