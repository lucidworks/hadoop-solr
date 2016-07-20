package com.lucidworks.hadoop.utils;

import com.lucidworks.hadoop.io.LWDocumentWritable;
import com.lucidworks.hadoop.io.impl.LWMockDocument;
import java.util.HashMap;
import java.util.Map;
import org.junit.Ignore;

@Ignore
public class TestUtils {

  public static LWDocumentWritable createLWDocumentWritable(String id, String... keyValues) {
    Map<String, String> fields = new HashMap<>();
    for (int i = 0; i < keyValues.length; i += 2) {
      fields.put(keyValues[i], keyValues[i + 1]);
    }
    LWMockDocument doc = new LWMockDocument(id, fields);
    return new LWDocumentWritable(doc);
  }
}
