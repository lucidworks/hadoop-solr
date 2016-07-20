package com.lucidworks.hadoop.ingest;

import com.lucidworks.hadoop.ingest.util.EmptyEntityResolver;
import com.lucidworks.hadoop.io.LWDocument;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.StrUtils;

import static com.lucidworks.hadoop.utils.ConfigurationKeys.COLLECTION;

/**
 * Process SolrXML add commands from raw Text.  Ignores all non-add commands.
 * <p/>
 * See http://wiki.apache.org/solr/UpdateXmlMessages
 */
public class SolrXMLIngestMapper extends AbstractIngestMapper<Writable, Text> {
  private final AbstractJobFixture fixture = new AbstractJobFixture() {
    @Override
    public void init(JobConf conf) throws IOException {
      boolean override = conf.getBoolean(IngestJob.INPUT_FORMAT_OVERRIDE, false);
      if (override == false) {
        conf.setInputFormat(SequenceFileInputFormat.class);
      }// else the user has overridden the input format and we assume it is OK.
    }
  };

  private SolrXMLLoader xmlLoader;

  // For test
  public SolrXMLLoader createXmlLoader(String collection, String idField) {
    return new SolrXMLLoader(collection, idField);
  }

  @Override
  public AbstractJobFixture getFixture() {
    return fixture;
  }

  @Override
  public void configure(JobConf conf) {
    super.configure(conf);
    String idField = conf.get("idField", "id");
    String collection = conf.get(COLLECTION);
    if (collection == null) {
      throw new RuntimeException("No collection specified, aborting");
    }
    xmlLoader = new SolrXMLLoader(collection, idField);
  }

  @Override
  protected LWDocument[] toDocuments(Writable key, Text value, Reporter reporter,
      Configuration conf) throws IOException {
    try {
      Collection<LWDocument> documents = xmlLoader
          .readDocs(new ByteArrayInputStream(value.getBytes(), 0, value.getLength()),
              key.toString());

      return documents.toArray(new LWDocument[documents.size()]);
    } catch (XMLStreamException e) {
      // nocommit TODO: how to handle this exception? Throw or swallow?
      log.error("Unable to parse SolrXML from " + key.toString(), e);
      // TODO: How should we report this?
      // reporter.getCounter(Counters.)
    }
    return new LWDocument[0];

  }

  /**
   * Create LWDocument from SolrXML files.  Ignores all commands other than &lt;add&gt;.
   */
  class SolrXMLLoader {
    XMLInputFactory inputFactory;
    SAXParserFactory saxFactory;
    private final String collection, idField;

    /**
     * @param idField
     */
    public SolrXMLLoader(String collection, String idField) {
      this.collection = collection;
      this.idField = idField;
      inputFactory = XMLInputFactory.newInstance();
      EmptyEntityResolver.configureXMLInputFactory(inputFactory);
      //inputFactory.setXMLReporter(xmllog);
      try {
        // The java 1.6 bundled stax parser (sjsxp) does not currently have a thread-safe
        // XMLInputFactory, as that implementation tries to cache and reuse the
        // XMLStreamReader.  Setting the parser-specific "reuse-instance" property to false
        // prevents this.
        // All other known open-source stax parsers (and the bea ref impl)
        // have thread-safe factories.
        inputFactory.setProperty("reuse-instance", Boolean.FALSE);
      } catch (IllegalArgumentException ex) {
        // Other implementations will likely throw this exception since "reuse-instance"
        // isimplementation specific.
        log.debug(
            "Unable to set the 'reuse-instance' property for the input chain: " + inputFactory);
      }

      // Init SAX parser (for XSL):
      saxFactory = SAXParserFactory.newInstance();
      saxFactory.setNamespaceAware(true); // XSL needs this!
      EmptyEntityResolver.configureSAXParserFactory(saxFactory);
    }

    /**
     * Closes the parser when it is done or on exception
     *
     * @return
     * @throws XMLStreamException
     */
    public Collection<LWDocument> readDocs(InputStream input, String missingIdPrefix)
        throws XMLStreamException {
      XMLStreamReader parser = inputFactory.createXMLStreamReader(input);
      Collection<LWDocument> docs = Collections.emptyList();
      int event = 0;
      try {
        long docCount = 0;
        while ((event = parser.next()) != XMLStreamConstants.END_DOCUMENT) {
          switch (event) {
            case XMLStreamConstants.START_ELEMENT: {
              String currTag = parser.getLocalName();
              if (currTag.equals("add")) {
                docs = new ArrayList<LWDocument>();
              } else if (currTag.equals("doc")) {
                LWDocument doc = readDoc(parser, missingIdPrefix, docCount);
                docs.add(doc);
              }//else, ignore everything else
            }
            default: {
              //do nothing
            }
          }
        }
      } finally {
        parser.close();
      }
      return docs;
    }

    /**
     * From Solr, copied here to avoid all the other stuff!
     * Given the input stream, read a document
     *
     * @since solr 1.3
     */
    private LWDocument readDoc(XMLStreamReader parser, String missingIdPrefix, long docCount)
        throws XMLStreamException {
      LWDocument doc = createDocument();

      String attrName = "";
      double docBoost = 1.0;
      for (int i = 0; i < parser.getAttributeCount(); i++) {
        attrName = parser.getAttributeLocalName(i);
        if ("boost".equals(attrName)) {
          docBoost = Float.parseFloat(parser.getAttributeValue(i));
        } else {
          log.warn("Unknown attribute doc/@" + attrName);
        }
      }

      StringBuilder text = new StringBuilder();
      String name = null;
      double boost = 1.0f;
      boolean isNull = false;
      String update = null;
      Map<String, Map<String, Object>> updateMap = null;
      boolean complete = false;
      while (!complete) {
        int event = parser.next();
        switch (event) {
          // Add everything to the text
          case XMLStreamConstants.SPACE:
          case XMLStreamConstants.CDATA:
          case XMLStreamConstants.CHARACTERS:
            text.append(parser.getText());
            break;

          case XMLStreamConstants.END_ELEMENT:
            if ("doc".equals(parser.getLocalName())) {
              complete = true;
              break;
            } else if ("field".equals(parser.getLocalName())) {
              Object v = isNull ? null : text.toString();
              if (update != null) {
                if (updateMap == null) {
                  updateMap = new HashMap<String, Map<String, Object>>();
                }
                Map<String, Object> extendedValues = updateMap.get(name);
                if (extendedValues == null) {
                  extendedValues = new HashMap<String, Object>(1);
                  updateMap.put(name, extendedValues);
                }
                Object val = extendedValues.get(update);
                if (val == null) {
                  extendedValues.put(update, v);
                } else {
                  // multiple val are present
                  if (val instanceof List) {
                    List list = (List) val;
                    list.add(v);
                  } else {
                    List<Object> values = new ArrayList<Object>();
                    values.add(val);
                    values.add(v);
                    extendedValues.put(update, values);
                  }
                }
                break;
              }
              //multivalued fields are dealt with later via the update map
              //TODO: fix boosts.
              doc.addField(name, v.toString());
            /*if (boost != 1) {
              doc.boosts.put(name, boost * docBoost);
            }*/
              boost = 1.0f;
            }
            break;

          case XMLStreamConstants.START_ELEMENT:
            text.setLength(0);
            String localName = parser.getLocalName();
            if (!"field".equals(localName)) {
              log.warn("unexpected XML tag doc/" + localName);
              throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                  "unexpected XML tag doc/" + localName);
            }
            boost = 1.0f;
            update = null;
            String attrVal = "";
            for (int i = 0; i < parser.getAttributeCount(); i++) {
              attrName = parser.getAttributeLocalName(i);
              attrVal = parser.getAttributeValue(i);
              if ("name".equals(attrName)) {
                name = attrVal;
              } else if ("boost".equals(attrName)) {
                boost = Float.parseFloat(attrVal);
              } else if ("null".equals(attrName)) {
                isNull = StrUtils.parseBoolean(attrVal);
              } else if ("update".equals(attrName)) {
                update = attrVal;
              } else {
                log.warn("Unknown attribute doc/field/@" + attrName);
              }
            }
            break;
        }
      }

      if (updateMap != null) {
        for (Map.Entry<String, Map<String, Object>> entry : updateMap.entrySet()) {
          name = entry.getKey();
          Map<String, Object> value = entry.getValue();
          //doc.addField(name, value, 1.0f);
          doc.addField(name, value);
          //doc.fields.put(name, value.toString());
          //doc.boosts.put(name, docBoost);
        }
      }

      doc = doc.checkId(idField, missingIdPrefix + docCount);
      return doc;
    }
  }

}
