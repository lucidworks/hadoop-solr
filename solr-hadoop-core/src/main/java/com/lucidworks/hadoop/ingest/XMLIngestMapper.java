package com.lucidworks.hadoop.ingest;

import com.lucidworks.hadoop.cache.DistributedCacheHandler;
import com.lucidworks.hadoop.io.LWDocument;
import com.lucidworks.hadoop.io.LWDocumentProvider;
import com.lucidworks.hadoop.io.XMLInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMResult;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamSource;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.io.IOException;
import java.io.StringReader;
import java.util.LinkedList;
import java.util.List;

public class XMLIngestMapper extends AbstractIngestMapper<Writable, Text> {

  private transient static Logger log = LoggerFactory.getLogger(XMLIngestMapper.class);

  public static final String LWW_XSLT = "lww.xslt";

  private final AbstractJobFixture fixture = new AbstractJobFixture() {
    @Override
    public void init(JobConf conf) throws IOException {
      super.init(conf);
      String xslt = conf.get(LWW_XSLT);
      if (xslt != null) {
        DistributedCacheHandler.addFileToCache(conf, new Path(xslt), LWW_XSLT);
      }
      boolean override = conf.getBoolean(IngestJob.INPUT_FORMAT_OVERRIDE, false);
      if (!override) {
        conf.setInputFormat(XMLInputFormat.class);
        if (conf.get(XMLInputFormat.START_TAG_KEY) == null || conf.get(XMLInputFormat.END_TAG_KEY) == null) {
          throw new RuntimeException("Missing XMLInputFormat Tags " + XMLInputFormat.START_TAG_KEY + " and/or " + XMLInputFormat.END_TAG_KEY);
        }
      } // else the user has overridden the input format and we assume it is OK.
    }
  };

  protected DocumentBuilder docBuilder;
  protected String docXPath;
  protected XPathExpression docXPathExpr;
  protected XPathExpression idXPathExpr;
  protected String includeParentAttrsPrefix = null;
  protected Transformer xsltTransformer = null;

  @Override
  public void configure(JobConf conf) {
    super.configure(conf);

    try {
      DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.newInstance();
      docBuilderFactory.setIgnoringElementContentWhitespace(true);
      docBuilder = docBuilderFactory.newDocumentBuilder();
    } catch (ParserConfigurationException e) {
      throw new RuntimeException("Failed to get a DocumentBuilder due to: " + e, e);
    }

    // XSLt enabled?
    String xslt = DistributedCacheHandler.getFileFromCache(conf, LWW_XSLT);
    if (xslt != null) {
      xslt = xslt.trim();
      if (!xslt.isEmpty()) {
        try {
          TransformerFactory factory = TransformerFactory.newInstance();
          xsltTransformer = factory.newTransformer(new StreamSource(new StringReader(xslt)));
        } catch (TransformerConfigurationException e) {
          throw new RuntimeException("Failed to initialize XSLt Transformer due to: " + e + ";\nXSL: " + xslt, e);
        }
      }
    }

    this.docXPath = conf.get("lww.xml.docXPathExpr", "/");
    XPath xpath = XPathFactory.newInstance().newXPath();
    try {
      docXPathExpr = xpath.compile(docXPath);
    } catch (XPathExpressionException e) {
      throw new RuntimeException("Failed to compile xpath '" + docXPath + "' due to: " + e, e);
    }

    String idXPath = conf.get("lww.xml.idXPathExpr");
    if (idXPath != null) {
      try {
        idXPathExpr = xpath.compile(idXPath);
      } catch (XPathExpressionException e) {
        throw new RuntimeException("Failed to compile ID xpath '" + idXPath + "' due to: " + e, e);
      }
    }

    includeParentAttrsPrefix = conf.get("lww.xml.includeParentAttrsPrefix");
  }

  @Override
  protected LWDocument[] toDocuments(Writable key, Text text, Reporter reporter, Configuration configuration) throws IOException {
    LWDocument[] docs = null;
    try {
      docs = toDocumentsImpl(key, text);
    } catch (Exception exc) {
      log.error("Failed to process XML " + key + " due to: " + exc, exc);
      reporter.incrCounter("XMLIngestMapper", "BadDocs", 1);
    }

    if (docs != null && docs.length > 0) {
      reporter.incrCounter("XMLIngestMapper", "DocsCreated", docs.length);
    } else {
      log.warn("No documents added in: " + key);
      docs = new LWDocument[0];
    }
    return docs;
  }

  protected LWDocument[] toDocumentsImpl(Writable key, Text text) throws Exception {

    String dataText = text.toString();
    InputSource input = new InputSource(new StringReader(dataText));

    Document doc;
    try {
      doc = docBuilder.parse(input);
    } catch (SAXException e) {
      dataText = "<" + dataText + ">";
      log.warn("Trying to process [" + key + "] again.");
      input = new InputSource(new StringReader(dataText));
      doc = docBuilder.parse(input);
    }

    Node docNode = doc;
    if (xsltTransformer != null) {
      DOMSource in = new DOMSource(doc);
      DOMResult out = new DOMResult();
      xsltTransformer.transform(in, out);
      docNode = out.getNode();
      xsltTransformer.reset();
      log.info("transformed doc into: " + docNode);
    }

    NodeList nodeList = (NodeList) docXPathExpr.evaluate(docNode, XPathConstants.NODESET);

    int numDocs = nodeList.getLength();
    log.info("Found " + numDocs + " docs using XPath: " + docXPath);

    List<LWDocument> docs = new LinkedList<>();
    String keyStr = key.toString();
    for (int i = 0; i < numDocs; i++) {
      Node child = nodeList.item(i);
      if (child.getNodeType() == Node.ELEMENT_NODE) {
        LWDocument pDoc = processElement(keyStr, (Element) child, i);
        if (pDoc != null) {
          docs.add(pDoc);
        }
      }
    }

    return docs.toArray(new LWDocument[0]);
  }

  protected LWDocument processElement(String keyStr, Element elm, int docIndex) {

    String docId = null;
    if (idXPathExpr != null) {
      try {
        docId = (String) idXPathExpr.evaluate(elm, XPathConstants.STRING);
      } catch (XPathExpressionException e) {
        log.error("Failed to evaluate ID XPath for doc " + docIndex + " in " + keyStr + " due to: " + e);
      }
    }

    if (docId == null) {
      docId = keyStr + "-" + docIndex;
    }

    LWDocument pDoc = LWDocumentProvider.createDocument();
    pDoc.setId(docId);

    // pull parent node attributes if desired
    if (includeParentAttrsPrefix != null) {
      Node parentNode = elm.getParentNode();
      if (parentNode != null && parentNode.getNodeType() == Node.ELEMENT_NODE) {
        Element parent = (Element) parentNode;
        NamedNodeMap attrs = parent.getAttributes();
        if (attrs != null) {
          for (int a = 0; a < attrs.getLength(); a++) {
            Attr attr = (Attr) attrs.item(a);
            pDoc.addField(includeParentAttrsPrefix + attr.getName(), attr.getValue());
          }
        }
      }
    }

    addFieldsToDoc("", elm, pDoc);

    return pDoc;
  }

  protected void addFieldsToDoc(String fieldPrefix, Element elm, LWDocument pDoc) {
    NamedNodeMap attrs = elm.getAttributes();
    if (attrs != null) {
      for (int a = 0; a < attrs.getLength(); a++) {
        Attr attr = (Attr) attrs.item(a);
        String attrName = attr.getNodeName();
        String fieldName = fieldPrefix.isEmpty() ? attrName : fieldPrefix + "." + attrName;
        pDoc.addField(fieldName, attr.getValue());
      }
    }

    NodeList childNodes = elm.getChildNodes();
    if (childNodes != null) {
      for (int c = 0; c < childNodes.getLength(); c++) {
        Node child = childNodes.item(c);
        if (child.getNodeType() == Node.ELEMENT_NODE) {
          Element childElm = (Element) child;
          String childName = childElm.getNodeName();
          String fieldName = fieldPrefix.isEmpty() ? childName : fieldPrefix + "." + childName;
          addFieldsToDoc(fieldName, childElm, pDoc);
        } else if (child.getNodeType() == Node.TEXT_NODE || child.getNodeType() == Node.CDATA_SECTION_NODE) {
          String text = child.getNodeValue().trim();
          if (!text.isEmpty()) {
            pDoc.addField(fieldPrefix, text);
          }
        }
      }
    }
  }

  @Override
  public AbstractJobFixture getFixture() {
    return fixture;
  }

}