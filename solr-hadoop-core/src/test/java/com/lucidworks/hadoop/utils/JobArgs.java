package com.lucidworks.hadoop.utils;


import com.google.common.collect.ObjectArrays;

import java.util.ArrayList;
import java.util.List;

public class JobArgs {

  public JobArgs() {
  }

  private String jobName;
  private String className;
  private String collection;
  private String input;
  private String conf;
  private String solrServer;
  private String zkString;
  private String outputFormat;
  private String reducersClass;
  private String reducersAmount;
  private String[] optional;

  public JobArgs withDArgs(String... optional) {
    this.optional = optional;
    return this;
  }

  public JobArgs withJobName(String jobName) {
    this.jobName = jobName;
    return this;
  }

  public JobArgs withClassname(String className) {
    this.className = className;
    return this;
  }

  public JobArgs withCollection(String collection) {
    this.collection = collection;
    return this;
  }

  public JobArgs withInput(String input) {
    this.input = input;
    return this;
  }

  public JobArgs withConf(String conf) {
    this.conf = conf;
    return this;
  }

  public JobArgs withSolrServer(String solrServer) {
    this.solrServer = solrServer;
    return this;
  }

  public JobArgs withZkString(String zkString) {
    this.zkString = zkString;
    return this;
  }

  public JobArgs withOutputFormat(String outputFormat) {
    this.outputFormat = outputFormat;
    return this;
  }

  public JobArgs withReducersClass(String reducersClass) {
    this.reducersClass = reducersClass;
    return this;
  }

  public JobArgs withReducersAmount(String reducersAmount) {
    this.reducersAmount = reducersAmount;
    return this;
  }

  public String[] getJobArgs() {

    List<String> args = new ArrayList<>();

    if (jobName != null) {
      args.add("-jn");
      args.add(jobName);
    }

    if (className != null) {
      args.add("-cls");
      args.add(className);
    }

    if (collection != null) {
      args.add("-c");
      args.add(collection);
    }

    if (input != null) {
      args.add("-i");
      args.add(input);
    }

    if (solrServer != null) {
      args.add("-s");
      args.add(solrServer);
    }
    if (zkString != null) {
      args.add("-zk");
      args.add(zkString);
    }

    args.add("-of");
    if (outputFormat != null) {
      args.add(outputFormat);
    } else {
      args.add(IngestJobMockMapRedOutFormat.class.getName());
    }

    if (reducersClass != null) {
      args.add("-redcls");
      args.add(reducersClass);
    }

    if (reducersAmount != null) {
      args.add("-ur");
      args.add(reducersAmount);
    }

    if (conf != null) {
      args.add("--conf");
      args.add(conf);
    }

    String[] stockArr = new String[args.size()];
    stockArr = args.toArray(stockArr);

    if (optional != null) {
      return ObjectArrays.concat(optional, stockArr, String.class);
    }

    return stockArr;
  }
}
