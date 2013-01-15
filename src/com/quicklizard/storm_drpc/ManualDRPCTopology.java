/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.quicklizard.storm_drpc;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.drpc.DRPCSpout;
import backtype.storm.drpc.ReturnResults;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 *
 * @author zohar
 */
public class ManualDRPCTopology {

  // Unique Identifier for DRPC stream
  static final String REQUEST_STREAM_ID = WordCountBolt.class.getName() + "/request-stream";

  public static void main(String[] args) throws InterruptedException {
    TopologyBuilder builder = new TopologyBuilder();
    // Use LocalDRPC in local mode
    LocalDRPC drpc = new LocalDRPC();
    
    // Create a DRPC spout
    DRPCSpout drpcSpout = new DRPCSpout("drpc-query", drpc);
    builder.setSpout("drpc-input", drpcSpout);

    // Set first bold in DRPC to emit tuples on a different stream
    builder.setBolt("prepare-drpc", new StreamChangerBolt(REQUEST_STREAM_ID, "args", "return-info"), 1)
           .noneGrouping("drpc-input");
    
    // Create random sentence spout on default stream
    builder.setSpout("spout", new RandomSentenceSpout(), 2);
    // Add word splitter bolt that splits random sentence into words
    builder.setBolt("split", new SplitSentenceBolt(), 2)
           .shuffleGrouping("spout");
    
    // Add word counter bolt that receives splitted words and counts occurances
    // Note that use of allGrouping with drpc bolt to ensure it is placed before each count bolt in the toplogy
    builder.setBolt("count", new WordCountBolt(REQUEST_STREAM_ID), 2)
           .fieldsGrouping("split", new Fields("word")) //place after word splitter bolt in default stream
           .allGrouping("prepare-drpc", REQUEST_STREAM_ID); //place after DRPC stream change bolt in DRPC stream
    
    // Add DRPC return bolt that receives tuples from count bolt and returns them to DRPC
    builder.setBolt("return", new ReturnResults(), 3)
           .noneGrouping("count", REQUEST_STREAM_ID);
    
    Config conf = new Config();
    conf.setDebug(false);
    conf.setMaxTaskParallelism(3);

    LocalCluster cluster = new LocalCluster();
    cluster.submitTopology("word-count", conf, builder.createTopology());

    Thread.sleep(10000);
    
    System.out.println("+++++++++++++++++++++++++++++++++++++");
    System.out.println(drpc.execute("drpc-query", "test"));
    cluster.shutdown();
  }
  
}
