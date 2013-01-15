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

  static final String REQUEST_STREAM_ID = WordCountBolt.class.getName() + "/request-stream";
  
  public static class DRPCReceiverBolt extends BaseBasicBolt {

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("return-info"));
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
      Object retInfo = tuple.getValue(1);
      collector.emit(new Values(retInfo));
    }

  }
  
  public static class DRPCListenerBolt extends BaseBasicBolt {
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("result", "return-info"));
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
      String counts = tuple.getString(2);
      Object retInfo = tuple.getValue(3);
      collector.emit(new Values(counts, retInfo));
    }

  }

  public static void main(String[] args) throws InterruptedException {
    TopologyBuilder builder = new TopologyBuilder();
    LocalDRPC drpc = new LocalDRPC();
    
    DRPCSpout drpcSpout = new DRPCSpout("drpc-query", drpc);
    builder.setSpout("drpc-input", drpcSpout);

    builder.setBolt("prepare-drpc", new StreamChangerBolt(REQUEST_STREAM_ID, "args", "return-info"), 1)
           .allGrouping("drpc-input");
    
    builder.setSpout("spout", new RandomSentenceSpout(), 2);
    builder.setBolt("split", new SplitSentenceBolt(), 2)
           .shuffleGrouping("spout");
    
    builder.setBolt("count", new WordCountBolt(REQUEST_STREAM_ID), 2)
           .fieldsGrouping("split", new Fields("word"))
           .allGrouping("prepare-drpc", REQUEST_STREAM_ID);
    
    builder.setBolt("return", new ReturnResults(), 3)
           .allGrouping("count", REQUEST_STREAM_ID);
    
    Config conf = new Config();
    conf.setDebug(false);
    conf.setMaxTaskParallelism(3);

    LocalCluster cluster = new LocalCluster();
    cluster.submitTopology("word-count", conf, builder.createTopology());

    Thread.sleep(10000);
    
    System.out.println("+++++++++++++++++++++++++++++++++++++");
    System.out.println(drpc.execute("drpc-query", null));
    cluster.shutdown();
  }
  
  public static void main_old(String[] args) throws InterruptedException {
    TopologyBuilder builder = new TopologyBuilder();
    
    LocalDRPC drpc = new LocalDRPC();
    DRPCSpout drpcSpout = new DRPCSpout("drpc_count", drpc);
    builder.setSpout("drpc", drpcSpout);
    builder.setBolt("receive_drpc", new DRPCReceiverBolt(), 3)
            .noneGrouping("drpc");
    
    builder.setSpout("spout", new RandomSentenceSpout(), 2);
    builder.setBolt("split", new SplitSentenceBolt(), 2)
           .shuffleGrouping("spout");
    builder.setBolt("count", new WordCountBolt(REQUEST_STREAM_ID), 2)
           .fieldsGrouping("split", new Fields("word"))
           .noneGrouping("receive_drpc");

    builder.setBolt("listen_drpc", new DRPCListenerBolt(), 3)
            .shuffleGrouping("count");
    builder.setBolt("return", new ReturnResults(), 3)
            .shuffleGrouping("listen_drpc");
    
    
    Config conf = new Config();
    conf.setDebug(false);
    conf.setMaxTaskParallelism(3);

    LocalCluster cluster = new LocalCluster();
    cluster.submitTopology("word-count", conf, builder.createTopology());

    Thread.sleep(10000);
    
    System.out.println("===================================");
    System.out.println(drpc.execute("drpc_count", "aaa"));
    cluster.shutdown();
  }  
}
