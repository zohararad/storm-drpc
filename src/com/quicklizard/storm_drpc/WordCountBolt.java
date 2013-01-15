package com.quicklizard.storm_drpc;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author zohar
 */
public class WordCountBolt extends BaseBasicBolt {
  Map<String, Integer> counts = new HashMap<String, Integer>();
  
  private String _targetStreamId;

  public WordCountBolt(String targetStreamId) {
    _targetStreamId = targetStreamId;
  }
  
  @Override
  public void execute(Tuple tuple, BasicOutputCollector collector) {
    if (tuple.getSourceStreamId().equals(_targetStreamId)){
      Object retInfo = tuple.getValue(1);
      collector.emit(_targetStreamId, new Values(counts.toString(), retInfo));
    } else {
      String word = tuple.getString(0);
      Integer count = counts.get(word);
      if(count == null){
        count = 0;
      }
      count++;
      counts.put(word, count);
      collector.emit(new Values(word, count));
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    // output fields on default stream
    declarer.declare(new Fields("word", "count"));
    // output fields on drpc stream
    declarer.declareStream(_targetStreamId, new Fields("counts", "return-info"));
  }
}
