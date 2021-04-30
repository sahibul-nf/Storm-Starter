package sahibul.bolts;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

public class LearnStormBolt extends BaseBasicBolt {

  Map<String, Integer> counters = new HashMap<String, Integer>();

  @Override
  public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
    // mengambil field "word" dari input tuple
    String word = tuple.getStringByField("word");

    if (!counters.containsKey(word)) {
      counters.put(word, 1);
    } else {
      counters.put(word, counters.get(word) + 1);
    }
  }

  @Override
  public void cleanup() {
    // print nilai atau isi dari field "word" ke console
    String msg = "\nHasil hitung jumlah kata: ☺";
    System.out.println(msg);
    for (Map.Entry<String, Integer> entry : counters.entrySet()) {
      System.out.println(entry.getKey() + " - " + entry.getValue());
    }
    System.out.println();
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    // method yang auto generate, hasil dari extends class BaseBasicBolt
  }
}
