package sahibul.spouts;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.util.Map;

public class LearnStormSpout extends BaseRichSpout {

    private SpoutOutputCollector spoutOutputCollector;
    private String[] sentences = {
            "Sahibul Nuzul Firdaus",
            "Sahibul Nuzul",
            "Sahibul",
            "si_syin",
            "Java",
            "Apache Storm",
            "Storm",
            "Apache",
            "Big Data",
            "Data"
    };
    boolean isCompleted;

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        // open the spout
        this.spoutOutputCollector = spoutOutputCollector;
    }

    @Override
    public void nextTuple() {
        // storm cluster akan menjalankan method ini secara continue
        // untuk mengalirkan tuple nya
        if (!isCompleted) {
            for (String sentence: sentences) {
                for (String word: sentence.split(" ")) {
                    spoutOutputCollector.emit(new Values(word));
                }
            }
            isCompleted = true;
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        // memancarkan tuple dengan set field nya adalah "word"
        outputFieldsDeclarer.declare(new Fields("word"));
    }
}
