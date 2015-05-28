package wordcount;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class WordCountBolt extends BaseBasicBolt {
	private static final long serialVersionUID = -7692196708975138876L;
	Map<String, Integer> counts = new HashMap<String, Integer>();

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		String word = input.getString(0);
	       Integer count = counts.get(word);
	       if (count == null)
	         count = 0;
	       count++;
	       counts.put(word, count);
	       collector.emit(new Values(word, count));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word", "count"));
	}

}
