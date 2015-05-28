package wordcount;

import org.apache.commons.lang.StringUtils;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class SplitSentenceBolt extends BaseBasicBolt {
	private static final long serialVersionUID = -2758420048670171779L;

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		String[] words = StringUtils.split(input.getString(0));       
	     for (String word : words) {
	         collector.emit(new Values(word));
	     }
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}

}
