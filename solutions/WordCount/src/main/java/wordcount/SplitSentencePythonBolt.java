package wordcount;

import java.util.Map;

import backtype.storm.task.ShellBolt;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;

public class SplitSentencePythonBolt extends ShellBolt implements IRichBolt {
	private static final long serialVersionUID = 8481684837822417675L;

	public SplitSentencePythonBolt() {
	      super("python", "splitsentence.py");
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
