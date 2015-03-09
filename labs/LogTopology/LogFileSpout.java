package log;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Values;

public class LogFileSpout extends BaseRichSpout {

	private static final long serialVersionUID = -6176746043688503098L;
	
	SpoutOutputCollector _collector;
	String _fileName;
	BufferedReader input;
	
	public LogFileSpout(String fileName) {
		_fileName = fileName;
		InputStream is = ClassLoader.getSystemResourceAsStream(fileName);
		InputStreamReader isr = new InputStreamReader(is);
		input = new BufferedReader(isr);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		_collector = collector;
	}

	@Override
	public void nextTuple() {
		try {
			String line = input.readLine();
			_collector.emit(new Values(line));
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}

}
