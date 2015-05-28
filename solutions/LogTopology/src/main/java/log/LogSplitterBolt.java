package log;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class LogSplitterBolt extends BaseBasicBolt {
	private static final long serialVersionUID = 6042830233153585693L;

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		try {
			String date = input.getString(0).substring(0, 23);
			String errormessage = input.getString(0).substring(24);
			String [] words = errormessage.split("\\s+");
//			System.out.println("********" + words[0] + "********");
			String outputMessage = input.getString(0).substring(24 + words[0].length());
//			System.out.println("***" + outputMessage + "****");
			collector.emit(new Values(date, words[0], outputMessage)); 
		} catch (Exception e) {
			//e.printStackTrace();
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("date","loglevel", "message"));
	}

}
