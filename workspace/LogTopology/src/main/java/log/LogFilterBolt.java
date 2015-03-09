package log;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class LogFilterBolt extends BaseBasicBolt {

	private static final long serialVersionUID = 5044797426107566662L;
	String _filterString = "";
	
	public LogFilterBolt(String filterString) {
		_filterString = filterString;
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		//String date = input.getString(0);
		String loglevel = input.getString(1);
		String message = input.getString(2);
		if(loglevel.equals(_filterString)) {
			collector.emit(new Values(message));
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("message"));
	}

}
