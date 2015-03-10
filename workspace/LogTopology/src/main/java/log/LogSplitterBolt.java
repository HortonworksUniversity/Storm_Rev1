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
      String message = input.getStringByField("message");
      String date = message.substring(0, 23);
      String errormessage = message.substring(24);
      String[] words = errormessage.split("\\s+");
      collector.emit(new Values(date, words[0], words[1], message));
    }
    catch (IndexOutOfBoundsException e) {
      //Invalid record, ignore
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("date", "loglevel", "component", "message"));
  }

}
