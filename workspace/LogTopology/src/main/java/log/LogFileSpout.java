package log;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;

public class LogFileSpout extends BaseRichSpout {

  private static final long serialVersionUID = -6176746043688503098L;

  SpoutOutputCollector collector;
  String fileName;
  BufferedReader input;

  public LogFileSpout(String fileName) {
    this.fileName = fileName;
  }

  @SuppressWarnings("rawtypes")
  @Override
  public void open(Map conf, TopologyContext context,
                   SpoutOutputCollector collector) {
    InputStream is = ClassLoader.getSystemResourceAsStream(fileName);
    InputStreamReader isr = new InputStreamReader(is);
    this.input = new BufferedReader(isr);
    this.collector = collector;
  }

  @Override
  public void nextTuple() {
    try {
      String line = input.readLine();
      if (line != null) {
        collector.emit(new Values(line));
      }
      else {
        Thread.sleep(10);
      }
    }
    catch (Exception e) {
      e.printStackTrace();

    }

  }

  @Override
  public void close() {
    if (input != null) {
      try {
        input.close();
      }
      catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("message"));

  }

}
