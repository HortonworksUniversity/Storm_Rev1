package log;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

public class LocalFilePersistenceBolt extends BaseBasicBolt {

  private final String filePath;
  private BufferedWriter writer;

  public LocalFilePersistenceBolt(String filePath) {
    this.filePath = filePath;
  }

  @Override
  public void prepare(Map stormConf, TopologyContext context) {
    try {
      writer = new BufferedWriter(new FileWriter(new File(filePath)));
    }
    catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  @Override
  public void cleanup() {
    if (writer != null) {
      try {
        writer.close();
      }
      catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  @Override
  public void execute(Tuple input, BasicOutputCollector collector) {
    try {
      writer.write(input.getStringByField("message"));
      writer.newLine();
    }
    catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
  }
}
