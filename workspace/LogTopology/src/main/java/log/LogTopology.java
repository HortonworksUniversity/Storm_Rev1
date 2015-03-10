package log;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import org.apache.commons.cli.*;
import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;

public class LogTopology {

  public static void main(String[] args) throws Exception {
    Options options = new Options();
    options.addOption(new Option("local", false, "Run locally?"));
    CommandLineParser parser = new BasicParser();
    CommandLine cmd = parser.parse(options, args);
    boolean local = cmd.hasOption("local");

    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout("spout", new LogFileSpout("node1.log"), 3);

    builder.setBolt("logsplitter",
        new LogSplitterBolt(),
        8).shuffleGrouping("spout");

    builder.setBolt("logfilter",
        new LogFilterBolt("ERROR"),
        4).shuffleGrouping("logsplitter");

    builder = configurePeristenceBolt(builder, local);

    Config conf = new Config();
    if (local) {
      conf.setDebug(true);
      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("logfilter", conf, builder.createTopology());

      Thread.sleep(10000);
      cluster.shutdown();
      System.exit(0);
    }
    else {
      conf.setNumWorkers(3);
      StormSubmitter.submitTopology("error-log", conf, builder.createTopology());
    }
  }

  public static TopologyBuilder configurePeristenceBolt(TopologyBuilder builder, boolean local) {
    if (local) {
      builder.setBolt("localfile",
          new LocalFilePersistenceBolt("/tmp/errorlog.txt"),
          1).globalGrouping("logfilter");
    }
    else {
      HdfsBolt hdfsBolt = new HdfsBolt()
          .withFsUrl("hdfs://namenode:8020")
          .withFileNameFormat(new DefaultFileNameFormat().withPath("/user/root/errorlog"))
          .withSyncPolicy(new CountSyncPolicy(1))
          .withRotationPolicy(new FileSizeRotationPolicy(5.0f, FileSizeRotationPolicy.Units.MB))
          .withRecordFormat(new DelimitedRecordFormat().withFieldDelimiter("|").withFields(new Fields("message")));
      builder.setBolt("hdfs", hdfsBolt, 2).fieldsGrouping("logfilter", new Fields("loglevel"));
    }

    return builder;
  }
}
