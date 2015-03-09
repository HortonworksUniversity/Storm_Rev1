package log;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

public class LogTopology {

  public static void main(String[] args) {
    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout("spout", new LogFileSpout("node1.log"), 3);

    builder.setBolt("logsplitter",
        new LogSplitterBolt(),
        8).shuffleGrouping("spout");

    builder.setBolt("logfilter",
        new LogFilterBolt("ERROR"),
        12).shuffleGrouping("logsplitter");

    Config conf = new Config();
    conf.setMaxTaskParallelism(3);
    conf.setDebug(true);
    LocalCluster cluster = new LocalCluster();
    cluster.submitTopology("logfilter", conf, builder.createTopology());
    try {
      Thread.sleep(10000);
    }
    catch (InterruptedException e1) {
      e1.printStackTrace();
    }
    cluster.shutdown();
    System.exit(0);

	   /* try {
			StormSubmitter.submitTopology("logfilter", conf, builder.createTopology());
		} catch (AlreadyAliveException e) {
			e.printStackTrace();
		} catch (InvalidTopologyException e) {
			e.printStackTrace();
		}
		*/

  }

}
