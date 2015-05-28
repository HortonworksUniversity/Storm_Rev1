package trident;

import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.Debug;
import storm.trident.testing.Split;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.tuple.Fields;

public class TridentWordCount {

  public static void main(String[] args)  {
	
      System.out.println("******** TridentWordCount is starting");
      Config config = new Config();
      config.put("inputFile", "/root/install_course.sh");


      LineReaderSpout lineReaderSpout = new LineReaderSpout();

      TridentTopology topology = new TridentTopology();
      topology.newStream("spout",lineReaderSpout)
              .each(new Fields("line"), new Split(), new Fields("word"))
              .groupBy(new Fields("word"))
              .aggregate(new Fields("word"), new Count(), new Fields("count"))
              .each(new Fields("word","count"), new Debug());

      LocalCluster localCluster = new LocalCluster();
      localCluster.submitTopology("WordCount",config,topology.build());;
  }
}
