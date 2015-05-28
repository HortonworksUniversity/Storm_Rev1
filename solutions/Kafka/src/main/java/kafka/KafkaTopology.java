package kafka;

import java.util.UUID;

import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

public class KafkaTopology {

	public static void main(String[] args) {
		ZkHosts hosts = new ZkHosts("namenode:2181");
		SpoutConfig sc = new SpoutConfig(hosts, "my_topic","/my_topic", UUID.randomUUID().toString());
		KafkaSpout spout = new KafkaSpout(sc);

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("my_topic_spout", spout, 1);
		
		builder.setBolt("simplebolt", 
                new SimpleBolt(), 
                1).shuffleGrouping("my_topic_spout");
		
/*		builder.setBolt("logfilter", 
                new LogFilterBolt("WARN"), 
                12).shuffleGrouping("logsplitter");
*/		
		Config conf = new Config();
		//conf.setMaxTaskParallelism(3);
		//conf.setDebug(true);

		LocalCluster cluster = new LocalCluster();
	    cluster.submitTopology("kafka_topology", conf, builder.createTopology());
	    try {
			Thread.sleep(60000);
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}
//	    cluster.shutdown();

/*
	    try {
			StormSubmitter.submitTopology("logfilter", conf, builder.createTopology());
		} catch (AlreadyAliveException e) {
			e.printStackTrace();
		} catch (InvalidTopologyException e) {
			e.printStackTrace();
		}
*/
	
	}

}
