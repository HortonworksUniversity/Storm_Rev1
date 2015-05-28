package wordcount;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class MultilangWordCount {

	public static void main(String[] args) {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout", new RandomSentenceSpout(), 5);
		builder.setBolt("split", 
                new SplitSentencePythonBolt(), 
                8).shuffleGrouping("spout");
		builder.setBolt("count", 
                new WordCountBolt(), 
                12).fieldsGrouping("split", new Fields("word"));
		
		Config conf = new Config();
		conf.setMaxTaskParallelism(3);
		conf.setDebug(true);
		try {
			StormSubmitter.submitTopology("multilang-word-count", conf, builder.createTopology());
		} catch (AlreadyAliveException e) {
			e.printStackTrace();
		} catch (InvalidTopologyException e) {
			e.printStackTrace();
		}
	}

}
