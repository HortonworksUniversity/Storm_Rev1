package kafka;

import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import storm.kafka.trident.TransactionalTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.FilterNull;
import storm.trident.operation.builtin.MapGet;
import storm.trident.operation.builtin.Sum;
import storm.trident.testing.MemoryMapState;
import storm.trident.tuple.TridentTuple;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class TridentKafkaWordCount {
	public static class Split extends BaseFunction {
		private static final long serialVersionUID = -8451631476210125187L;

		@Override
		public void execute(TridentTuple tuple, TridentCollector collector) {
			String sentence = tuple.getString(0);
			for (String word : sentence.split(" ")) {
				collector.emit(new Values(word));
			}
		}
	}

	public static StormTopology buildTopology(LocalDRPC drpc) {
		//Add code here...
	}

	public static void main(String[] args) throws Exception {

		Config conf = new Config();
		conf.setMaxSpoutPending(20);
		LocalDRPC drpc = new LocalDRPC();
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("trident-kafka-word-count", conf, buildTopology(drpc));
		for (int i = 0; i < 100; i++) {
			System.out.println("Sum: " + drpc.execute("words", "good happy"));
			Thread.sleep(1000);
		}
		drpc.shutdown();
		cluster.shutdown();
	}
}
