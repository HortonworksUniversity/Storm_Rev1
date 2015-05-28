package log;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class LogFileSpout extends BaseRichSpout {

	private static final long serialVersionUID = -6176746043688503098L;
	
	SpoutOutputCollector _collector;
	String _fileName;
	BufferedReader input;
	
	
	public LogFileSpout(String fileName) {
		_fileName = fileName;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		_collector = collector;

		System.out.println("*******Reading file from HDFS************");
		Path path = new Path( _fileName);
		Configuration config = new Configuration();
		config.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
		try {
			FileSystem fs = DistributedFileSystem.get(config);
			FSDataInputStream in = fs.open(path);
			input = new BufferedReader(new InputStreamReader(in));
		} catch (IOException e) {
			System.out.println("***********Unable to open file***********");
			e.printStackTrace();
		}
	}

	@Override
	public void nextTuple() {
		try {
			String line = input.readLine();
			_collector.emit(new Values(line));
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("errormessage"));
	}

}
