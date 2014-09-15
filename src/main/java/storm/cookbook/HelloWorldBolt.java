package storm.cookbook;

import java.util.Map;

import org.apache.log4j.Logger;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class HelloWorldBolt extends BaseRichBolt {
	
	public static Logger LOG = Logger.getLogger(HelloWorldBolt.class);
	
	private static final long serialVersionUID = -841805977046116528L;
	
	private int myCount = 0;

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
	}

	@Override
	public void execute(Tuple input) {
		String test = input.getStringByField("sentence");
		if(test == "A"){
			myCount++;
			System.out.println("A: " + Integer.toString(myCount));
			LOG.debug("A: " + Integer.toString(myCount));
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("myCount"));

	}

}