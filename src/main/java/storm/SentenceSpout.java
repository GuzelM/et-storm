package storm;
import java.util.Map;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

/*
 * SentenceSpout implementation creates a static list of sentences to be iterated in order to simulate a data source, 
 * where each sentence will be emitted as a single field tuple.
 * The BaseRichSpout class provides a convenient implementation of the ISpout and IComponent interfaces.
 */
public class SentenceSpout extends BaseRichSpout {
	private SpoutOutputCollector collector;
	private String[] sentences = {
			"sleep, sugar",
		      "let your dreams flood in",
		      "like waves of sweet fire, you're safe within",
		      "sleep, sweetie",
		      "let your floods come rushing in",
			  "and carry you over to a new morning"
	};
	private int ind = 0;
	
	/*
	 * Must be defined for all spouts and bolts.
	 * Tells Storm which streams the component needs to emit and the fields each tuple of the stream will contain 
	 * @see org.apache.storm.topology.IComponent#declareOutputFields(org.apache.storm.topology.OutputFieldsDeclarer)
	 */
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("sentence"));
	}
	
	/*
	 * The open() method uses three parameters: 
	 * a map with the Storm configuration, 
	 * a TopologyContext object with the info w.r.t. components in a topology, 
	 * a SpoutOutputCollector object provides methods to emit tuples. 
	 * Here the open() implementation simply stores a reference to the SpoutOutputCollector object in an instance variable.
	 * @see org.apache.storm.spout.ISpout#open(java.util.Map, org.apache.storm.task.TopologyContext, org.apache.storm.spout.SpoutOutputCollector)
	 */
	public void open(Map config, TopologyContext context,
			SpoutOutputCollector collector) {
		this.collector = collector;
	}
	
	/*
	 * Storm uses this method to make the spout emit tuples to the SpoutOutputCollector at the current index and increments it.
	 * @see org.apache.storm.spout.ISpout#nextTuple()
	 */
	public void nextTuple() {
		this.collector.emit(new Values(sentences[ind]));
		ind++;
		if (ind >= sentences.length) {
			ind = 0;
		}
		//Utils.sleep(1000);
	}
}