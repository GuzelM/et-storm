package storm;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/*
 * The BaseRichBolt class to implement the IComponent and IBolt interfaces. 
 * Splits sentences into words
 */
public class SplitSentenceBolt extends BaseRichBolt{
	private OutputCollector collector;
	/*
	 * Since doesn't require to prepare the resources during Bolt initialization, 
	 * only saves a reference to OutputCollector object
	 * @see org.apache.storm.task.IBolt#prepare(java.util.Map, org.apache.storm.task.TopologyContext, org.apache.storm.task.OutputCollector)
	 */
	public void prepare(Map config, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
	}
	/*
	 * The execute() method is called every time after receiving a tuple from Stream to which the Bolt is subscribed.
	 * Looks up the value of the "sentence" field as String, splits the value into separate words and for each word emits new tuple.
	 * @see org.apache.storm.task.IBolt#execute(org.apache.storm.tuple.Tuple)
	 */
	public void execute(Tuple tuple) {
		String sentence = tuple.getStringByField("sentence");
		String[] words = sentence.split(" ");
		for(String word : words){
			this.collector.emit(new Values(word));
		}
	}
	/*
	 * SentenceSplitBolt class declares a single stream of tuples, each containing one field ("word").
	 * @see org.apache.storm.topology.IComponent#declareOutputFields(org.apache.storm.topology.OutputFieldsDeclarer)
	 */
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}
}