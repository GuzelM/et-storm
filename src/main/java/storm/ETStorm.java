package storm;
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
//import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.Utils;



/**
 * This is a basic example of a Storm topology.
 */
public class ETStorm {
	//private static final String SENTENCE_SPOUT_ID = "sentence-spout";
	private static final String TEST_SPOUT_ID = "test-spout";
	//private static final String SPLIT_BOLT_ID = "split-sentence-bolt";
	private static final String EXLAM_BOLT_ID_1 = "exclam-bolt-1";
	//private static final String EXLAM_BOLT_ID_2 = "exclam-bolt-2";
	
	private static final String TOPOLOGY_NAME = "exclamation-topology";
	
	

  public static class ExclamationBolt extends BaseRichBolt  {
    //OutputCollector _collector;
	  private long nItems;
	  private long startTime;

    @Override
    @SuppressWarnings("rawtypes")
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
      //_collector = collector;
    	nItems = 0;
        startTime = System.currentTimeMillis();
    }

    @Override
    public void execute(Tuple tuple) {
      //_collector.emit(tuple, new Values(tuple.getString(0) + "!!!"));
      //_collector.ack(tuple);
    	 if (++nItems % 100000 == 0) {
    	        long runtime = System.currentTimeMillis() - startTime;
    	        System.out.println(tuple.getString(0) + "!!!");
    	        System.out.println("Bolt processed " + nItems + " tuples in " + runtime + " ms");
    	      }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      //declarer.declare(new Fields("word"));
    }

  }
  
  public static void main(String[] args) throws Exception {	  
	    TopologyBuilder builder = new TopologyBuilder();
	    
	    int parallelism = 2;

	    int spouts = parallelism;

	    builder.setSpout(TEST_SPOUT_ID, new TestWordSpout(),//new SentenceSpout(), 
	    		spouts);
	    
	    int bolts = 2 * parallelism;
	    //builder.setBolt(SPLIT_BOLT_ID, new SplitSentenceBolt(), bolts).shuffleGrouping(SENTENCE_SPOUT_ID);
	    builder.setBolt(EXLAM_BOLT_ID_1, new ExclamationBolt(), bolts).shuffleGrouping(TEST_SPOUT_ID);
	    //builder.setBolt(EXLAM_BOLT_ID_2, new ExclamationBolt(), bolts).shuffleGrouping(EXLAM_BOLT_ID_1);

	    Config conf = new Config();
	    //conf.setDebug(true);
	    conf.setMaxSpoutPending(10);
	    conf.setMessageTimeoutSecs(600);

	    if (args != null && args.length > 0) {
	      conf.setNumWorkers(parallelism);

	      StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
	      
	      //Utils.sleep(50000);	      
	      //Map map = Utils.readStormConfig();
	      //map.put("nimbus.seeds", "localhost");
	      //NimbusClient.getConfiguredClient(map).getClient().killTopology(args[0]);
	      
	    }
	    else {

	      LocalCluster cluster = new LocalCluster();
	      cluster.submitTopology(TOPOLOGY_NAME, conf, builder.createTopology());
	      Utils.sleep(30000);
	      cluster.killTopology(TOPOLOGY_NAME);
	      cluster.shutdown();
	    }
	  }

  
}