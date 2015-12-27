package com.rs.twitterstorm;

import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;

import com.rs.twitterstorm.bolt.HashtagExtractionBolt;
import com.rs.twitterstorm.bolt.LogTwitterEventsBolt;
import com.rs.twitterstorm.bolt.SolrUpdateBolt;
import com.rs.twitterstorm.bolt.TwitterFilterBolt;
import com.rs.twitterstorm.bolt.TwitterHashCountHBaseBolt;


public class TwitterEventProcessingTopology extends BaseTwitterEventTopology {

	private static final String KAFKA_SPOUT_ID = "kafkatwiterSpout";
	private static final String LOG_TWITTER_BOLT_ID = "logtwitterEventBolt";
	private static final String TWITTER_FILTER_BOLT_ID = "twitterFilterEventBolt";
	private static final String HASH_TAG_SPLITTER_BOLD_ID = "hashtagSplitterBolt";
	private static final String HASH_COUNT_HBASE_BOLD_ID = "hashcountHbaseBolt";
	private static final String SOLOR_UPDATE_BOLT_ID = "solorUpdateBolt";

	public TwitterEventProcessingTopology(String configFileLocation)
			throws Exception {
		super(configFileLocation);
	}

	private SpoutConfig constructKafkaSpoutConf() {
		BrokerHosts hosts = new ZkHosts(
				topologyConfig.getProperty("kafka.zookeeper.host.port"));
		String topic = topologyConfig.getProperty("kafka.topic");
		String zkRoot = topologyConfig.getProperty("kafka.zkRoot");
		String consumerGroupId = "StormSpout";

		SpoutConfig spoutConfig = new SpoutConfig(hosts, topic, zkRoot,
				consumerGroupId);

		spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

		return spoutConfig;
	}

	public void configureKafkaSpout(TopologyBuilder builder) {
		KafkaSpout kafkaSpout = new KafkaSpout(constructKafkaSpoutConf());
		int spoutCount = Integer.valueOf(topologyConfig
				.getProperty("spout.thread.count"));
		builder.setSpout(KAFKA_SPOUT_ID, kafkaSpout);
	}

	public void configureTwitterFileter(TopologyBuilder builder) {
		TwitterFilterBolt twitterFilterBolt = new TwitterFilterBolt();
		builder.setBolt(TWITTER_FILTER_BOLT_ID, twitterFilterBolt).shuffleGrouping(
				SOLOR_UPDATE_BOLT_ID);
	}
	
	public void configureHasTagSplitter(TopologyBuilder builder) {
		HashtagExtractionBolt hashTagSplitter = new HashtagExtractionBolt();
		builder.setBolt(HASH_TAG_SPLITTER_BOLD_ID, hashTagSplitter).
					shuffleGrouping(TWITTER_FILTER_BOLT_ID);
	}
	
	public void configureTwitterHashCountBolt(TopologyBuilder builder) {
		TwitterHashCountHBaseBolt hashCountHbase= new TwitterHashCountHBaseBolt(topologyConfig);
		builder.setBolt(HASH_COUNT_HBASE_BOLD_ID, hashCountHbase).
					shuffleGrouping(HASH_TAG_SPLITTER_BOLD_ID);
	}
	
	public void configureLogTwitterEventBolt(TopologyBuilder builder) {
		LogTwitterEventsBolt logBolt = new LogTwitterEventsBolt();
		builder.setBolt(LOG_TWITTER_BOLT_ID, logBolt).shuffleGrouping(HASH_COUNT_HBASE_BOLD_ID);
	}
	
	public void configureSolorUpdate(TopologyBuilder builder) {
		SolrUpdateBolt solorUpate = new SolrUpdateBolt();
		builder.setBolt(SOLOR_UPDATE_BOLT_ID, solorUpate).shuffleGrouping(KAFKA_SPOUT_ID);
	}

	private void buildAndSubmit() throws Exception {
		TopologyBuilder builder = new TopologyBuilder();
		configureKafkaSpout(builder);	
		configureSolorUpdate(builder);
		configureTwitterFileter(builder);
		configureHasTagSplitter(builder);
		configureTwitterHashCountBolt(builder);
		//configureLogTwitterEventBolt(builder);

		Config conf = new Config();
		conf.setDebug(true);

		StormSubmitter.submitTopology("twitter-event-processor", conf,
				builder.createTopology());
	}

	public static void main(String[] str) throws Exception {
		String configFileLocation = "twitter_tag_topology.properties";
		TwitterEventProcessingTopology twitterTrend = new TwitterEventProcessingTopology(
				configFileLocation);
		twitterTrend.buildAndSubmit();
	}
}
