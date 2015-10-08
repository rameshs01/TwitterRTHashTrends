package com.rs.twitterstorm.bolt;

import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class TwitterHashCountHBaseBolt implements IRichBolt {

	private static final long serialVersionUID = 2946379346389650318L;
	private static final Logger LOG = Logger.getLogger(TwitterHashCountHBaseBolt.class);

	// TABLES
	private static final String TWITTER_HASH_TAG_TABLE_NAME = "twitter_hash_counts";

	// CF
	private static final byte[] CF_HASHTAG_TABLE = Bytes.toBytes("hash_tags");

	// COL
	private static final byte[] COL_COUNT_VALUE = Bytes.toBytes("count");

	private OutputCollector collector;
	private HConnection connection;
	private HTableInterface hashcountTable;

	public TwitterHashCountHBaseBolt(Properties topologyConfig) {

	}

	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		try {
			this.connection = HConnectionManager
					.createConnection(constructConfiguration());
			this.hashcountTable = connection
					.getTable(TWITTER_HASH_TAG_TABLE_NAME);
		} catch (Exception e) {
			String errMsg = "Error retrievinging connection and access to HBase Tables";
			LOG.error(errMsg, e);
			throw new RuntimeException(errMsg, e);
		}
	}

	public void execute(Tuple tuple) {

		// LOG.info("About to insert tuple["+input +"] into HBase...");

		String hashTags = tuple.getStringByField("hash_tags");

		if (hashTags != null && hashTags.length() > 0) {

			String[] hasTags = hashTags.split(",");
			for (String hasTag : hasTags) {
				long hashCount = getHashCount(hasTag);
				hashCount = hashCount + 1;

				try {
					Put put = constructRow(TWITTER_HASH_TAG_TABLE_NAME, hasTag,
							hashCount);
					this.hashcountTable.put(put);
				} catch (Exception e) {
					LOG.error("Error inserting event into HBase table["
							+ TWITTER_HASH_TAG_TABLE_NAME + "]", e);
				}
			}
		}

		collector.emit(tuple, new Values(hashTags));

		// acknowledge even if there is an error
		collector.ack(tuple);
	}

	public static Configuration constructConfiguration() {
		Configuration config = HBaseConfiguration.create();
		return config;
	}

	private Put constructRow(String columnFamily, String hasTag, long count) {

		String rowKey = hasTag;
		Put put = new Put(Bytes.toBytes(rowKey));
		put.add(CF_HASHTAG_TABLE, COL_COUNT_VALUE, Bytes.toBytes(count));

		return put;
	}

	public void cleanup() {
		try {
			hashcountTable.close();
			connection.close();
		} catch (Exception e) {
			LOG.error("Error closing connections", e);
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("hash_tags"));
	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	private long getHashCount(String hashTag) {
		long count = 0;
		try {
			byte[] hasTag = Bytes.toBytes(hashTag);
			Get get = new Get(hasTag);
			Result result = hashcountTable.get(get);
			

			if (result != null) {
				byte[] countBytes = result.getValue(CF_HASHTAG_TABLE,
						COL_COUNT_VALUE);
				if (countBytes != null) {
					count = Bytes.toLong(countBytes);
				}
			}

			
		} catch (Exception e) {
			LOG.error("Error getting infraction count", e);
			//throw new RuntimeException("Error getting infraction count");
		}
		
		return count;
	}
}
