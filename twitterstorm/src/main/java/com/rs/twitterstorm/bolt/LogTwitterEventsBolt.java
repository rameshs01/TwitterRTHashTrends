package com.rs.twitterstorm.bolt;

import java.util.Map;

import org.apache.log4j.Logger;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class LogTwitterEventsBolt extends BaseRichBolt {
	private static final Logger LOG = Logger
			.getLogger(LogTwitterEventsBolt.class);

	public void declareOutputFields(OutputFieldsDeclarer ofd) {
		// none prints to the Logger.
	}

	public void prepare(Map map, TopologyContext tc, OutputCollector oc) {
		// no output.
	}

	public void execute(Tuple tuple) {
		String json = tuple.getString(0);
		LOG.info("twitter message" + json);
	}
}
