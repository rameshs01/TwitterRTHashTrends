package com.rs.twitterstorm.bolt;


import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.commons.lang.StringUtils;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;


public class HashtagExtractionBolt extends BaseBasicBolt {

	private static final long serialVersionUID = 1L;
	

    public void execute(Tuple tuple, BasicOutputCollector collector) {
        String text = tuple.getStringByField("tweet_text");
        StringTokenizer st = new StringTokenizer(text);
        List<String> hastags = new ArrayList<String>();
        
        System.out.println("---- Split by space ------");
        while (st.hasMoreElements()) {

            String term = (String) st.nextElement();
            if (StringUtils.startsWith(term, "#")){
                hastags.add(term);
            }
        }
        
        StringBuilder sb = new StringBuilder();
        for (String n : hastags) { 
            if (sb.length() > 0) sb.append(',');
            sb.append(n);
        }	
        
        collector.emit(new Values(sb.toString()));
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("hash_tags"));
    }

    @Override
    public void cleanup() {
        super.cleanup();
    }
}
