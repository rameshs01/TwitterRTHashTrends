package com.rs.twitterstorm.bolt;

import java.io.IOException;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.SolrInputDocument;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class SolrUpdateBolt extends BaseBasicBolt {
   
	private static final ObjectMapper mapper = new ObjectMapper();

	
	public void updateSolor(String twitterJsonString) throws SolrServerException,
			IOException {

		JsonNode root = mapper.readValue(twitterJsonString, JsonNode.class);

		SolrInputDocument sdoc = new SolrInputDocument();
		sdoc.addField("id", root.get("id_str").textValue());
		sdoc.addField("screen_name_s", root.get("user").get("screen_name").textValue());
		sdoc.addField("type_s", "post");
		sdoc.addField("lang_s", root.get("lang").textValue());
		sdoc.addField("text_t", root.get("text").textValue());
		
		HttpSolrClient solrClient = new HttpSolrClient(
		"http://localhost:8983/solr/twittersearch");
		solrClient.add(sdoc); // send it to the solr server
		solrClient.commit();
		solrClient.close(); // shutdown client before we exit
	}
	
    public void execute(Tuple tuple, BasicOutputCollector collector) {
    	String json = tuple.getString(0);
    	try {
			updateSolor(json);
		} catch (SolrServerException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		collector.emit(new Values(json));
    }

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tweet"));
	}
}
