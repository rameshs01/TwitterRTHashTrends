package com.rs.twitterstream;

import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

public class TwitterKafkaProducer {

	private static final String topic = "twitter-topic";

	public static void run(String consumerKey, String consumerSecret,
			String token, String secret) throws InterruptedException {

		Properties properties = new Properties();
		properties.put("metadata.broker.list", "sandbox.hortonworks.com:6667");
		properties.put("serializer.class", "kafka.serializer.StringEncoder");
		properties.put("client.id","twitter-trends");
		ProducerConfig producerConfig = new ProducerConfig(properties);
		kafka.javaapi.producer.Producer<String, String> producer = new kafka.javaapi.producer.Producer<String, String>(
				producerConfig);

		BlockingQueue<String> queue = new LinkedBlockingQueue<String>(10000);
		StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
		
		//track terms
		endpoint.trackTerms(Lists.newArrayList("news"));

		Authentication auth = new OAuth1(consumerKey, consumerSecret, token,
				secret);
		
		Client client = new ClientBuilder().hosts(Constants.STREAM_HOST)
				.endpoint(endpoint).authentication(auth)
				.processor(new StringDelimitedProcessor(queue)).build();

		// Establish a connection
		client.connect();

		while (!client.isDone()) {
			KeyedMessage<String, String> message = null;
			try {
				String tweets = queue.take();
				System.out.println("tweets" + tweets);
				
				message = new KeyedMessage<String, String>(topic, tweets);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

			// send message to Kafka
			try {
				producer.send(message);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		producer.close();
		client.stop();
	}

	public static void main(String[] args) {
		try {
			TwitterKafkaProducer.run("iWMiefZDYt9xAjZJIw1KCgKNJ", 
					"aoRnQPNhf9CgcMjgXkPod9eBLGdgMA8NvdZEA8ZsFLnIM6CJWJ", 
					"332047462-6QSrj6YpmGy45WIKF7q191LXheifHLvyHmkh6Qp7", 
					"cbQ4TLtfzVZUqG4RsQ74ccfUKLLVbc9QSYU9bOTFHPSGI"); //Todo: read from propertis file
		} catch (InterruptedException e) {
			System.out.println(e);
		}
	}
}
