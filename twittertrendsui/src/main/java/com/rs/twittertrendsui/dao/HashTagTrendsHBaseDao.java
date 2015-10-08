package com.rs.twittertrendsui.dao;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.stereotype.Component;

import com.rs.twittertrendsui.vo.HashTrend;

@Component
public class HashTagTrendsHBaseDao {
	
	// TABLES
	private static final String HASH_TRENDS_TABLE_NAME = "twitter_hash_counts";

	// CF
	private static final byte[] CF_EVENTS_TABLE = Bytes.toBytes("hash_tags");

	// COL
	private static final byte[] COL_COUNT_VALUE = Bytes.toBytes("count");


	public List<HashTrend> getHashTrends(int batchSize) throws IOException {
		
		HTable hashTrendCountTbl = null;
		ResultScanner scanner = null;
		List<HashTrend> hashTrends = new ArrayList<HashTrend>();

		try {
			Configuration conf = HBaseConfiguration.create();
			//conf.addResource(new URL("http://localhost:60010/conf"));
            conf.addResource(new org.apache.hadoop.fs.Path("/opt/Twitterexample/twitterui/src/main/resources/hbase-site.xml"));//Todo: configurble
			hashTrendCountTbl = new HTable(conf, HASH_TRENDS_TABLE_NAME);

			Scan scan = new Scan();
			scanner = hashTrendCountTbl.getScanner(scan);

			int cnt = 0;
			for (Result r = scanner.next(); r != null && cnt < batchSize; ++cnt, r = scanner
					.next()) {
				
				String hasTag =  Bytes.toString(r.getRow());
				
				long count = Bytes.toLong(r.getValue(CF_EVENTS_TABLE, COL_COUNT_VALUE));
				HashTrend hasTagCount = new HashTrend();
				hasTagCount.setHashTag(hasTag);
				hasTagCount.setCount(count);
				hashTrends.add(hasTagCount);
			}

			return hashTrends;
			
		} finally {
			if (scanner != null) {
				scanner.close();
			}
			if (hashTrendCountTbl != null) {
				hashTrendCountTbl.close();
			}
		}

	}

	public static void main(String str[]) throws IOException {
		(new HashTagTrendsHBaseDao()).getHashTrends(1000); //for testing
	}
}