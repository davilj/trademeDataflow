package org.davilj.trademe.dataflow.reports;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.davilj.trademe.dataflow.reports.DailySales.DailySalesOptions;
import org.junit.Assert;
import org.junit.Test;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.DoFnTester;
import com.google.cloud.dataflow.sdk.values.KV;

public class DailySalesTest {

	@Test
	public void ExtractValidBid() {
		DailySales.ExtractValidBid extractValidBids = new DailySales.ExtractValidBid();
		DoFnTester<String, String> extractValidBidsTester = DoFnTester.of(extractValidBids);

		// Test
		String testInput = "994633241|-computers-software-other|20151214 092309|/computers/software/other/auction-994633241.htm|Mastering Photoshop 7 New. Pay now.|1|1500";

		List<String> resultArr = extractValidBidsTester.processBatch(testInput);
		Assert.assertEquals(1, resultArr.size());
	}

	@Test
	public void ExtractInValidBid() {
		DailySales.ExtractValidBid extractValidBids = new DailySales.ExtractValidBid();
		DoFnTester<String, String> extractValidBidsTester = DoFnTester.of(extractValidBids);

		// Test
		String testInput = "994633241|-computers-software-other|20151214 092309|/computers/software/other/auction-994633241.htm|Mastering Photoshop 7 New. Pay now.||1500";

		List<String> resultArr = extractValidBidsTester.processBatch(testInput);
		Assert.assertEquals(0, resultArr.size());
	}

	@Test
	public void ExtractKey() {
		DailySales.ExtractKey extractKey = new DailySales.ExtractKey();
		DoFnTester<String, KV<String, String>> extractKeyTester = DoFnTester.of(extractKey);

		// Test
		String testInput = "994633241|-computers-software-other|20151214 092309|/computers/software/other/auction-994633241.htm|Mastering Photoshop 7 New. Pay now.||1500";

		List<KV<String, String>> resultArr = extractKeyTester.processBatch(testInput);
		Assert.assertEquals(1, resultArr.size());
		KV<String, String> keys = resultArr.get(0);
		Assert.assertEquals("994633241", keys.getKey());
		Assert.assertEquals(
				"994633241|-computers-software-other|20151214 092309|/computers/software/other/auction-994633241.htm|Mastering Photoshop 7 New. Pay now.||1500",
				keys.getValue());
	}

	@Test
	public void ExtractBidInfo() {
		DailySales.ExtractBidInfo extractBidInfo = new DailySales.ExtractBidInfo();
		DoFnTester<String, String> extractBidInfoTester = DoFnTester.of(extractBidInfo);

		// Test
		String testInput = "994633241|-computers-software-other|20151214 092309|/computers/software/other/auction-994633241.htm|Mastering Photoshop 7 New. Pay now.|10|1500";

		List<String> resultArr = extractBidInfoTester.processBatch(testInput);
		Assert.assertEquals(1, resultArr.size());
		String bidInfo = resultArr.get(0);
		// cat1, cat, day, hour, dayHour, bids, amount
		String[] parts = bidInfo.split("\\|");
		Assert.assertEquals("computers", parts[0]);
		Assert.assertEquals("computers-software-other", parts[1]);
		Assert.assertEquals("20151214", parts[2]);
		Assert.assertEquals("09", parts[3]);
		Assert.assertEquals("2015121409", parts[4]);
		Assert.assertEquals("10", parts[5]);
		Assert.assertEquals("1500", parts[6]);
	}

	@Test
	public void testPipeLine() throws IOException {
		String[] args = {
				"--inputFile=/Users/daniev/development/google/trademe/dataflow/src/test/resources/test.dailySales.v1",
				"--output=/Users/daniev/development/google/trademe/dataflow/src/test/resources/test.dailySales.result",
				"--errorFile=/Users/daniev/development/google/trademe/dataflow/src/test/resources/test.dailySales.errors" };

		DailySalesOptions dailySalesOptions = PipelineOptionsFactory.fromArgs(args).withValidation()
				.as(DailySalesOptions.class);

		Pipeline p = DailySales.createPipeline(dailySalesOptions);
		p.run();

		
		// 	994626850|-test-a|20151216 133636|3|1 
		//	994626851|-test-a|20151216 133636|3|2 
		//	994626852|-test-a|20151216 133636|3|3
		// 
		// 	994626853|-test-a|20151217 123636|3|4 
		// 	994626854|-test-a|20151217 123636|3|5 
		// 	994626855|-test-a|20151217 123636|3|6
		// 
		// 	994626856|-test-b|20151217 123636|3|7 
		//	994626857|-test-b|20151217 123636|3|8 
		//	994626858|-test-b|20151217 123636|3|9

		Map<String, String> resultMap = new HashMap<>();
		try (RandomAccessFile raf = new RandomAccessFile(
				"/Users/daniev/development/google/trademe/dataflow/src/test/resources/test.dailySales.result-00000-of-00001",
				"r")) {
			String line;
			do {
				line = raf.readLine();
				if (line != null) {
					String[] parts = line.split(":");
					resultMap.put(parts[0], parts[1]);
				}
			} while (line != null);
		}
		Assert.assertEquals(10, resultMap.size());

		Map<String, String> testMap = new HashMap<>();
		testMap.put("test|2015121613", "6|3|1|3");
		testMap.put("test|2015121712", "39|6|4|9");
		testMap.put("test|20151216", "6|3|1|3");
		testMap.put("test|20151217", "39|6|4|9");

		testMap.put("test-a|2015121613", "6|3|1|3");
		testMap.put("test-a|2015121712", "15|3|4|6");
		testMap.put("test-a|20151216", "6|3|1|3");
		testMap.put("test-a|20151217", "15|3|4|6");

		testMap.put("test-b|2015121712", "24|3|7|9");
		testMap.put("test-b|20151217", "24|3|7|9");
		
		for (String key : testMap.keySet()) {
			String expected = testMap.get(key);
			String result = resultMap.get(key);
			Assert.assertEquals(expected, result);
		}

	}
}
