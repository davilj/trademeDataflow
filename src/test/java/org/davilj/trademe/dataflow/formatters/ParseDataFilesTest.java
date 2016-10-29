package org.davilj.trademe.dataflow.formatters;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.davilj.trademe.dataflow.formatters.ParseDataFiles.DailySalesOptions;
import org.davilj.trademe.dataflow.formatters.ParseDataFiles.ExtractDetailsOfBids;
import org.junit.Assert;
import org.junit.Test;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.testing.DataflowAssert;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.transforms.Count;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFnTester;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;

public class ParseDataFilesTest {

	// Our static input data, which will make up the initial PCollection.
	static final String[] WORDS_ARRAY = new String[] { "hi", "there", "hi", "hi", "sue", "bob", "hi", "sue", "", "",
			"ZOW", "bob", "" };

	static final List<String> WORDS = Arrays.asList(WORDS_ARRAY);

	@Test
	public void test() throws IOException {
		String[] args = {
				"--inputFile=/Users/daniev/development/google/trademe/dataflow/src/test/resources/test.dailySales",
				"--output=/Users/daniev/development/google/trademe/dataflow/src/test/resources/test.dailySales.result" };

		DailySalesOptions dailySalesOptions = PipelineOptionsFactory.fromArgs(args).withValidation()
				.as(DailySalesOptions.class);

		Pipeline p = ParseDataFiles.createPipeline(dailySalesOptions);
		p.run();

		List<String> resultArray = new ArrayList<>();
		try (RandomAccessFile raf = new RandomAccessFile(
				"/Users/daniev/development/google/trademe/dataflow/src/test/resources/test.result-00000-of-00001", "r")) {
			String line;
			do {
				line = raf.readLine();
				if (line != null)
					resultArray.add(line);
			} while (line != null);
		}
		Assert.assertEquals(10, resultArray.size());

	}

	@Test
	public void testParseLineNoBid() {
		ParseDataFiles.ExtractBid extract = new ParseDataFiles.ExtractBid();
		DoFnTester<String, String> extractBidTester = DoFnTester.of(extract);

		// Test
		String testInput = "LatestListing [title=Mastering Photoshop 7 New. Pay now., link=/computers/software/other/auction-994633241.htm, closingTimeText=closes in 9 mins, bidInfo=, priceInfo=$15.00];, /Users/daniev/development/google/trademe/d_201512/20151214/0002-/201512140014.ll";

		List<String> resultArr = extractBidTester.processBatch(testInput);
		Assert.assertEquals(1, resultArr.size());
		String resultStr = resultArr.get(0);
		Assert.assertEquals(
				"994633241|-computers-software-other|20151214 092309|/computers/software/other/auction-994633241.htm|Mastering Photoshop 7 New. Pay now.||1500",
				resultStr);
	}

	@Test
	public void testParseLineNoPrice() {
		// this should not happens ...bids without price
		ParseDataFiles.ExtractBid extract = new ParseDataFiles.ExtractBid();
		DoFnTester<String, String> extractBidTester = DoFnTester.of(extract);

		// Test
		String testInput = "LatestListing [title=Mastering Photoshop 7 New. Pay now., link=/computers/software/other/auction-994633241.htm, closingTimeText=closes in 9 mins, bidInfo=14bids, priceInfo=];, /Users/daniev/development/google/trademe/d_201512/20151214/0002-/201512140014.ll";

		List<String> resultArr = extractBidTester.processBatch(testInput);
		Assert.assertEquals(1, resultArr.size());
		String resultStr = resultArr.get(0);
		Assert.assertEquals(
				"994633241|-computers-software-other|20151214 092309|/computers/software/other/auction-994633241.htm|Mastering Photoshop 7 New. Pay now.|14|",
				resultStr);
	}

	@Test
	public void testParseLine() {
		ParseDataFiles.ExtractBid extract = new ParseDataFiles.ExtractBid();
		DoFnTester<String, String> extractBidTester = DoFnTester.of(extract);

		// Test
		String testInput = "LatestListing [title=Mastering Photoshop 7 New. Pay now., link=/computers/software/other/auction-994633241.htm, closingTimeText=closes in 9 mins, bidInfo=1bid, priceInfo=$15.00];, /Users/daniev/development/google/trademe/d_201512/20151214/0002-/201512140014.ll";

		List<String> resultArr = extractBidTester.processBatch(testInput);
		Assert.assertEquals(1, resultArr.size());
		String resultStr = resultArr.get(0);
		Assert.assertEquals(
				"994633241|-computers-software-other|20151214 092309|/computers/software/other/auction-994633241.htm|Mastering Photoshop 7 New. Pay now.|1|1500",
				resultStr);
	}

	@Test
	public void testParseLine10Bids() {
		ParseDataFiles.ExtractBid extract = new ParseDataFiles.ExtractBid();
		DoFnTester<String, String> extractBidTester = DoFnTester.of(extract);

		// Test
		String testInput = "LatestListing [title=Mastering Photoshop 7 New. Pay now., link=/computers/software/other/auction-994633241.htm, closingTimeText=closes in 9 mins, bidInfo=1 bid, priceInfo=$15.00];, /Users/daniev/development/google/trademe/d_201512/20151214/0002-/201512140014.ll";

		List<String> resultArr = extractBidTester.processBatch(testInput);
		Assert.assertEquals(1, resultArr.size());
		String resultStr = resultArr.get(0);
		Assert.assertEquals(
				"994633241|-computers-software-other|20151214 092309|/computers/software/other/auction-994633241.htm|Mastering Photoshop 7 New. Pay now.|1|1500",
				resultStr);
	}

	@Test
	public void testParseLineBid() {
		ParseDataFiles.ExtractBid extract = new ParseDataFiles.ExtractBid();
		DoFnTester<String, String> extractBidTester = DoFnTester.of(extract);

		// Test
		String testInput = "LatestListing [title=Mastering Photoshop 7 New. Pay now., link=/computers/software/other/auction-994633241.htm, closingTimeText=closes in 9 mins, bidInfo=10 bids, priceInfo=$15.00];, /Users/daniev/development/google/trademe/d_201512/20151214/0002-/201512140014.ll";

		List<String> resultArr = extractBidTester.processBatch(testInput);
		Assert.assertEquals(1, resultArr.size());
		String resultStr = resultArr.get(0);
		Assert.assertEquals(
				"994633241|-computers-software-other|20151214 092309|/computers/software/other/auction-994633241.htm|Mastering Photoshop 7 New. Pay now.|10|1500",
				resultStr);
	}

	@Test
	public void testCount() {
		// Create a test pipeline.
		Pipeline p = TestPipeline.create();

		// Create an input PCollection.
		PCollection<String> input = p.apply(Create.of(WORDS)).setCoder(StringUtf8Coder.of());

		// Apply the Count transform under test.
		PCollection<KV<String, Long>> output = input.apply(Count.<String>perElement());

		// Assert on the results.
		DataflowAssert.that(output).containsInAnyOrder(KV.of("hi", 4L), KV.of("there", 1L), KV.of("sue", 2L),
				KV.of("bob", 2L), KV.of("", 3L), KV.of("ZOW", 1L));

		// Run the pipeline.
		p.run();
	}

}
