package org.davilj.trademe.dataflow.bids;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFnTester;
import org.apache.beam.sdk.values.KV;
import org.davilj.trademe.dataflow.bids.ExtractValidBids;
import org.davilj.trademe.dataflow.bids.ExtractValidBids.BidsPipeLineOptions;
import org.junit.Assert;
import org.junit.Test;


public class BidsAggregatorTest {

		
	

	@Test
	public void testPipeLine() throws IOException {
						
		String[] args = {
				"--inputFile=src/test/resources/dailyBids_Test/20170423/*",
				"--dailyCatBids=src/test/resources/dailyAgg/dailyCatBids/",
				"--dailyCat1Bids=src/test/resources/dailyAgg/dailyCat1Bids/",
				"--hourlyCatBids=src/test/resources/dailyAgg/hourlyCatBids/",
				"--hourlyCat1Bids=src/test/resources/dailyAgg/hourlyCat1Bids/",
				"--dailyCatSales=src/test/resources/dailyAgg/getDailyCatSales/",
				"--dailyCat1Sales=src/test/resources/dailyAgg/dailyCat1Sales/",
				"--hourlyCatSales=src/test/resources/dailyAgg/hourlyCatSales/",
				"--hourlyCat1Sales=src/test/resources/dailyAgg/hourlyCat1Sales/",
				"--errorFile=src/test/resources/dailyAgg/err" };

		BidsPipeLineOptions dailySalesOptions = PipelineOptionsFactory.fromArgs(args).withValidation()
				.as(BidsPipeLineOptions.class);

		Pipeline p = ExtractValidBids.createPipeline(dailySalesOptions);
		p.run();

		
		
	}
}
