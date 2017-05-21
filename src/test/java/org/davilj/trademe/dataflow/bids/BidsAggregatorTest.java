package org.davilj.trademe.dataflow.bids;

import java.io.IOException;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.davilj.trademe.dataflow.bids.BidsAggregation.AggregatorPipeLineOptions;
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
				"--errorFile=src/test/resources/dailyAgg/err/" };

		AggregatorPipeLineOptions aggSalesOptions = PipelineOptionsFactory.fromArgs(args).withValidation()
				.as(AggregatorPipeLineOptions.class);

		Pipeline p = BidsAggregation.createPipeline(aggSalesOptions);
		p.run();
		
	}
}
