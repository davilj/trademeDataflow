package org.davilj.trademe.dataflow.reports.top;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Test;


public class GroupByTest {

	@Test
	public void test() {
		String[] args = {
				"--inputDir=/Users/daniev/development/google/trademe/dataflow/src/test/resources/topsales",
				"--output=/Users/daniev/development/google/trademe/dataflow/src/test/resources/topsales/results",
				};

		GroupByPipeLine.GroupByOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
				.as(GroupByPipeLine.GroupByOptions.class);

		Pipeline p = GroupByPipeLine.createPipeline(options);
		p.run();

	}

}
