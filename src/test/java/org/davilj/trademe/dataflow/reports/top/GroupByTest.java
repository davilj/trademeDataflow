package org.davilj.trademe.dataflow.reports.top;

import org.junit.Test;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;

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
