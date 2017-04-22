package org.davilj.trademe.dataflow.reports.top;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
//Import SLF4J packages.
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import io.grpc.internal.Stream;

public class GroupByPipeLine {
	/**
	 * Pipeline group by cat1, and cat-3...also sum over month
	 * @author daniev
	 *
	 */
	public static interface GroupByOptions extends PipelineOptions {
		@Description("Path of the files to read from")
		String getInputDir();

		void setInputDir(String value);

		@Description("Path of the file to write to")
		// @Default.InstanceFactory(OutputFactory.class)
		String getOutput();

		void setOutput(String value);

		String getErrorFile();

		void setErrorFile(String errorFile);
	}

	public static class ExtractDailyBidsAndGroupByCat extends DoFn<String, KV<String, String>> {
		private static final Logger LOG = LoggerFactory.getLogger(ExtractDailyBidsAndGroupByCat.class);

		// only listing with bids
		public void processElement(ProcessContext c) {
			// business|20151223:6|116019|399|51100
			String line = c.element();
			if (line != null && !line.trim().isEmpty()) {
				try {
					String[] parts = line.split(":");
					if (parts.length != 2) {
						throw new RuntimeException("Expecting 2 parts, but was " + parts.length);
					}
					String[] keyParts = parts[0].split("\\|");
					String date = keyParts[1];
					String cat = keyParts[0];
					if (date.trim().length() == 8) {
						KV<String, String> keyValue = KV.of(cat, line);
						c.output(keyValue);
					}
				} catch (Exception e) {
					LOG.error("Error in extracting data from: " + line);
				}
			}
		}
	}

	public static class GroupByFunction extends PTransform<PCollection<String>, PCollection<String>> {
		public PCollection<String> apply(PCollection<String> lines) {
			return lines.apply(ParDo.of(new ExtractDailyBidsAndGroupByCat())).apply(GroupByKey.<String, String>create())
					.apply(MapElements.via(new SimpleFunction<KV<String, Iterable<String>>, String>() {

						public String apply(KV<String, Iterable<String>> kv) {
							String cat = kv.getKey();
							Iterable<String> values = kv.getValue();
							List<StatsInfo> entries = build(values);
							String statsAsString = entries.stream().map(e -> {
								return e == null ? "" : String.format("%s|%d|%d|%d|%d",e.getDate(), e.numberOfTransaction,e.min,e.max,e.sum);
							}).collect(Collectors.joining(","));
							
							return String.format("%s:%s",cat, statsAsString);
						}
					}));
		}

		private StatsInfo aggregateMonth(List<StatsInfo> entries) {
			
			if (!entries.isEmpty()) {
				StatsInfo statsInfo = new StatsInfo();
				StatsInfo template = entries.get(0);
				statsInfo.cat = template.cat;
				statsInfo.year = template.year;
				statsInfo.month = template.month;
				statsInfo.dateInfo = statsInfo.year + ((statsInfo.month<0)?"0":"") + statsInfo.month;
				statsInfo.day = 0;
				statsInfo.max = template.max;
				statsInfo.min = template.min;

				statsInfo.numberOfTransaction = 0;
				statsInfo.sum = 0;
				
				entries.stream()
					.filter(e -> e!=null)
					.forEach(e -> {
						statsInfo.sum+=e.sum;
						statsInfo.numberOfTransaction+=e.numberOfTransaction;
						statsInfo.mergeMin(e.min);
						statsInfo.mergeMax(e.max);
					});
				return statsInfo;
			} else {
				return null;
			}
		}

		private List<StatsInfo> build(Iterable<String> values) {
			List<StatsInfo> days = new ArrayList<>();
			values.forEach(e -> {
				StatsInfo statsInfo = StatsInfo.from(e);
				days.add(statsInfo);
			});
			
			Collections.reverse(days);
			days.add(aggregateMonth(days));
			Collections.reverse(days);
			return days;
		}

		@Override
		public PCollection<String> expand(PCollection<String> arg0) {
			// TODO Auto-generated method stub
			return null;
		}
	}

	public static Pipeline createPipeline(GroupByPipeLine.GroupByOptions options) {
		String dir = options.getInputDir() + File.separator + "ds_*.txt-*";
		Pipeline p = Pipeline.create(options);
		p.apply(TextIO.Read.from(dir))
				.apply(new GroupByFunction())
				.apply(TextIO.Write.to(options.getOutput()));
		return p;
	}

	public static void main(String[] args) {
		GroupByPipeLine.GroupByOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(GroupByPipeLine.GroupByOptions.class);
		Pipeline p = createPipeline(options);
		p.run();
	}
}
