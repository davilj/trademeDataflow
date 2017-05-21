package org.davilj.trademe.dataflow.bids;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Min;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.util.gcsfs.GcsPath;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.davilj.trademe.dataflow.bids.ExtractValidBids.ExtractValidBid;
import org.davilj.trademe.dataflow.model.helpers.BidParser;
import org.davilj.trademe.dataflow.model.helpers.ListingFactory;
import org.davilj.trademe.dataflow.model.helpers.ListingParser;
//Import SLF4J packages.
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A pipeline that run thought all listing, extract the bids (listing with
 * bids), group by cat and cat3, aggregate over month, month:day and
 * month:day:hour
 * 
 * @author daniev
 *
 */
public class BidsAggregation {
	final static TupleTag<String> errorsTuples = new TupleTag<String>() {
	};
	
	final static TupleTag<String> dailyCatBidsTuple = new TupleTag<String>() {
	};
	
	final static TupleTag<String> dailyCat1BidsTuple = new TupleTag<String>() {
	};
	
	final static TupleTag<String> hourlyCatBidsTuple = new TupleTag<String>() {
	};
	
	final static TupleTag<String> hourlyCat1BidsTuple = new TupleTag<String>() {
	};
	
	final static TupleTag<String> dailyCatSalesTuple = new TupleTag<String>() {
	};
	
	final static TupleTag<String> dailyCat1SalesTuple = new TupleTag<String>() {
	};
	
	final static TupleTag<String> hourlyCatSalesTuple = new TupleTag<String>() {
	};
	
	final static TupleTag<String> hourlyCat1SalesTuple = new TupleTag<String>() {
	};
	
	final static List<TupleTag<?>> tuples = Arrays.asList(dailyCat1BidsTuple, hourlyCatBidsTuple, hourlyCat1BidsTuple, dailyCatSalesTuple, dailyCat1SalesTuple, hourlyCatSalesTuple, hourlyCat1SalesTuple, errorsTuples);

	public static interface AggregatorPipeLineOptions extends PipelineOptions {
		@Description("Path of the file to read from")
		@Default.String("gs://tradmerawdata/*")
		String getInputFile();
		void setInputFile(String value);

		@Description("File for daily bids per Cat")
		String getDailyCatBids();
		void setDailyCatBids(String value);

		@Description("File for daily bids per Cat1")
		String getDailyCat1Bids();
		void setDailyCat1Bids(String value);

		@Description("File for hourly bids per Cat")
		String getHourlyCatBids();
		void setHourlyCatBids(String value);

		@Description("File for hourly bids per Cat1")
		String getHourlyCat1Bids();
		void setHourlyCat1Bids(String value);

		@Description("File for daily sales per Cat")
		String getDailyCatSales();
		void setDailyCatSales(String value);

		@Description("File for daily sales per Cat1")
		String getDailyCat1Sales();
		void setDailyCat1Sales(String value);

		@Description("File for hourly sales per Cat")
		String getHourlyCatSales();
		void setHourlyCatSales(String value);

		@Description("File for hourly sales per Cat1")
		String getHourlyCat1Sales();
		void setHourlyCat1Sales(String value);

		
		
		/**
		 * Returns "gs://${YOUR_STAGING_DIRECTORY}/counts.txt" as the default
		 * destination.
		 */
		public static class OutputFactory implements DefaultValueFactory<String> {
			@Override
			public String create(PipelineOptions options) {
				DataflowPipelineOptions dataflowOptions = options.as(DataflowPipelineOptions.class);
				if (dataflowOptions.getStagingLocation() != null) {
					return GcsPath.fromUri(dataflowOptions.getStagingLocation()).resolve("counts.txt").toString();
				} else {
					throw new IllegalArgumentException("Must specify --output or --stagingLocation");
				}
			}
		}

		String getErrorFile();
		void setErrorFile(String errorFile);
	}

	public static class GroupByCats extends DoFn<String, String> {
		// only listing with bids will be used
		@ProcessElement
		public void processElement(ProcessContext c) {
			// 994626942|-computers-cables-adaptors-networking|20151216
			// 045252|/computers/cables-adaptors/networking/auction-994626942.htm|Ethernet
			// Cable CAT6 Ethernet LAN 30M New. Pay now.|20|1200
			String line = c.element();
			
			if (line == null || line.trim().isEmpty()) {
				return;
			}

			try {
				BidParser parser = BidParser.create(line);
				
				String dailyCats = String.format("%s-%s", parser.getDay(), parser.getCat3());
				String dailyCat1s = String.format("%s-%s", parser.getDay(), parser.getCat1());
				String hourlyCats = String.format("%s-%s", parser.getDayHour(), parser.getCat3());
				String hourlyCat1s = String.format("%s-%s", parser.getDayHour(), parser.getCat1());
				int sales = parser.getAmount();
				int bids = parser.getNumberOfBids();
				
				//bids,dailyCatBids is
				c.output(String.format("%s|%d", dailyCats, bids));
				c.sideOutput(dailyCat1BidsTuple,  String.format("%s|%d", dailyCat1s, bids) );
				c.sideOutput(hourlyCatBidsTuple,  String.format("%s|%d", hourlyCats, bids) );
				c.sideOutput(hourlyCat1BidsTuple,  String.format("%s|%d", hourlyCat1s, bids) );
				
				//sales
				c.sideOutput(dailyCatSalesTuple, String.format("%s|%d", dailyCats, sales));
				c.sideOutput(dailyCat1SalesTuple,  String.format("%s|%d", dailyCat1s, sales) );
				c.sideOutput(hourlyCatSalesTuple,  String.format("%s|%d", hourlyCats, sales) );
				c.sideOutput(hourlyCat1SalesTuple,  String.format("%s|%d", hourlyCat1s, sales) );
				
			} catch (Exception e) {
				String error = String.format("%s: [%s], %s", "ERROR", line, e.getMessage());
				c.sideOutput(errorsTuples, error);
			}

		}
	}
	
	// ensure that we remove duplicates
	public static class ExtractKey extends DoFn<String, KV<String, Integer>> {

		@ProcessElement
		public void processElement(ProcessContext c) throws Exception {
			String line = c.element();
			String[] parts = line.split("\\|");
			int number = Integer.parseInt(parts[1]);
			c.output(KV.of(parts[0], number));
		}

	}
	
	public static class Debugger extends PTransform<PCollection<String>, PCollection<String>> {
		public PCollection<String> expand(PCollection<String> lines) {
			PCollection<String> result = lines.apply(MapElements.via(new SimpleFunction<String, String>() {
						@Override
						public String apply(String input) {
							System.err.println("W: " + input);
							return input;
						}
					}));
			return lines;
		}
	}
	
	public static class Aggregator extends PTransform<PCollection<String>, PCollection<String>> {

		public PCollection<String> expand(PCollection<String> lines) {
			PCollection<KV<String, Iterable<Integer>>> groupKeys = lines.apply(ParDo.of(new ExtractKey())).apply(GroupByKey.create());
			PCollection<KV<String, Integer>> groups = groupKeys.apply(Combine.groupedValues(Sum.ofIntegers()));
			PCollection<String> result = groups.apply(MapElements.via(new SimpleFunction<KV<String, Integer>, String>() {
						@Override
						public String apply(KV<String, Integer> input) {
							String entry = String.format("%s:%s", input.getKey(), input.getValue());
							return entry;
						}
					}));
			return result;
		}
	}

	public static Pipeline createPipeline(AggregatorPipeLineOptions bidsOptions) {
		Pipeline p = Pipeline.create(bidsOptions);
		String inputFile = bidsOptions.getInputFile();
		String filePrefix = extractPrefix(inputFile);

		PCollectionTuple results =	p
				.apply(TextIO.Read.withCompressionType(TextIO.CompressionType.AUTO).from(bidsOptions.getInputFile()))
				.apply(ParDo.withOutputTags(dailyCatBidsTuple, TupleTagList.of(tuples)).of(new GroupByCats()));
		
		//write errors to disk
		results.get(errorsTuples).apply(TextIO.Write.to(bidsOptions.getErrorFile()));
		
		//bids
		Aggregator agg = new Aggregator();
		Debugger debug = new Debugger();
		
		results.get(dailyCatBidsTuple).apply(agg).apply(debug).apply(TextIO.Write.to(getFileLocation(filePrefix, bidsOptions.getDailyCatBids())));
		results.get(dailyCat1BidsTuple).apply(agg).apply(TextIO.Write.to(getFileLocation(filePrefix, bidsOptions.getDailyCat1Bids())));
		results.get(hourlyCatBidsTuple).apply(agg).apply(TextIO.Write.to(getFileLocation(filePrefix, bidsOptions.getHourlyCatBids())));
		results.get(hourlyCat1BidsTuple).apply(agg).apply(TextIO.Write.to(getFileLocation(filePrefix, bidsOptions.getHourlyCat1Bids())));
		
		//sales
		results.get(dailyCatSalesTuple).apply(agg).apply(TextIO.Write.to(getFileLocation(filePrefix, bidsOptions.getDailyCatSales())));
		results.get(dailyCat1SalesTuple).apply(agg).apply(TextIO.Write.to(getFileLocation(filePrefix, bidsOptions.getDailyCat1Sales())));
		results.get(hourlyCatSalesTuple).apply(agg).apply(TextIO.Write.to(getFileLocation(filePrefix, bidsOptions.getHourlyCatSales())));
		results.get(hourlyCat1SalesTuple).apply(agg).apply(TextIO.Write.to(getFileLocation(filePrefix, bidsOptions.getHourlyCat1Sales())));
		
		return p;
	}

	private static String extractPrefix(String inputFile) {
		File file = new File(inputFile);
		Path path = file.toPath();
		int parts = path.getNameCount();
		return path.getName(parts-2).toString();
	}
	
	private static String getFileLocation(String prefix, String baseLocation) {
		return baseLocation + File.separator + prefix;
	}

	public static void main(String[] args) {
		AggregatorPipeLineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(AggregatorPipeLineOptions.class);
		Pipeline p = createPipeline(options);
		p.run();
	}
}