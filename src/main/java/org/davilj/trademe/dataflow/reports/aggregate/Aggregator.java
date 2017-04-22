package org.davilj.trademe.dataflow.reports.aggregate;

import java.util.ArrayList;
import java.util.List;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
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
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.util.gcsfs.GcsPath;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.davilj.trademe.dataflow.model.helpers.BidParser;
import org.davilj.trademe.dataflow.model.helpers.ListingFactory;
import org.davilj.trademe.dataflow.model.helpers.ListingParser;
//Import SLF4J packages.
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A pipeline that run thought all bids, aggregate over hour and day and
 * group by cat and cat3
 * @author daniev
 *
 */
public class Aggregator {
	final static TupleTag<String> errorsTag = new TupleTag<String>() {
	};
	final static TupleTag<String> validBidsTag = new TupleTag<String>() {
	};
	
	public static String ERROR = "ERROR";

	public static interface AggregateOptions extends PipelineOptions {
		@Description("Path of the file to read from")
		@Default.String("gs://trademedata/d_201512.merge.zip")
		String getInputFile();

		void setInputFile(String value);

		@Description("Path of the file to write to")
		@Default.InstanceFactory(OutputFactory.class)
		String getOutput();

		void setOutput(String value);

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

	public static class ExtractValidBid extends DoFn<String, String> {
		// only listing with bids
		@ProcessElement
		public void processElement(ProcessContext c) {
			// 994626942|-computers-cables-adaptors-networking|20151216
			// 045252|/computers/cables-adaptors/networking/auction-994626942.htm|Ethernet
			// Cable CAT6 Ethernet LAN 30M New. Pay now.|20|1200
			String line = c.element();
			if (line != null && !line.trim().isEmpty()) {
				try {
					String[] parts = line.split("\\|");
					if (parts.length != 7) {
						throw new RuntimeException("Expecting 7 parts, but was " + parts.length);
					}
					String bids = parts[parts.length - 2];
					if (!bids.trim().isEmpty()) {
						c.output(line);
					}
				} catch (Exception e) {
					String error = String.format("%s: [%s], %s", ERROR, line, e.getMessage());
					c.sideOutput(errorsTag, error);
				}
			}
		}
	}

	// extract cat1, cat, date, date and hour, amount and number of bids
	public static class ExtractBidInfo extends DoFn<String, String> {
		
		@ProcessElement
		public void processElement(ProcessContext c) throws Exception {
			// 994626942|-computers-cables-adaptors-networking|20151216
			// 045252|/computers/cables-adaptors/networking/auction-994626942.htm|Ethernet
			// Cable CAT6 Ethernet LAN 30M New. Pay now.|20|1200
			String line = c.element();

			if (line != null && !line.trim().isEmpty()) {
				ListingParser listingParser = ListingFactory.createParser(line);
				String dateStr = listingParser.getDateStr();
				String[] dateKeys = listingParser.extractDayAndHour(dateStr);
				String[] catKeys = listingParser.getCategory();
				String bidNumber = listingParser.getBidStr();
				String amount = listingParser.getAmountStr();
				// cat1, cat, day, hour, dayHour, bids, amount
				String bidData = String.format("%s|%s|%s|%s|%s|%s|%s", catKeys[0], catKeys[1], dateKeys[0], dateKeys[1],
						dateKeys[2], bidNumber, amount);
				c.output(bidData);
			}
		}
	}

	// ensure that we remove duplicates
	public static class ExtractKey extends DoFn<String, KV<String, String>> {

		@ProcessElement
		public void processElement(ProcessContext c) throws Exception {
			String line = c.element();
			ListingParser listingParser = ListingFactory.createParser(line);
			String id = listingParser.getId();
			c.output(KV.of(id, line));
		}

	}

	public static class RemoveDuplicate extends DoFn<KV<String, Iterable<String>>, String> {

		@ProcessElement
		public void processElement(DoFn<KV<String, Iterable<String>>, String>.ProcessContext c) throws Exception {
			String _bid = null;
			for (String bid : c.element().getValue()) {
				if (!bid.isEmpty()) {
					_bid = bid;
					break;
				}
			}

			if (_bid != null) {
				c.output(_bid);
			}
		}
	}

	// take each listing and convert it into cat (or cat1), day (or dayhour) and
	// amount
	public static class ExtratDailyData extends DoFn<String, String[]> {
		private static final Logger LOG = LoggerFactory.getLogger(ExtratDailyData.class);

		// Side output, grouping in Cat1, cat, day, day-hour, hour for
		// numberOfBids and amount
		@ProcessElement
		public void processElement(DoFn<String, String[]>.ProcessContext c) throws Exception {
			String bidStr = c.element();
			try {
				BidParser bid = BidParser.create(bidStr);
				String cat1 = bid.getCat1();
				String cat = bid.getCategory();
				String date = bid.getDay();
				String dateTime = bid.getDayHour();
				Integer amount = bid.getAmount();

				String key = "%s|%s|%d";

				String[] results = { String.format(key, cat1, date, amount), String.format(key, cat, date, amount),
						String.format(key, cat1, dateTime, amount), String.format(key, cat, dateTime, amount) };

				c.output(results);
			} catch (Exception e) {
				LOG.error("Could not parse: " + bidStr);
			}
		}
	}

	public static class RemoveDuplicates extends PTransform<PCollection<String>, PCollectionTuple> {
		@Override
		public PCollectionTuple expand(PCollection<String> bids) {
			return bids.apply(ParDo.withOutputTags(validBidsTag, TupleTagList.of(errorsTag)).of(new ExtractBidInfo()));
		}
	}

	// extract only valid bids (a bid against listing), remove duplicates
	public static class ExtractValidBids extends PTransform<PCollection<String>, PCollection<String>> {
		@Override
		public PCollection<String> expand(PCollection<String> lines) {
			return lines.apply(ParDo.of(new ExtractValidBid())).apply(ParDo.of(new ExtractKey()))
					.apply(GroupByKey.<String, String>create()).apply(ParDo.of(new RemoveDuplicate()));
		}
	}

	// map each transaction to cat-1, cat, day and day+hour
	public static class Classifer extends PTransform<PCollection<String>, PCollection<String>> {
		private static final Logger LOG = LoggerFactory.getLogger(Classifer.class);

		public PCollection<String> expand(PCollection<String> lines) {
			return lines.apply(ParDo.of(new ExtratDailyData()))
					.apply(FlatMapElements.via(new SimpleFunction<String[], Iterable<KV<String, Integer>>>() {
						@Override
						public Iterable<KV<String, Integer>> apply(String[] input) {
							List<KV<String, Integer>> results = new ArrayList<>();

							for (String data : input) {
								try {
									String[] parts = data.split("\\|");
									Integer value = parts[2].isEmpty() ? 0 : Integer.parseInt(parts[2]);
									results.add(KV.of(String.format("%s|%s", parts[0], parts[1]), value));
								} catch (Exception e) {
									LOG.error("Data: [" + data + "]", e);
								}
							}

							return results;
						}
					})).apply(Combine.<String, Integer, String>perKey(new DailyStats()))
					.apply(MapElements.via(new SimpleFunction<KV<String, String>, String>() {
						@Override
						public String apply(KV<String, String> input) {
							return input.getKey() + ":" + input.getValue();
						}
					}));
		}
	}

	public static Pipeline createPipeline(AggregateOptions dailySalesOptions) {
		Pipeline p = Pipeline.create(dailySalesOptions);
		p.getCoderRegistry().registerCoder(DailyStats.Stats.class, DailyStats.getCoder());

		PCollectionTuple tuples = p
				.apply(TextIO.Read.from(dailySalesOptions.getInputFile())
						.withCompressionType(TextIO.CompressionType.GZIP))
				.apply(new ExtractValidBids()).apply(new RemoveDuplicates());
		tuples.get(errorsTag).apply(TextIO.Write.to(dailySalesOptions.getErrorFile()));
		tuples.get(validBidsTag).apply(new Classifer())
				.apply(TextIO.Write.to(dailySalesOptions.getOutput()));
		return p;
	}

	public static void main(String[] args) {
		AggregateOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(AggregateOptions.class);
		Pipeline p = createPipeline(options);
		p.run();
	}
}
