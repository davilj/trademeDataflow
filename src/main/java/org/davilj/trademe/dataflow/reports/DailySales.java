package org.davilj.trademe.dataflow.reports;

import org.davilj.trademe.dataflow.formatters.ParseDataFiles.ExtractBid;
import org.davilj.trademe.dataflow.formatters.ParseDataFiles.ExtractDetailsOfBids;

import java.util.Iterator;

import org.davilj.trademe.dataflow.formatters.ParseDataFiles.DailySalesOptions.OutputFactory;
import org.davilj.trademe.dataflow.model.Listing;
import org.davilj.trademe.dataflow.model.helpers.BidParser;
import org.davilj.trademe.dataflow.model.helpers.ListingFactory;
import org.davilj.trademe.dataflow.model.helpers.ListingParser;
import org.davilj.trademe.dataflow.reports.DailySales.DailySalesOptions;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.DefaultValueFactory;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.DoFn.ProcessContext;
import com.google.cloud.dataflow.sdk.transforms.GroupByKey;
import com.google.cloud.dataflow.sdk.util.gcsfs.GcsPath;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionTuple;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.cloud.dataflow.sdk.values.TupleTagList;

public class DailySales {
	final static TupleTag<String> errors = new TupleTag<String>(){};
	final static TupleTag<String> validBids = new TupleTag<String>(){};
	
	public static String ERROR="ERROR";
	public static interface DailySalesOptions extends PipelineOptions {
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
	}
	
	public static class ExtractValidBid extends DoFn<String, String> {
		//only listing with bids
		@Override
		public void processElement(ProcessContext c) {
			//994626942|-computers-cables-adaptors-networking|20151216 045252|/computers/cables-adaptors/networking/auction-994626942.htm|Ethernet Cable CAT6 Ethernet LAN 30M New. Pay now.|20|1200
			String line = c.element();
			if (line!=null && !line.trim().isEmpty()) { 
				try {
					String[] parts = line.split("\\|");
					if (parts.length!=7) {
						throw new RuntimeException("Expecting 7 parts, but was " + parts.length);
					}
					String bids = parts[parts.length-2];
					if (!bids.trim().isEmpty()) {
						c.output(line);
					}
				} catch (Exception e) {
					String error = String.format("%s: [%s], %s",ERROR, line, e.getMessage());
					c.sideOutput(errors, error);
				}
			}
		}
	}


	public static class ExtractBidInfo extends DoFn<String, String> {
		//extract date, date and hour, and true date
		
		@Override
		public void processElement(ProcessContext c) throws Exception {
			//994626942|-computers-cables-adaptors-networking|20151216 045252|/computers/cables-adaptors/networking/auction-994626942.htm|Ethernet Cable CAT6 Ethernet LAN 30M New. Pay now.|20|1200
			String line = c.element();
			
			if (line!=null && !line.trim().isEmpty()) {
				ListingParser listingParser = ListingFactory.createParser(line);
				String dateStr = listingParser.getDateStr();
				String[] dateKeys = listingParser.extractDayAndHour(dateStr);
				String[] catKeys = listingParser.getCategory();
				String bidNumber = listingParser.getBidStr();
				String amount = listingParser.getAmountStr();
				//cat1, cat, day, hour, dayHour, bids, amount
				String bidData = String.format("%s|%s|%s|%s|%s|%s|%s", catKeys[0], catKeys[1], dateKeys[0], dateKeys[1], dateKeys[2], bidNumber, amount);
				c.output(bidData);
			}
		}
	}
	
	//ensure that we remove duplicates
	public static class ExtractKey extends DoFn<String, KV<String, String>> {

		@Override
		public void processElement(ProcessContext c) throws Exception {
			String line = c.element();
			ListingParser listingParser = ListingFactory.createParser(line);
			String id = listingParser.getId();
			c.output(KV.of(id, line));
		}
		
	}
	
	public static class RemoveDuplicate extends DoFn<KV<String, Iterable<String>>, String> {

		@Override
		public void processElement(DoFn<KV<String, Iterable<String>>, String>.ProcessContext c) throws Exception {
			String _bid = null;
			for (String bid : c.element().getValue()) {
				if (!bid.isEmpty()) {
					_bid = bid;
					break;
				}
			}
			
			if (_bid!=null) {
				c.output(_bid);
			}
		}
	}
	
	public static class Grouping extends DoFn<String, String> {
		//Side output, grouping in Cat1, cat, day, day-hour, hour for numberOfBids and amount
		@Override
		public void processElement(DoFn<String, String>.ProcessContext c) throws Exception {
			String bidStr = c.element();
			BidParser bid = BidParser.create(bidStr);
			
			
			
		}
	}

	public static Pipeline createPipeline(DailySalesOptions dailySalesOptions) {
		final TupleTag<KV<String, Integer>> cat1BidsPerDay = new TupleTag<KV<String, Integer>>(){};
		final TupleTag<KV<String, Integer>> cat1BidsPerDayHour = new TupleTag<KV<String, Integer>>(){};
		final TupleTag<KV<String, Integer>> cat1BidsPerHour = new TupleTag<KV<String, Integer>>(){};
		final TupleTag<KV<String, Integer>> catBidsPerDay = new TupleTag<KV<String, Integer>>(){};
		final TupleTag<KV<String, Integer>> catBidsPerDayHour = new TupleTag<KV<String, Integer>>(){};
		final TupleTag<KV<String, Integer>> catBidsPerHour = new TupleTag<KV<String, Integer>>(){};
		
		final TupleTag<KV<String, Integer>> cat1AmountPerDay = new TupleTag<KV<String, Integer>>(){};
		final TupleTag<KV<String, Integer>> cat1AmountPerDayHour = new TupleTag<KV<String, Integer>>(){};
		final TupleTag<KV<String, Integer>> cat1AmountPerHour = new TupleTag<KV<String, Integer>>(){};
		final TupleTag<KV<String, Integer>> catAmountPerDay = new TupleTag<KV<String, Integer>>(){};
		final TupleTag<KV<String, Integer>> catAmountPerDayHour = new TupleTag<KV<String, Integer>>(){};
		final TupleTag<KV<String, Integer>> catAmountPerHour = new TupleTag<KV<String, Integer>>(){};
		
		Pipeline p = Pipeline.create(dailySalesOptions);
		p.apply(TextIO.Read.named("ReadLines").from(dailySalesOptions.getInputFile()).withCompressionType(TextIO.CompressionType.GZIP))
		.apply(new ExtractValidBids())
		.apply(new RemoveDuplicates())
		.apply(TextIO.Write.named("WriteTransactions").to(dailySalesOptions.getOutput()));
		return p;
	}
	
	public static class RemoveDuplicates extends PTransform<PCollection<String>, PCollectionTuple> {
		@Override
		public PCollectionTuple apply(PCollection<String> bids) {
			return bids.apply(ParDo.withOutputTags(validBids, TupleTagList.of(errors)).of(new ExtractBidInfo()));
		}
	}
	
	public static class ExtractValidBids extends PTransform<PCollection<String>, PCollection<String>> {
		@Override
		public PCollection<String> apply(PCollection<String> lines) {
			return lines.apply(ParDo.of(new ExtractValidBid()))
					.apply(ParDo.of(new ExtractKey()))
					.apply(GroupByKey.create()).apply(ParDo.of(new RemoveDuplicate()));
		}
	}

	public static void main(String[] args) {
		DailySalesOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(DailySalesOptions.class);
		Pipeline p = createPipeline(options);
		p.run();
	}
}
