package org.davilj.trademe.dataflow.formatters;

import org.davilj.trademe.dataflow.model.Listing;

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
import com.google.cloud.dataflow.sdk.util.gcsfs.GcsPath;
import com.google.cloud.dataflow.sdk.values.PCollection;

public class ParseDataFiles {

	public static interface DailySalesOptions extends PipelineOptions {
		@Description("Path of the file to read from")
		@Default.String("gs://dataflow-samples/shakespeare/kinglear.txt")
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
	
	
	
	public static class ExtractBid extends DoFn<String, String> {
		
		@Override
		public void processElement(ProcessContext c) {
			//LatestListing [title=Mastering Photoshop 7 New. Pay now., link=/computers/software/other/auction-994633241.htm, closingTimeText=closes in 9 mins, bidInfo=, priceInfo=$15.00];, /Users/daniev/development/google/trademe/d_201512/20151214/0002-/201512140014.ll
			String line = c.element();
			if (line!=null && !line.trim().isEmpty() && line.startsWith("LatestListing")) { 
				try {
				Listing listing = Listing.getLineFactory().create(line);
				c.output(toString(listing));
				} catch (Exception e) {
					c.output("ERROR: " + line);
				}
			}
		}

		private String toString(Listing listing) {
			StringBuilder builder = new StringBuilder();
			builder.append(listing.getId());
			builder.append("|");
			builder.append(listing.getCat());
			builder.append("|");
			builder.append(listing.getClosingTime());
			builder.append("|");
			builder.append(listing.getLink());
			builder.append("|");
			builder.append(listing.getTitle());
			builder.append("|");
			builder.append(listing.getNumberOfBids()==null?"":listing.getNumberOfBids());
			builder.append("|");
			builder.append(listing.getListingprice()==null?"":listing.getListingprice());
			return builder.toString();
		}

		
	}
	
	public static class ExtractDetailsOfBids extends PTransform<PCollection<String>, PCollection<String>> {
		@Override
		public PCollection<String> apply(PCollection<String> lines) {
			return lines.apply(ParDo.of(new ExtractBid()));
		}
	}

	public static void main(String[] args) {
		DailySalesOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(DailySalesOptions.class);
		Pipeline p = createPipeline(options);
		p.run();
	}
	
	public static Pipeline createPipeline(DailySalesOptions dailySalesOptions) {
		Pipeline p = Pipeline.create(dailySalesOptions);
		p.apply(TextIO.Read.named("ReadLines").from(dailySalesOptions.getInputFile()).withCompressionType(TextIO.CompressionType.GZIP))
		.apply(new ExtractDetailsOfBids())
		.apply(TextIO.Write.named("WriteTransactions").to(dailySalesOptions.getOutput()));
		return p;
	}
}
