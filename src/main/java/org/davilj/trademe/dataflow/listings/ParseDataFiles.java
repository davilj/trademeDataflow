package org.davilj.trademe.dataflow.listings;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.util.gcsfs.GcsPath;
import org.apache.beam.sdk.values.PCollection;
import org.davilj.trademe.dataflow.model.Listing;


/**
 * Data flow that process raw uploadeded files and create listings (not a bid)
 * The difference between a listing and a bid is that a bid have a bid on it (a listing does not)
 * 
 * @author daniev
 *
 */
public class ParseDataFiles {

	public static interface DailySalesOptions extends PipelineOptions {
		@Description("Path of the file to read from")
		@Default.String("gs://trademeupload/*")
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
	
	
	/**
	 * Parse a line
	 * Create Listing object from line, create string with | to separate properties
	 *
	 */
	public static class ExtractListings extends DoFn<String, String> {
		
		@ProcessElement
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
	
	public static class ExtractDetailsOfListings extends PTransform<PCollection<String>, PCollection<String>> {
		@Override
		public PCollection<String> expand(PCollection<String> lines) {
			return lines.apply(ParDo.of(new ExtractListings()));
		}
	}

	public static void main(String[] args) {
		DailySalesOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(DailySalesOptions.class);
		Pipeline p = createPipeline(options);
		p.run();
	}
	
	public static Pipeline createPipeline(DailySalesOptions dailySalesOptions) {
		Pipeline p = Pipeline.create(dailySalesOptions);
		p.apply(TextIO.Read.withCompressionType(TextIO.CompressionType.AUTO).from(dailySalesOptions.getInputFile()))
		.apply("Extract Listings", new ExtractDetailsOfListings())
		.apply(TextIO.Write.to(dailySalesOptions.getOutput()));
		return p;
	}
}
