package org.davilj.trademe.dataflow.bids;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
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
import org.davilj.trademe.dataflow.model.helpers.BidParser;
//Import SLF4J packages.
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;

/**
 * A pipeline that run thought all listing, extract the bids (listing with
 * bids), group by cat and cat3, aggregate over month, month:day and
 * month:day:hour
 * 
 * @author daniev
 *
 */
public class Bids2BigQuery {
	final static String CAT1 = "cat1";
	final static String CATF = "catf";
	final static String CAT3 = "cat3";
	//date with hour precission
	final static String DATE = "date";
	final static String BIDS = "bids";
	final static String AMOUNT = "amount";
	
	public static interface BigQImport extends PipelineOptions {
		@Description("Path of the file to read from")
		@Default.String("gs://tradmerawdata/*")
		String getInputFile();

		void setInputFile(String value);

		@Description("Table Identifier")
		String getTable();

		void setTable(String value);

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

	// take a bid and convert into table row
	// amount
	public static class TableRowFactory extends DoFn<String, TableRow> {
		private static final Logger LOG = LoggerFactory.getLogger(TableRowFactory.class);

		
		@ProcessElement
		public void processElement(DoFn<String, TableRow>.ProcessContext c) throws Exception {
			SimpleDateFormat read = new SimpleDateFormat("yyyyMMddHH");
			SimpleDateFormat write = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			String bidStr = c.element();
			try {
				BidParser bid = BidParser.create(bidStr);
				TableRow row = new TableRow();
				row.set(CAT1, bid.getCat1());
				row.set(CATF, bid.getCategory());
				row.set(CAT3, bid.getCat3());
				Date date = read.parse(bid.getDayHour().trim());
				row.set(DATE, write.format(date));
				row.set(BIDS, bid.getNumberOfBids());
				row.set(AMOUNT, bid.getAmount());
				
				c.output(row);
			} catch (Exception e) {
				LOG.error("Could not parse: " + bidStr, e);
			}
		}
	}

	// extract only valid bids (a bid against listing), remove duplicates
	public static class buildTableRowFromBids extends PTransform<PCollection<String>, PCollection<TableRow>> {
		@Override
		public PCollection<TableRow> expand(PCollection<String> lines) {
			return lines.apply(ParDo.of(new TableRowFactory()));
		}
	}

	
	
	public static Pipeline createPipeline(BigQImport bigQImport) {
		//this input file is a dir "test/*"
		String input = bigQImport.getInputFile();
		String table = bigQImport.getTable();
		
		
		List<TableFieldSchema> fields = new ArrayList<>();
		  fields.add(new TableFieldSchema().setName(CAT1).setType("STRING"));
		  fields.add(new TableFieldSchema().setName(CAT3).setType("STRING"));
		  fields.add(new TableFieldSchema().setName(CATF).setType("STRING"));
		  fields.add(new TableFieldSchema().setName(DATE).setType("TIMESTAMP"));
		  fields.add(new TableFieldSchema().setName(BIDS).setType("INTEGER"));
		  fields.add(new TableFieldSchema().setName(AMOUNT).setType("INTEGER"));
		  TableSchema schema = new TableSchema().setFields(fields);
		
		Pipeline p = Pipeline.create(bigQImport);
		PCollection<TableRow> trBids = p.apply(TextIO.Read.withCompressionType(TextIO.CompressionType.AUTO).from(input))
				.apply("bids facotry", ParDo.of(new TableRowFactory()));
		
		//write to bigQuery
		trBids.apply("dd", BigQueryIO.Write.to(table).withSchema(schema).withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
	            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));
		
		return p;
	}

	public static void main(String[] args) {
		BigQImport options = PipelineOptionsFactory.fromArgs(args).withValidation().as(BigQImport.class);
		Pipeline p = createPipeline(options);
		p.run();
	}
}
