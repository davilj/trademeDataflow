package org.davilj.trademe.dataflow.reports.monthly;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;

import org.davilj.trademe.dataflow.reports.SalesReportTemplate;
import org.davilj.trademe.dataflow.reports.monthly.model.Report;
import org.davilj.trademe.dataflow.reports.monthly.model.SalesData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TopAvgPrices extends SalesReportTemplate {
	static final Logger LOG = LoggerFactory.getLogger(TopAvgPrices.class);
	
	private TopAvgPrices(String name, String sourceDir, String resultDir) {
		super(name, sourceDir, resultDir);
	}
	
	public static final TopAvgPrices create(String name, String sourceDir, String resultDir) {
		return new TopAvgPrices(name, sourceDir, resultDir);
	}
	
	public static final TopAvgPrices createWithDefaults() {
		return new TopAvgPrices(
					"Top AvgPrice MontlySales",
					"/Users/daniev/development/google/trademe/dataflow/src/test/resources/topsales",
					"/Users/daniev/development/google/trademe/dataflow/src/test/resources/topsales/results");
	}
	
	protected Optional<Report> createReport(String name, List<SalesData> salesData) {
		Comparator<SalesData> comparator = new Comparator<SalesData>() {

			@Override
			public int compare(SalesData o1, SalesData o2) {
				Double avg1 = ((double)o1.getSum())/((double)o1.getNumberOfSales());
				Double avg2 = ((double)o2.getSum())/((double)o2.getNumberOfSales());
				return (int)(avg2 - avg1);
			}
		};
		return Report.create(name, salesData, comparator);
	}
}
