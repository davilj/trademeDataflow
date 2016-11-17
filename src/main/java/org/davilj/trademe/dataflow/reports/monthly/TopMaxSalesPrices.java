package org.davilj.trademe.dataflow.reports.monthly;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;

import org.davilj.trademe.dataflow.reports.SalesReportTemplate;
import org.davilj.trademe.dataflow.reports.monthly.model.Report;
import org.davilj.trademe.dataflow.reports.monthly.model.SalesData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TopMaxSalesPrices extends SalesReportTemplate {
	static final Logger LOG = LoggerFactory.getLogger(TopMaxSalesPrices.class);
	
	private TopMaxSalesPrices(String name, String sourceDir, String resultDir) {
		super(name, sourceDir, resultDir);
	}
	
	public static final TopMaxSalesPrices create(String name, String sourceDir, String resultDir) {
		return new TopMaxSalesPrices(name, sourceDir, resultDir);
	}
	
	public static final TopMaxSalesPrices createWithDefaults() {
		return new TopMaxSalesPrices(
					"Top Max Price for MontlySales",
					"/Users/daniev/development/google/trademe/dataflow/src/test/resources/topsales",
					"/Users/daniev/development/google/trademe/dataflow/src/test/resources/topsales/results");
	}
	
	protected Optional<Report> createReport(String name, List<SalesData> salesData) {
		Comparator<SalesData> comparator = new Comparator<SalesData>() {

			@Override
			public int compare(SalesData o1, SalesData o2) {
				return o2.getMax() - o1.getMax();
			}
		};
		return Report.create(name, salesData, comparator);
	}
}
