package org.davilj.trademe.reports.monthly;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;

import org.davilj.trademe.reports.SalesReportTemplate;
import org.davilj.trademe.reports.monthly.model.Report;
import org.davilj.trademe.reports.monthly.model.SalesData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TopNumberOfSales extends SalesReportTemplate {
	static final Logger LOG = LoggerFactory.getLogger(TopNumberOfSales.class);
	
	private TopNumberOfSales(String name, String sourceDir, String resultDir) {
		super(name, sourceDir, resultDir);
	}
	
	public static final TopNumberOfSales create(String name, String sourceDir, String resultDir) {
		return new TopNumberOfSales(name, sourceDir, resultDir);
	}
	
	public static final TopNumberOfSales createWithDefaults() {
		return new TopNumberOfSales(
					"Top Number of MontlySales",
					"/Users/daniev/development/google/trademe/dataflow/src/test/resources/topsales",
					"/Users/daniev/development/google/trademe/dataflow/src/test/resources/topsales/results");
	}
	
	protected Optional<Report> createReport(String name, List<SalesData> salesData) {
		Comparator<SalesData> comparator = new Comparator<SalesData>() {

			@Override
			public int compare(SalesData o1, SalesData o2) {
				return o2.getNumberOfSales() - o1.getNumberOfSales();
			}
		};
		return Report.create(name, salesData, comparator);
	}
}
