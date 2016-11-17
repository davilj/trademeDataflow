package org.davilj.trademe.dataflow.reports.monthly;

import org.davilj.trademe.dataflow.reports.SalesReportTemplate;
import org.junit.Test;

public class SalesReportTest {

	@Test
	public void testTopSumOfSales() {
		SalesReportTemplate report = TopTotalSales.createWithDefaults();
		report.run();
	}
	
	@Test
	public void testNumberOfSales() {
		SalesReportTemplate report = TopNumberOfSales.createWithDefaults();
		report.run();
	}
	
	@Test
	public void testItemSales() {
		SalesReportTemplate report = TopMaxSalesPrices.createWithDefaults();
		report.run();
	}
	
	@Test
	public void testAvgSales() {
		SalesReportTemplate report = TopAvgPrices.createWithDefaults();
		report.run();
	}

}
