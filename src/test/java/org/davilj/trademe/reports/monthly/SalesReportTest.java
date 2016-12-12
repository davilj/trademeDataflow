package org.davilj.trademe.reports.monthly;

import org.davilj.trademe.reports.SalesReportTemplate;
import org.davilj.trademe.reports.monthly.TopAvgPrices;
import org.davilj.trademe.reports.monthly.TopMaxSalesPrices;
import org.davilj.trademe.reports.monthly.TopNumberOfSales;
import org.davilj.trademe.reports.monthly.TopTotalSales;
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
