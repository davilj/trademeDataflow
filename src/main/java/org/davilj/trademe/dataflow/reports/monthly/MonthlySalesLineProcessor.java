package org.davilj.trademe.dataflow.reports.monthly;

import java.util.Optional;

import org.davilj.trademe.dataflow.reports.LineParser;
import org.davilj.trademe.dataflow.reports.monthly.model.SalesData;

public class MonthlySalesLineProcessor implements LineParser<SalesData>{

	@Override
	public Optional<SalesData> processline(String aLine) {
		if (SalesData.isDailyReport(aLine)) {
			SalesData salesData = SalesData.create(aLine);
			return Optional.of(salesData);
		} else {
			return Optional.empty();
		}
	}

}
