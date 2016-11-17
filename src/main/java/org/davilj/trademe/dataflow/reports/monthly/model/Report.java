package org.davilj.trademe.dataflow.reports.monthly.model;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class Report {
	private String name;
	private List<MonthlyData> monthlyData = new ArrayList<>();
	
	public Report(String name) {
		this.name=name;
	}
	
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public List<MonthlyData> getMonthlyData() {
		return monthlyData;
	}

	public void setMonthlyData(List<MonthlyData> monthlyData) {
		this.monthlyData = monthlyData;
	}

	private void addMonthlyData(MonthlyData data) {
		this.monthlyData.add(data);
	}
	
	public static Optional<Report> create(String name, List<SalesData> listOfSalesData, Comparator<SalesData> comparator) {
		Map<ReportDate, MonthlyData> db = new HashMap<>();
		if (listOfSalesData.isEmpty()) return Optional.empty();
		for (SalesData salesData : listOfSalesData) {
			ReportDate key = salesData.getDate();
			db.putIfAbsent(key, new MonthlyData(key));
			MonthlyData data = db.get(key);
			data.addSalesData(salesData);
		}
		
		//always sort days in reverse order
		List<ReportDate> dates = new ArrayList<>(db.keySet());
		dates.sort(new Comparator<ReportDate>() {
			@Override
			public int compare(ReportDate o1, ReportDate o2) {
				return o2.dateAsAInt()-o1.dateAsAInt();
			}
		});
		
		Report report  = new Report(name);
		
		for (ReportDate date : dates) {
			MonthlyData monthlyData = db.get(date);
			monthlyData.sort(comparator);
			report.addMonthlyData(monthlyData);
		}
		return Optional.of(report);
	}
}
