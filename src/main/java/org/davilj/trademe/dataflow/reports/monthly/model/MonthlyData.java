package org.davilj.trademe.dataflow.reports.monthly.model;


import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class MonthlyData {
	private ReportDate date;
	private List<SalesData> listOfData = new ArrayList<>();
	
	public MonthlyData(ReportDate date) {
		this.date = date;
	}
	
	public ReportDate getDate() {
		return date;
	}
	public void setDate(ReportDate date) {
		this.date = date;
	}
	public List<SalesData> getSalesData() {
		return listOfData;
	}
	public void setSalesData(List<SalesData> salesData) {
		this.listOfData = salesData;
	}
	public void addSalesData(SalesData salesData) {
		this.listOfData.add(salesData);
	}

	public void sort(Comparator<SalesData> comparator) {
		this.listOfData.sort(comparator);
	}

}
