package org.davilj.trademe.reports.monthly.model;

public class SalesData {

	private int max;
	private int min;
	private int sum;
	private int numberOfSales;
	private ReportDate date;
	private String cat;

	public int getMax() {
		return max;
	}

	public void setMax(int max) {
		this.max = max;
	}

	public int getMin() {
		return min;
	}

	public void setMin(int min) {
		this.min = min;
	}

	public int getSum() {
		return sum;
	}

	public void setSum(int sum) {
		this.sum = sum;
	}

	public int getNumberOfSales() {
		return numberOfSales;
	}

	public void setNumberOfSales(int numberOfSales) {
		this.numberOfSales = numberOfSales;
	}

	public ReportDate getDate() {
		return date;
	}

	public void setDate(ReportDate date) {
		this.date = date;
	}

	public static SalesData create(String aLine) {
		// antiques-collectables-militaria-swords:14|250|49500|154180,,,,,,,,,,,,,,2|5990|10000|15990,2|4100|6500|10600,1|5990|5990|5990,4|250|39500|50940,,,,3|900|49500|56560,1|5000|5000|5000,,1|9100|9100|9100,,,,,,,
		// the first enty after cat is montly summary
		String[] parts = aLine.split(":");
		String[] entries = parts[1].split(",");
		String monthlyEntry = entries[0];
		String[] monthlyData = monthlyEntry.split("\\|");
		SalesData salesData = new SalesData();
		salesData.numberOfSales = Integer.parseInt(monthlyData[1]);
		salesData.min = Integer.parseInt(monthlyData[2]);
		salesData.max = Integer.parseInt(monthlyData[3]);
		salesData.sum = Integer.parseInt(monthlyData[4]);
		salesData.cat = parts[0];
		salesData.date = new ReportDate(monthlyData[0]);
		return salesData;
	}

	public String getCat() {
		return cat;
	}

	public void setCat(String cat) {
		this.cat = cat;
	}

	public static boolean isDailyReport(String aLine) {
		return true;
	}
}
