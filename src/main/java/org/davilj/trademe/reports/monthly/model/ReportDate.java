package org.davilj.trademe.reports.monthly.model;

public class ReportDate {
	private int year;
	private int month;
	private int day;
	
	public ReportDate(String dateStr) {
		this.year = Integer.parseInt(dateStr.substring(0,4));
		this.month = Integer.parseInt(dateStr.substring(4,6));
		if (dateStr.length()>6) {
			this.day = Integer.parseInt(dateStr.substring(6,8));
		} else {
			this.day=0;
		}
	}
	
	public int dateAsAInt() {
		return this.year * 1000 + this.month * 100 + this.day;
	}
	
	public int getYear() {
		return year;
	}

	public void setYear(int year) {
		this.year = year;
	}

	public int getMonth() {
		return month;
	}

	public void setMonth(int month) {
		this.month = month;
	}

	public int getDay() {
		return day;
	}

	public void setDay(int day) {
		this.day = day;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + day;
		result = prime * result + month;
		result = prime * result + year;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ReportDate other = (ReportDate) obj;
		if (day != other.day)
			return false;
		if (month != other.month)
			return false;
		if (year != other.year)
			return false;
		return true;
	}
	
	
}
