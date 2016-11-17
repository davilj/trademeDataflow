package org.davilj.trademe.dataflow.reports.top;

public class StatsInfo {
	String data;
	int year;
	int month;
	int day;
	int hour;
	String dateInfo;
	String cat;
	int min;
	int max;
	int numberOfTransaction;
	int sum;
	
	public static StatsInfo from(String aString) {
		// business|20151223:6|116019|399|51100
		String[] parts = aString.split(":");
		String[] keyParts = parts[0].split("\\|");
		StatsInfo statsInfo = new StatsInfo();
		statsInfo.data = aString;
		statsInfo.cat = keyParts[0];
		statsInfo.dateInfo = keyParts[1];
		statsInfo.year = Integer.parseInt(statsInfo.dateInfo.substring(0, 4));
		statsInfo.month = Integer.parseInt(statsInfo.dateInfo.substring(4,6));
		statsInfo.day = Integer.parseInt(statsInfo.dateInfo.substring(6,8));
		statsInfo.hour = (statsInfo.dateInfo.length()>8)?Integer.parseInt(statsInfo.dateInfo.substring(8,statsInfo.dateInfo.length())):0;
		
		String[] valueParts = parts[1].split("\\|");
		statsInfo.numberOfTransaction=Integer.parseInt(valueParts[0]);
		statsInfo.sum=Integer.parseInt(valueParts[1]);
		statsInfo.min=Integer.parseInt(valueParts[2]);
		statsInfo.max=Integer.parseInt(valueParts[3]);
		return statsInfo;
	}

	@Override
	public String toString() {
		return "S[year=" + year + ", month=" + month + ", day=" + day + ", cat=" + cat + ", min=" + min
				+ ", max=" + max + ", numberOfTransaction=" + numberOfTransaction + ", sum=" + sum + "]";
	}

	public void mergeMin(int _min) {
		this.min =  (this.min<_min)?this.min:_min;
	}

	public void mergeMax(int _max) {
		this.max = (this.max>_max)?this.max:_max;
	}

	public String getDate() {
		return this.dateInfo;
	}
	
}


