package org.davilj.trademe.dataflow.model.helpers;

public class BidParser {
	private String originalString;
	private String[] parts;
	public BidParser(String bidStr) {
		this.originalString = bidStr;
		String[] bidParts = bidStr.split("\\|");
		//cat1, ca3, cat, day, hour, dayHour, bids, amount
		if (bidParts.length!=8) {
			throw new RuntimeException("Expecting 7 parts, but was " + bidParts.length);
		}
		this.parts = bidParts;
	}
	
	public String getCat1() {
		return this.parts[0];
	}
	
	public String getCat3() {
		return this.parts[1];
	}
	
	public String getCategory() {
		return this.parts[2];
	}
	
	public String getDay() {
		return this.parts[3];
	}
	
	public String getHour() {
		return this.parts[4];
	}
	
	public String getDayHour() {
		return this.parts[5];
	}
	
	public int getNumberOfBids() {
		return Integer.parseInt(this.parts[6].trim());
	}
	
	public int getAmount() {
		return Integer.parseInt(this.parts[7].trim());
	}
	
	public static BidParser create(String bidStr) {
		return new BidParser(bidStr);
	}

	@Override
	public String toString() {
		return originalString;
	}
}
