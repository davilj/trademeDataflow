package org.davilj.trademe.dataflow.model.helpers;

public class ListingParser {
	private String[] parts; 
	public ListingParser(String line) {
		//994626942|-computers-cables-adaptors-networking|20151216 045252|/computers/cables-adaptors/networking/auction-994626942.htm|Ethernet Cable CAT6 Ethernet LAN 30M New. Pay now.|20|1200
		parts = line.split("\\|");
		if (parts.length!=7) {
			throw new RuntimeException("Expecting 7 parts, but was " + parts.length);
		}
	}

	public String getDateStr() {
		return parts[2];
	}
	
	public String[] getCategory() {
		String category = parts[1];
		if (category.startsWith("-")) {
			category = category.substring(1, category.length());
		}
		String[] catParts =  category.split("-");
		
		String[] catInfo = {catParts[0], cat3(catParts), category};
		return catInfo;
	}
	
	//build category name 3 levels deep of full category
	//if less that 3 levels deep return full
	protected String cat3(String[] catInfo) {
		int end = (catInfo.length > 3)?3:catInfo.length;
		StringBuilder cat = new StringBuilder();
		cat.append(catInfo[0]);
		for (int n=1; n < end; n++) {
			cat.append("-");
			cat.append(catInfo[n]);
		}
		return cat.toString();
	}
	
	public String[] extractDayAndHour(String timeText) {
		String[] parts = timeText.trim().split(" ");
		String day = parts[0];
		String hour = parts[1].substring(0, 2);
		String dayHour = day+hour;
		String[] timeDetails =  {day, hour, dayHour}; 
		return timeDetails;
	}

	public String getBidStr() {
		return parts[5];
	}

	public String getAmountStr() {
		return parts[6];
	}

	public String getId() {
		return parts[0];
	}

}
