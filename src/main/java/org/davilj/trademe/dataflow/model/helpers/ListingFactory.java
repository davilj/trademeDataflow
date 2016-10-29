package org.davilj.trademe.dataflow.model.helpers;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.davilj.trademe.dataflow.model.Listing;

public class ListingFactory {

	static final String TITLE = "title";
	static final String LINK = "link";
	static final String CL_TIME = "closingTimeText";
	static final String BID_INFO = "bidInfo";
	static final String PRICE = "priceInfo";
	static final long SEC = 1000;
	static final long MIN = 60 * SEC;
	static final long HOUR = 60 * MIN;
	static final Pattern p = Pattern.compile("\\d+");

	public ListingFactory() {
	}

	// LatestListing [title=Mastering Photoshop 7 New. Pay now.,
	// link=/computers/software/other/auction-994633241.htm,
	// closingTimeText=closes in 9 mins, bidInfo=, priceInfo=$15.00];,
	// /Users/daniev/development/google/trademe/d_201512/20151214/0002-/201512140014.ll
	public Listing create(String line) {
		String dataLine = line.replace("LatestListing [", "");
		String[] parts = dataLine.split("];, ");
		long timeStamp = extractTimeStamp(parts[1]);
		String[] keys = { TITLE, LINK, CL_TIME, BID_INFO, PRICE };
		Map<String, Object> values = extract(parts[0].trim(), keys);
		String category = extractCategory(values.get(LINK).toString());
		String id = extractId(values.get(LINK).toString());
		String closingTime = extractTime(values.get(CL_TIME).toString(), timeStamp);

		Listing listing = new Listing(id);
		listing.setCat(category);
		Object bids = values.get(BID_INFO);
		if (bids != null)
			listing.setNumberOfBids(extractBidInfo(values.get(BID_INFO).toString()));
		listing.setClosingTime(closingTime);
		listing.setLink(values.get(LINK).toString());
		Object price = values.get(PRICE);
		if (price != null)
			listing.setListingprice(extractPrice(price.toString()));
		Object title = values.get(TITLE);
		if (title!=null)
			listing.setTitle(values.get(TITLE).toString());
		return listing;

	}

	private Integer extractBidInfo(String bidInfo) {
		Matcher m = p.matcher(bidInfo);
		if (m.find()) {
			return  (Integer.parseInt(m.group()));
		} else {
			return null;
		}
	}

	private Integer extractPrice(String bidAsStr) {
		if (bidAsStr != null) {
			String _clean = bidAsStr.replace("$", "").replace(",", ".").trim();
			return (int) (Double.parseDouble(_clean) * 100.00);
		}
		return null;
	}

	private String extractTime(String closeTimeStr, long timeStamp) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd HHmmss");
		String timeStr = closeTimeStr.trim();
		if (closeTimeStr.contains(("auction closed"))) {
			return sdf.format(new Date(timeStamp));
		}

		timeStr = timeStr.replace("closes in", "").trim();
		long _timeStamp = timeStamp + extractMilli(closeTimeStr, "sec") + extractMilli(closeTimeStr, "min")
				+ extractMilli(closeTimeStr, "hour");
		return sdf.format(new Date(_timeStamp));
	}

	private long extractMilli(String timeStr, String unit) {
		long multiplier = (unit == "sec" ? SEC : (unit == "min" ? MIN : HOUR));

		Matcher m = p.matcher(timeStr);
		if (m.find()) {
			return multiplier * (Long.parseLong(m.group()));
		} else {
			return 0;
		}
	}

	private String extractId(String linkStr) {
		String[] parts = linkStr.split("/");
		String idPart = parts[parts.length - 1];
		return idPart.replaceAll("auction-", "").replace(".htm", "").trim();
	}

	private String extractCategory(String linkStr) {
		String[] parts = linkStr.split("/");
		StringBuilder cat = new StringBuilder();
		for (int n = 0; n < parts.length - 1; n++) {
			if (n != 0) {
				cat.append("-");
			}
			cat.append(parts[n]);
		}
		return cat.toString().trim();
	}

	private Map<String, Object> extract(String line, String[] keys) {
		// Extract keys
		ArrayList<Key> keyList = new ArrayList<>();
		for (String key : keys) {
			int index = line.indexOf(key);
			if (index > -1) {
				Key newKey = new Key();
				newKey.name = key;
				newKey.index = index;
				keyList.add(newKey);
			}
		}

		// Sort Values
		Key[] sorted = keyList.toArray(new Key[keyList.size()]);
		Arrays.sort(sorted, new Comparator<Key>() {
			@Override
			public int compare(Key o1, Key o2) {
				return o1.index - o2.index;
			}
		});

		// Extract Values in order
		Map<String, Object> values = new HashMap<>();
		for (int index = 0; index < sorted.length; index++) {
			Key key = sorted[index];
			int start = line.indexOf("=", key.index) + 1;
			int end = line.length() - 1;
			if (index < sorted.length - 1) {
				Key nextKey = sorted[index + 1];
				end = nextKey.index;
			}
			String value = null;
			if (start < end) {
				value = line.substring(start, end).trim();
				if (value.trim().equals(",")) {
					value = null;
				} else if (value.endsWith(",")) {
					value = value.substring(0, value.length() - 1);
				}
			}
			values.put(key.name, value);
		}
		return values;
	}

	private long extractTimeStamp(String aString) {
		String[] parts = aString.split("/");
		String timeAsStr = parts[parts.length - 1].replace(".ll", "");
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");
		try {
			return sdf.parse(timeAsStr).getTime();
		} catch (ParseException e) {
			throw new RuntimeException(e);
		}
	}

	static class Key {
		String name;
		int index;

	}

	public static ListingParser createParser(String line) {
		return new ListingParser(line);
	}

}
