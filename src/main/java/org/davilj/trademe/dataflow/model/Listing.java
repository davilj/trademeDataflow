package org.davilj.trademe.dataflow.model;

import org.davilj.trademe.dataflow.model.helpers.ListingFactory;

public class Listing {
	private String title;
	private String link;
	private Integer numberOfBids;
	private Integer listingprice;
	private String cat;
	private String id;
	private String closingTime;
	
	
	public Listing(String id) {
		this.id = id;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public String getLink() {
		return link;
	}

	public void setLink(String link) {
		this.link = link;
	}

	
	public Integer getNumberOfBids() {
		return numberOfBids;
	}

	public void setNumberOfBids(Integer numberOfBids) {
		this.numberOfBids = numberOfBids;
	}

	public Integer getListingprice() {
		return listingprice;
	}

	public void setListingprice(int listingprice) {
		this.listingprice = listingprice;
	}

	public String getCat() {
		return cat;
	}

	public void setCat(String cat) {
		this.cat = cat;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getClosingTime() {
		return closingTime;
	}

	public void setClosingTime(String closingTime) {
		this.closingTime = closingTime;
	}

	public static ListingFactory getLineFactory() {
		return new ListingFactory();
	}
	
	
}
