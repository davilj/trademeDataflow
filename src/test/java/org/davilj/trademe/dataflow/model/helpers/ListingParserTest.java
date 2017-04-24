package org.davilj.trademe.dataflow.model.helpers;

import static org.junit.Assert.*;

import org.junit.Test;

public class ListingParserTest {

	@Test
	public void testCat3_MoreThan3Parts() {
		String[] parts = { "one", "two", "three", "four" };
		testCats("one-two-three", parts);
	}

	@Test
	public void testCat3_3Parts() {
		String[] parts = { "one", "two", "three" };
		testCats("one-two-three", parts);
	}

	@Test
	public void testCat3_lessThan3Parts() {
		String[] parts = { "one", "two" };
		testCats("one-two", parts);
	}

	@Test
	public void testCat3_lessThan1Parts() {
		String[] parts = { "one" };
		testCats("one", parts);
	}

	private void testCats(final String expected, String[] parts) {
		ListingParser listingParser = new ListingParser(
				"994626942|-computers-cables-adaptors-networking|20151216 045252|/computers/cables-adaptors/networking/auction-994626942.htm|Ethernet Cable CAT6 Ethernet LAN 30M New. Pay now.|20|1200");
		String cat = listingParser.cat3(parts);
		assertEquals(expected, cat);
	}
}
