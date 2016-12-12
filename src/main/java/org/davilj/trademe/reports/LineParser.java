package org.davilj.trademe.reports;

import java.util.Optional;

public interface LineParser<T> {
	Optional<T> processline(String aLine);
}
