package org.davilj.trademe.reports;

import java.util.stream.Stream;

public interface StreamFile {
	Stream<String> getLines();
}
