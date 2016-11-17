package org.davilj.trademe.dataflow.reports;

import java.util.stream.Stream;

public interface StreamFile {
	Stream<String> getLines();
}
