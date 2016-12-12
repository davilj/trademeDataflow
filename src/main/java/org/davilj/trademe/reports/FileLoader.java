package org.davilj.trademe.reports;

import java.util.stream.Stream;

public interface FileLoader {

	Stream<StreamFile> getFiles();
}
