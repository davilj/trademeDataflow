package org.davilj.trademe.reports;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.stream.Stream;

public class LocalStreamFile implements StreamFile {
	private final File file;
	public LocalStreamFile(File _file) {
		this.file = _file;
	}

	@Override
	public Stream<String> getLines() {
		try {
			return Files.readAllLines(this.file.toPath()).stream();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

}
