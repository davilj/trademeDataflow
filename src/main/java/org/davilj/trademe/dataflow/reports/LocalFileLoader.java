package org.davilj.trademe.dataflow.reports;

import java.io.File;
import java.io.FileFilter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class LocalFileLoader implements FileLoader{
	private File dir;
	private FileFilter fileFilter;
	
	public LocalFileLoader(String dirStr, FileFilter fileFilter) {
		this.dir = new File(dirStr);
		this.fileFilter = fileFilter;
		
		if (!this.dir.exists()) throw new RuntimeException(dirStr + " does not exist");
		if (!this.dir.isDirectory()) throw new RuntimeException(dirStr + " is not a directory");
	}

	@Override
	public Stream<StreamFile> getFiles() {
		return getFilesFromDir(dir);
	}
	
	private Stream<StreamFile> getFilesFromDir(File dir) {
		List<File> files = Arrays.asList(dir.listFiles(this.fileFilter));
		return files.stream()
				.map(d -> {
						List<StreamFile> fileList = new ArrayList<>();
						if (d.isDirectory()) {
							fileList.addAll(getFilesFromDir(d).collect(Collectors.toList()));
						} else if (d.isFile()) {
							fileList.add(new LocalStreamFile(d));
						}
						return fileList;
		}).flatMap(e -> e.stream());
	}
}
