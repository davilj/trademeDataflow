package org.davilj.trademe.dataflow.reports.monthly;

import org.davilj.trademe.dataflow.reports.StreamFile;
import org.davilj.trademe.dataflow.reports.aggregate.DailyStats;

import java.util.Comparator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.davilj.trademe.dataflow.reports.FileLoader;
import org.davilj.trademe.dataflow.reports.LineParser;

/**
 * Read monthly aggregation file and build json file with sales data for a day
 * @author daniev
 */
public class FileParser<T> {
	final private FileLoader fileLoader;
	final private LineParser<T> lineProcessor;
	
	public FileParser(final FileLoader fileLoader, LineParser<T> lineProcessor) {
		this.fileLoader = fileLoader;
		this.lineProcessor = lineProcessor;
	}
	
	public List<T> generate() {
		final Function<StreamFile, Stream<T>> streamFunction = buildFunction(this.lineProcessor);
		return this.fileLoader.getFiles()
			.map(streamFunction::apply)
			.flatMap(sfs -> sfs.collect(Collectors.toList()).stream())
			.collect(Collectors.toList());
	}
	
	private Function<StreamFile, Stream<T>> buildFunction(LineParser<T> lineProcessor) {
		return new Function<StreamFile, Stream<T>>() {
			@Override
			public Stream<T> apply(StreamFile t) {
				return t.getLines()
						.map(l->lineProcessor.processline(l))
						.filter(optionaltype -> optionaltype.isPresent())
						.map(optionaltype -> optionaltype.get());
			}
		};
	}

}
