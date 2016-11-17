package org.davilj.trademe.dataflow.reports;

import java.io.File;
import java.io.FileFilter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Optional;

import org.davilj.trademe.dataflow.reports.monthly.FileParser;
import org.davilj.trademe.dataflow.reports.monthly.MonthlySalesLineProcessor;
import org.davilj.trademe.dataflow.reports.monthly.TopTotalSales;
import org.davilj.trademe.dataflow.reports.monthly.model.Report;
import org.davilj.trademe.dataflow.reports.monthly.model.SalesData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public abstract class SalesReportTemplate {
	static final Logger LOG = LoggerFactory.getLogger(TopTotalSales.class);
	final protected String reduceName;
	final protected String sourceDir;
	final protected String resultDir;

	public SalesReportTemplate(String name, String sourceDir, String resultDir) {
		this.reduceName = name.replaceAll(" ", "_").trim();
		this.sourceDir = sourceDir;
		this.resultDir = resultDir;
	}

	protected abstract Optional<Report> createReport(String name, List<SalesData> salesData);
	
	public void run() {
		LocalFileLoader lfl = new LocalFileLoader(this.sourceDir, new FileFilter() {
			@Override
			public boolean accept(File pathname) {
				if (pathname.isDirectory())
					return false;
				return pathname.getName().startsWith("results");
			}
		});
		LineParser<SalesData> lineProcessor = new MonthlySalesLineProcessor();

		FileParser<SalesData> monthyTopSalesReport = new FileParser<>(lfl, lineProcessor);
		Optional<Report> report = createReport(this.reduceName, monthyTopSalesReport.generate());

		if (report.isPresent()) {
			try {
				File reportDir = new File(this.resultDir);
				File reportFile = generateJSONReport(report.get(), reportDir);
			} catch (Exception e) {
				LOG.error("Could not write report", e);
			}
		} else {
			System.err.println("No report generated");
		}
	}

	private File generateJSONReport(Report report, File reportDir)
			throws JsonGenerationException, JsonMappingException, IOException {
		reportDir.mkdirs();
		ObjectMapper mapper = new ObjectMapper();
		File file = new File(reportDir + File.separator + report.getName() + ".json");
		if (file.exists()) {
			Files.deleteIfExists(file.toPath());
		}
		FileOutputStream out = new FileOutputStream(file);
		mapper.writerWithDefaultPrettyPrinter().writeValue(out, report);
		return file;
	}

}