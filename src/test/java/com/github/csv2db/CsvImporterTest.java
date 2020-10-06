package com.github.csv2db;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.sql.DataSource;

import org.junit.Ignore;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

@Testcontainers
public class CsvImporterTest {

	@SuppressWarnings("rawtypes")
	@Container
	private final PostgreSQLContainer postgreSQLContainer = new PostgreSQLContainer<>().withDatabaseName("test")
			.withUsername("test").withPassword("test").withInitScript("init.sql");

	private final static String CSV_FILE = "table_name.csv";

	private final static String ZIP_FILE = "test.zip";

	@Test
	@Ignore
	public void shouldLoadTableFromCsvFile() throws URISyntaxException {
		CsvImporter csvImporter = new CsvImporter(getDataSource());

		URL url = getClass().getClassLoader().getResource(CSV_FILE);

		List<LoadResult> loadResult = csvImporter.load(new File(url.toURI()));
		assertEquals(1, loadResult.size());
		assertEquals(expectedCsvLoadResult(), loadResult.get(0));
	}

	@Test
	@Ignore
	public void shouldLoadTableFromZipFile() throws URISyntaxException {
		CsvImporter csvImporter = new CsvImporter(getDataSource());

		URL url = getClass().getClassLoader().getResource(ZIP_FILE);

		List<LoadResult> results = csvImporter.load(new File(url.toURI()));
		assertEquals(1, results.size());
		assertEquals(expectedZipLoadResult_1(), results.get(0));
		assertEquals(expectedZipLoadResult_2(), results.get(1));
	}

	private DataSource getDataSource() {
		HikariConfig hikariConfig = new HikariConfig();
		hikariConfig.setJdbcUrl(postgreSQLContainer.getJdbcUrl());
		hikariConfig.setUsername("test");
		hikariConfig.setPassword("test");
		return new HikariDataSource(hikariConfig);
	}

	private LoadResult expectedCsvLoadResult() {
		return new LoadResult("table_name.csv", 3, 2,
				Stream.of(new LoadResult.SkippedRecord(2, new Exception())).collect(Collectors.toList()));
	}

	private LoadResult expectedZipLoadResult_1() {
		return new LoadResult("1-table_name.csv", 1, 1, new ArrayList<LoadResult.SkippedRecord>());
	}

	private LoadResult expectedZipLoadResult_2() {
		return new LoadResult("2-table_name.csv", 1, 0,
				Stream.of(new LoadResult.SkippedRecord(1, new Exception())).collect(Collectors.toList()));
	}
}
