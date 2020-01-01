package com.github.csv2db;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import javax.sql.DataSource;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CsvImporter {
	private static final Logger logger = LoggerFactory.getLogger(CsvImporter.class);

	private static final String CSV_EXT = "csv";
	private static final String ZIP_EXT = "zip";
	private static final String CONSTRAINT_VIOLATION = "23505";

	private static final String GET_TABLE_STRUCTURE = "SELECT * " +
			"FROM information_schema.columns " +
			"WHERE table_schema = ?" +
			"    AND table_name   = ?";

	private final DataSource dataSource;

	public CsvImporter(DataSource dataSource) {
		this.dataSource = dataSource;
	}

	public List<LoadResult> load(File file) {
		switch (FilenameUtils.getExtension(file.getName())) {
			case CSV_EXT: {
				return Stream.of(doLoad(file)).collect(Collectors.toList());
			}
			case ZIP_EXT: {
				List<File> unzipped = unZip(file).stream()
						.sorted(Comparator.comparing(File::getName))
						.collect(Collectors.toList());
				return doLoad(unzipped);
			}
			default:
				throw new CsvImportException("Unsupported file format! Only csv and zip");
		}
	}

	private List<File> unZip(File zipFile) {
		Path dir;
		try {
			dir = Files.createTempDirectory(zipFile.getName() + "-", new FileAttribute[] {});
		} catch (IOException e) {
			throw new CsvImportException(e);
		}

		List<File> files = new ArrayList<>();
		byte[] buffer = new byte[1024];
		try (FileInputStream fis = new FileInputStream(zipFile)) {
			ZipInputStream zis = new ZipInputStream(fis);
			ZipEntry ze = zis.getNextEntry();
			while (ze != null) {
				String fileName = ze.getName();
				File newFile = new File(dir + File.separator + fileName);
				files.add(newFile);
				// create directories for sub directories in zip
				new File(newFile.getParent()).mkdirs();
				FileOutputStream fos = new FileOutputStream(newFile);
				int len;
				while ((len = zis.read(buffer)) > 0) {
					fos.write(buffer, 0, len);
				}
				fos.close();
				// close this ZipEntry
				zis.closeEntry();
				ze = zis.getNextEntry();
			}
			// close last ZipEntry
			zis.closeEntry();
			zis.close();
			return files;
		} catch (IOException e) {
			throw new CsvImportException(e);
		}
	}

	private List<LoadResult> doLoad(List<File> files) {
		return files.stream().map(this::doLoad).collect(Collectors.toList());
	}

	private LoadResult doLoad(File file) {
		logger.info("Started loading of master-data for file: {}", file.getName());
		int recordIndex = 0;
		List<LoadResult.SkippedRecord> recordsSkipped = new ArrayList<>();

		try (Connection connection = dataSource.getConnection();
				CSVParser csvParser = new CSVParser(new FileReader(file), CSVFormat.DEFAULT.withHeader().withDelimiter(';'))) {
			String tableName = getTableName(file);
			Map<String, String> columnTypes = getColumnTypes(connection, tableName);
			Map<String, Integer> headers = csvParser.getHeaderMap();
			String preparedInsert = prepareInsert(headers, tableName);
			logger.info("Prepared statement: {}", preparedInsert);

			PreparedStatement statement = connection.prepareStatement(preparedInsert);
			for (CSVRecord record : csvParser.getRecords()) {
				int columnIndex = 1;
				++recordIndex;
				try {
					logger.info("Inserting record: {}", recordIndex);
					for (String header : headers.keySet()) {
						String value = record.get(header);
						if (isNull(value)) {
							statement.setObject(columnIndex, null);
						} else {
							statement.setObject(columnIndex, convert(header, value, columnTypes));
						}
						++columnIndex;
					}
					statement.execute();

				} catch (SQLException e) {
					if (!CONSTRAINT_VIOLATION.equals(e.getSQLState())) {
						throw new CsvImportException(e);
					} else {
						recordsSkipped.add(new LoadResult.SkippedRecord(recordIndex, e.getMessage()));
						logger.warn("Record {} has constraint violation, so it was skipped: {}", recordIndex, e);
					}
				}
			}
		} catch (IOException | SQLException e) {
			throw new CsvImportException(e);
		}
		logger.info("Successfully processed master-data file: {}", file.getName());

		return new LoadResult(
				file.getName(),
				recordIndex,
				recordIndex - recordsSkipped.size(),
				recordsSkipped);
	}

	private boolean isNow(String value) {
		return "now()".equals(value) || "NOW()".equals(value);
	}

	private boolean isNull(String value) {
		return "null".equals(value) || "NULL".equals(value);
	}

	private String getTableName(File file) {
		String filename = FilenameUtils.getBaseName(file.getName());
		if (filename.contains("-")) {
			return filename.split("-")[1];
		}
		return filename;
	}

	public Table getTableInfo(String tableName) {
		List<Table.Column> columns = new ArrayList<>();
		try (Connection connection = dataSource.getConnection()) {
			PreparedStatement statement = connection.prepareStatement(GET_TABLE_STRUCTURE);
			statement.setString(1, connection.getSchema());
			statement.setString(2, tableName);
			ResultSet resultSet = statement.executeQuery();

			while (resultSet.next()) {
				columns.add(new Table.Column(
						resultSet.getString("column_name"),
						resultSet.getString("is_nullable"),
						resultSet.getString("data_type"),
						resultSet.getString("character_maximum_length")));
			}
		} catch (Exception e) {
			throw new TableInfoException(e);
		}

		StringBuilder stringBuilder = new StringBuilder();
		try (CSVPrinter printer = new CSVPrinter(stringBuilder, CSVFormat.DEFAULT)) {
			for (Table.Column column : columns) {
				printer.print(column.name);
			}
			return new Table(tableName, columns);
		} catch (Exception e) {
			throw new TableInfoException(e);
		}
	}

	private Map<String, String> getColumnTypes(Connection connection, String tableName) throws SQLException {
		Map<String, String> columnTypes = new HashMap<>();
		PreparedStatement statement = connection.prepareStatement(GET_TABLE_STRUCTURE);
		statement.setString(1, connection.getSchema());
		statement.setString(2, tableName);
		ResultSet resultSet = statement.executeQuery();
		if (resultSet.next()) {
			do {
				String columnName = resultSet.getString("column_name");
				String dataType = resultSet.getString("data_type");
				columnTypes.put(columnName.toLowerCase(), dataType.toLowerCase());
			} while (resultSet.next());
			return columnTypes;
		} else {
			throw new TableNotFoundException(tableName);
		}
	}

	private Object convert(
			String header,
			String value,
			Map<String, String> columnTypes) {
		String columnType = columnTypes.get(header.toLowerCase());

		if (columnType == null) {
			throw new IllegalArgumentException("No database column found by csv header=" + header);
		}

		if (StringUtils.isBlank(value)) {
			return null;
		}

		if (isNow(value)) {
			return LocalDateTime.now();
		}

		switch (columnType) {
			case "date":
				return LocalDate.parse(value);
			case "timestamp with time zone":
			case "timestamp without time zone":
				return Timestamp.valueOf(value);
			case "uuid":
				return UUID.fromString(value);
			case "boolean":
				return Boolean.valueOf(value);
			case "smallint":
			case "integer":
				return Integer.valueOf(value);
			case "bigint":
				return Long.valueOf(value);
			case "numeric":
			case "double precision":
				return Double.valueOf(value);
			default:
				return value;
		}
	}

	private String prepareInsert(Map<String, Integer> headerMap, String tableName) {
		String insert = "INSERT INTO \"" + tableName + "\" (%s) VALUES (%s);";
		String columns = String.join(",", headerMap.keySet());
		String params = headerMap.keySet().stream()
				.map(n -> "?")
				.collect(Collectors.joining(","));
		return String.format(insert, columns, params);
	}
}
