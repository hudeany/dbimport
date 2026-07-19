package de.soderer.dbimport;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;

import javax.sql.DataSource;

import de.soderer.utilities.IoUtilities;
import de.soderer.utilities.Utilities;
import de.soderer.utilities.csv.CsvFormat;
import de.soderer.utilities.csv.CsvWriter;
import de.soderer.utilities.db.DbUtilities;
import de.soderer.utilities.db.data.DbVendor;

public class TestDbUtilities {
	public static String readout(final DataSource dataSource, final String statementString, final char separator, final Character stringQuote) throws Exception {
		try (Connection connection = dataSource.getConnection()) {
			return readout(connection, statementString, separator, stringQuote);
		}
	}

	public static String readout(final Connection connection, final String statementString, final char separator, final Character stringQuote) throws Exception {
		final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		readoutInOutputStream(connection, statementString, outputStream, StandardCharsets.UTF_8, separator, stringQuote);
		return new String(outputStream.toByteArray(), StandardCharsets.UTF_8);
	}

	public static String readoutTable(final DataSource dataSource, final String tableName, final char separator, final Character stringQuote) throws Exception {
		try (Connection connection = dataSource.getConnection()) {
			return readoutTable(connection, tableName, separator, stringQuote);
		}
	}

	public static String readoutTable(final Connection connection, final String tableName) throws Exception {
		return readoutTable(connection, tableName, ';', '"');
	}

	public static String readoutTable(final Connection connection, final String tableName, final char separator, final Character stringQuote) throws Exception {
		if (connection == null) {
			throw new Exception("Invalid empty connection for getColumnNames");
		} else if (Utilities.isBlank(tableName)) {
			throw new Exception("Invalid empty tableName for getColumnNames");
		} else {
			final DbVendor dbVendor = DbUtilities.getDbVendor(connection);
			final List<String> columnNames = new ArrayList<>(DbUtilities.getColumnNames(connection, tableName));
			Collections.sort(columnNames);
			final List<String> keyColumnNames = new ArrayList<>(DbUtilities.getPrimaryKeyColumns(connection, tableName));
			Collections.sort(keyColumnNames);
			final List<String> readoutColumns = new ArrayList<>();
			readoutColumns.addAll(keyColumnNames);
			for (final String columnName : columnNames) {
				if (!Utilities.containsIgnoreCase(readoutColumns, columnName)) {
					readoutColumns.add(columnName);
				}
			}
			String orderPart = "";
			if (!keyColumnNames.isEmpty()) {
				orderPart = " ORDER BY " + DbUtilities.joinColumnVendorEscaped(dbVendor, keyColumnNames);
			}
			return readout(connection, "SELECT " + DbUtilities.joinColumnVendorEscaped(dbVendor, readoutColumns) + " FROM " + tableName + orderPart, separator, stringQuote);
		}
	}

	public static int readoutInOutputStream(final DataSource dataSource, final String statementString, final OutputStream outputStream, final Charset encoding, final char separator, final Character stringQuote) throws Exception {
		try (Connection connection = dataSource.getConnection()) {
			return readoutInOutputStream(connection, statementString, outputStream, encoding, separator, stringQuote);
		}
	}

	public static int readoutInOutputStream(final Connection connection, final String statementString, final OutputStream outputStream, final Charset encoding, final char separator, final Character stringQuote) throws Exception {
		final DbVendor dbVendor = DbUtilities.getDbVendor(connection);
		try (Statement statement = connection.createStatement();
				ResultSet resultSet = statement.executeQuery(statementString)) {
			statement.setFetchSize(100);
			try (CsvWriter csvWriter = new CsvWriter(outputStream, encoding, new CsvFormat().withSeparator(separator).withStringQuote(stringQuote))) {
				final ResultSetMetaData metaData = resultSet.getMetaData();

				final List<String> headers = new ArrayList<>();
				for (int i = 1; i <= metaData.getColumnCount(); i++) {
					headers.add(metaData.getColumnLabel(i));
				}
				csvWriter.writeValues(headers);

				while (resultSet.next()) {
					final List<String> values = new ArrayList<>();
					for (int i = 1; i <= metaData.getColumnCount(); i++) {
						if (metaData.getColumnType(i) == Types.BLOB
								|| metaData.getColumnType(i) == Types.BINARY
								|| metaData.getColumnType(i) == Types.VARBINARY
								|| metaData.getColumnType(i) == Types.LONGVARBINARY) {
							if (dbVendor == DbVendor.SQLite || dbVendor == DbVendor.PostgreSQL || dbVendor == DbVendor.Cassandra) {
								// Database vendor does not allow "resultSet.getBlob(i)"
								try (InputStream input = resultSet.getBinaryStream(metaData.getColumnName(i))) {
									if (input != null) {
										final byte[] data = IoUtilities.toByteArray(input);
										values.add(Base64.getEncoder().encodeToString(data));
									} else {
										values.add("");
									}
								} catch (@SuppressWarnings("unused") final Exception e) {
									// NULL blobs throw a NullpointerException in SQLite
									values.add("");
								}
							} else {
								final Blob blob = resultSet.getBlob(i);
								if (resultSet.wasNull()) {
									values.add("");
								} else {
									try (InputStream input = blob.getBinaryStream()) {
										final byte[] data = IoUtilities.toByteArray(input);
										values.add(Base64.getEncoder().encodeToString(data));
									}
								}
							}
						} else if (dbVendor == DbVendor.SQLite && "DATE".equals(metaData.getColumnTypeName(i))) {
							final LocalDate extractSqliteLocalDate = DbUtilities.extractSqliteLocalDate(resultSet.getObject(i));
							if (extractSqliteLocalDate != null) {
								values.add(extractSqliteLocalDate.toString());
							} else {
								values.add(null);
							}
						} else if (dbVendor == DbVendor.SQLite && "TIMESTAMP".equals(metaData.getColumnTypeName(i))) {
							final LocalDateTime extractSqliteLocalDateTime = DbUtilities.extractSqliteLocalDateTime(resultSet.getObject(i));
							if (extractSqliteLocalDateTime != null) {
								values.add(extractSqliteLocalDateTime.toString());
							} else {
								values.add(null);
							}
						} else if (metaData.getColumnType(i) == Types.DATE) {
							values.add(resultSet.getString(i));
						} else if (metaData.getColumnType(i) == Types.TIMESTAMP) {
							values.add(resultSet.getString(i));
						} else {
							values.add(resultSet.getString(i));
						}
					}
					csvWriter.writeValues(values);
				}

				return csvWriter.getWrittenLines() - 1;
			}
		}
	}
}
