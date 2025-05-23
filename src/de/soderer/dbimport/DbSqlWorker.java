package de.soderer.dbimport;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import de.soderer.utilities.CountingInputStream;
import de.soderer.utilities.DateUtilities;
import de.soderer.utilities.FileUtilities;
import de.soderer.utilities.SqlScriptReader;
import de.soderer.utilities.Tuple;
import de.soderer.utilities.Utilities;
import de.soderer.utilities.csv.CsvDataException;
import de.soderer.utilities.db.DbDefinition;
import de.soderer.utilities.db.DbUtilities;
import de.soderer.utilities.worker.WorkerParentSimple;
import de.soderer.utilities.zip.TarGzUtilities;
import de.soderer.utilities.zip.Zip4jUtilities;
import de.soderer.utilities.zip.ZipUtilities;

public class DbSqlWorker extends DbImportWorker {
	private SqlScriptReader sqlScriptReader = null;
	private Integer itemsAmount = null;

	private final boolean isInlineData;
	private final String importFilePathOrData;
	private final char[] zipPassword;

	private final Charset encoding = StandardCharsets.UTF_8;

	public DbSqlWorker(final WorkerParentSimple parent, final DbDefinition dbDefinition, final String tableName, final boolean isInlineData, final String importFilePathOrData, final char[] zipPassword) throws Exception {
		super(parent, dbDefinition, tableName, null, null);

		this.isInlineData = isInlineData;
		this.importFilePathOrData = importFilePathOrData;
		this.zipPassword = zipPassword;
	}

	@Override
	public String getConfigurationLogString() {
		String dataPart;
		if (isInlineData) {
			dataPart = "Data: " + importFilePathOrData + "\n";
		} else {
			dataPart = "File: " + importFilePathOrData + "\n";
			if (Utilities.endsWithIgnoreCase(importFilePathOrData, ".zip")) {
				dataPart += "Compressed: zip\n";
			} else if (Utilities.endsWithIgnoreCase(importFilePathOrData, ".tar.gz")) {
				dataPart += "Compressed: targz\n";
			} else if (Utilities.endsWithIgnoreCase(importFilePathOrData, ".tgz")) {
				dataPart += "Compressed: tgz\n";
			} else if (Utilities.endsWithIgnoreCase(importFilePathOrData, ".gz")) {
				dataPart += "Compressed: gz\n";
			}
		}
		return
				dataPart
				+ "Format: SQL" + "\n"
				+ "Encoding: " + encoding + "\n";
	}

	public int getItemsAmountToImport() throws Exception {
		if (itemsAmount == null) {
			try (SqlScriptReader scanSqlScriptReader = new SqlScriptReader(getInputStream(), encoding)) {
				int statementsCount = 0;
				while (scanSqlScriptReader.readNextStatement() != null) {
					statementsCount++;
				}
				itemsAmount = statementsCount;
			} catch (final CsvDataException e) {
				throw new DbImportException(e.getMessage(), e);
			} catch (final Exception e) {
				throw e;
			}
		}
		return itemsAmount;
	}

	public void close() {
		Utilities.closeQuietly(sqlScriptReader);
		sqlScriptReader = null;
	}

	@Override
	public Boolean work() throws Exception {
		OutputStream logOutputStream = null;

		if (!isInlineData) {
			if (!new File(importFilePathOrData).exists()) {
				throw new DbImportException("Import file does not exist: " + importFilePathOrData);
			} else if (new File(importFilePathOrData).isDirectory()) {
				throw new DbImportException("Import path is a directory: " + importFilePathOrData);
			}
		}

		Connection connection = null;
		boolean previousAutoCommit = false;
		try {
			connection = DbUtilities.createConnection(dbDefinition, true);
			previousAutoCommit = connection.getAutoCommit();
			connection.setAutoCommit(false);

			validItems = 0;
			invalidItems = new ArrayList<>();

			try {
				if (logFile != null) {
					logOutputStream = new FileOutputStream(logFile);
					logToFile(logOutputStream, getConfigurationLogString());
				}

				logToFile(logOutputStream, "Start: " + DateUtilities.formatDate(DateUtilities.getDateTimeFormatWithSecondsPattern(Locale.getDefault()), getStartTime()));

				signalUnlimitedProgress();

				itemsToDo = getItemsAmountToImport();
				logToFile(logOutputStream, "Statements to execute: " + itemsToDo);
				signalProgress(true);

				try (Statement statement = connection.createStatement()) {
					openReader();

					// Execute statements
					String nextStatement;
					while ((nextStatement = sqlScriptReader.readNextStatement()) != null) {
						try {
							statement.execute(nextStatement);
							validItems++;
						} catch (final Exception e) {
							if (commitOnFullSuccessOnly) {
								connection.rollback();
								throw new DbImportException("Erroneous statement number " + (itemsDone + 1) + " at character index " + sqlScriptReader.getReadCharacters() + ": " + e.getMessage());
							}
							invalidItems.add((int) itemsDone);
							if (logErroneousData) {
								if (erroneousDataFile == null) {
									erroneousDataFile = new File(DateUtilities.formatDate(DateUtilities.DD_MM_YYYY_HH_MM_SS_ForFileName, getStartTime()) + ".errors");
								} else {
									FileUtilities.append(erroneousDataFile, "\n", StandardCharsets.UTF_8);
								}
								FileUtilities.append(erroneousDataFile, nextStatement, StandardCharsets.UTF_8);
							}
						}
						itemsDone++;
					}
					connection.commit();
				}

				setEndTime(LocalDateTime.now());

				importedDataAmount += isInlineData ? importFilePathOrData.length() : new File(importFilePathOrData).length();

				logToFile(logOutputStream, getResultStatistics());

				final long elapsedTimeInSeconds = Duration.between(getStartTime(), getEndTime()).toSeconds();
				if (elapsedTimeInSeconds > 0) {
					final long itemsPerSecond = validItems / elapsedTimeInSeconds;
					logToFile(logOutputStream, "Import speed: " + itemsPerSecond + " items/second");
				} else {
					logToFile(logOutputStream, "Import speed: immediately");
				}
				logToFile(logOutputStream, "End: " + DateUtilities.formatDate(DateUtilities.getDateTimeFormatWithSecondsPattern(Locale.getDefault()), getEndTime()));
				logToFile(logOutputStream, "Time elapsed: " + DateUtilities.getHumanReadableTimespanEnglish(Duration.between(getStartTime(), getEndTime()), true));
			} catch (final SQLException sqle) {
				throw new DbImportException("SQL error: " + sqle.getMessage());
			} catch (final Exception e) {
				try {
					logToFile(logOutputStream, "Error: " + e.getMessage());
				} catch (final Exception e1) {
					e1.printStackTrace();
				}
				throw e;
			} finally {
				close();
				Utilities.closeQuietly(logOutputStream);
			}

			return !cancel;
		} catch (final Exception e) {
			throw e;
		} finally {
			if (connection != null) {
				if (!connection.isClosed()) {
					connection.rollback();
					connection.setAutoCommit(previousAutoCommit);
					connection.close();
				}
			}
		}
	}

	private InputStream getInputStream() throws Exception {
		if (!isInlineData) {
			if (!new File(importFilePathOrData).exists()) {
				throw new DbImportException("Import file does not exist: " + importFilePathOrData);
			} else if (new File(importFilePathOrData).isDirectory()) {
				throw new DbImportException("Import path is a directory: " + importFilePathOrData);
			} else if (new File(importFilePathOrData).length() == 0) {
				throw new DbImportException("Import file is empty: " + importFilePathOrData);
			}

			InputStream inputStream = null;
			try {
				if (Utilities.endsWithIgnoreCase(importFilePathOrData, ".zip") || ZipUtilities.isZipArchiveFile(new File(importFilePathOrData))) {
					if (zipPassword != null) {
						inputStream = Zip4jUtilities.openPasswordSecuredZipFile(importFilePathOrData, zipPassword);
					} else {
						final List<String> filepathsFromZipArchiveFile = ZipUtilities.getZipFileEntries(new File(importFilePathOrData));
						if (filepathsFromZipArchiveFile.size() == 0) {
							throw new DbImportException("Zipped import file is empty: " + importFilePathOrData);
						} else if (filepathsFromZipArchiveFile.size() > 1) {
							throw new DbImportException("Zipped import file contains more than one file: " + importFilePathOrData);
						}

						inputStream = new ZipInputStream(new FileInputStream(new File(importFilePathOrData)));
						final ZipEntry zipEntry = ((ZipInputStream) inputStream).getNextEntry();
						if (zipEntry == null) {
							throw new DbImportException("Zipped import file is empty: " + importFilePathOrData);
						} else if (zipEntry.getSize() == 0) {
							throw new DbImportException("Zipped import file is empty: " + importFilePathOrData + ": " + zipEntry.getName());
						}
					}
				} else if (Utilities.endsWithIgnoreCase(importFilePathOrData, ".tar.gz")) {
					if (TarGzUtilities.getFilesCount(new File(importFilePathOrData)) != 1) {
						throw new DbImportException("Compressed import file does not contain a single compressed file: " + importFilePathOrData);
					} else {
						inputStream = new CountingInputStream(TarGzUtilities.openCompressedFile(new File(importFilePathOrData)));
					}
				} else if (Utilities.endsWithIgnoreCase(importFilePathOrData, ".tgz")) {
					if (TarGzUtilities.getFilesCount(new File(importFilePathOrData)) != 1) {
						throw new DbImportException("Compressed import file does not contain a single compressed file: " + importFilePathOrData);
					} else {
						inputStream = new CountingInputStream(TarGzUtilities.openCompressedFile(new File(importFilePathOrData)));
					}
				} else if (Utilities.endsWithIgnoreCase(importFilePathOrData, ".gz")) {
					inputStream = new CountingInputStream(new GZIPInputStream(new FileInputStream(importFilePathOrData)));
				} else {
					inputStream = new FileInputStream(new File(importFilePathOrData));
				}
				return inputStream;
			} catch (final Exception e) {
				if (inputStream != null) {
					try {
						inputStream.close();
					} catch (@SuppressWarnings("unused") final IOException e1) {
						// do nothing
					}
				}
				throw e;
			}
		} else {
			return new ByteArrayInputStream(importFilePathOrData.getBytes(StandardCharsets.UTF_8));
		}
	}

	private void openReader() throws Exception {
		final InputStream inputStream = null;
		try {
			sqlScriptReader = new SqlScriptReader(getInputStream(), encoding);
		} catch (final Exception e) {
			Utilities.closeQuietly(sqlScriptReader);
			Utilities.closeQuietly(inputStream);
			throw e;
		}
	}

	@Override
	public Map<String, Tuple<String, String>> getMapping() throws Exception {
		return mapping;
	}
}
