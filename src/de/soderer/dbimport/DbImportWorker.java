package de.soderer.dbimport;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.DateTimeException;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.MonthDay;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

import de.soderer.dbimport.DbImportDefinition.DuplicateMode;
import de.soderer.dbimport.DbImportDefinition.ImportMode;
import de.soderer.dbimport.dataprovider.DataProvider;
import de.soderer.utilities.CountingInputStream;
import de.soderer.utilities.DateUtilities;
import de.soderer.utilities.InputStreamWithOtherItemsToClose;
import de.soderer.utilities.IoUtilities;
import de.soderer.utilities.LangResources;
import de.soderer.utilities.TextUtilities;
import de.soderer.utilities.Tuple;
import de.soderer.utilities.Utilities;
import de.soderer.utilities.collection.CaseInsensitiveMap;
import de.soderer.utilities.collection.CaseInsensitiveSet;
import de.soderer.utilities.db.DbColumnType;
import de.soderer.utilities.db.DbDefinition;
import de.soderer.utilities.db.DbNotExistsException;
import de.soderer.utilities.db.DbUtilities;
import de.soderer.utilities.db.DbUtilities.DbVendor;
import de.soderer.utilities.db.SimpleDataType;
import de.soderer.utilities.json.JsonArray;
import de.soderer.utilities.json.JsonNode;
import de.soderer.utilities.json.JsonObject;
import de.soderer.utilities.json.JsonReader;
import de.soderer.utilities.json.utilities.NetworkUtilities;
import de.soderer.utilities.worker.WorkerParentSimple;
import de.soderer.utilities.worker.WorkerSimple;
import de.soderer.utilities.zip.TarGzUtilities;
import de.soderer.utilities.zip.Zip4jUtilities;
import de.soderer.utilities.zip.ZipUtilities;

public class DbImportWorker extends WorkerSimple<Boolean> {
	// Mandatory parameters
	protected DbDefinition dbDefinition = null;
	protected ImportMode importMode = ImportMode.INSERT;
	protected DuplicateMode duplicateMode = DuplicateMode.UPDATE_ALL_JOIN;
	protected List<String> keyColumns = null;
	protected List<String> keyColumnsWithFunctions = null;
	protected String tableName;
	protected boolean createTableIfNotExists = false;
	protected String structureFilePath;
	protected boolean commitOnFullSuccessOnly = true;
	protected boolean createNewIndexIfNeeded = true;
	protected boolean deactivateForeignKeyConstraints = false;
	protected boolean deactivateTriggers = false;
	protected String newIndexName = null;

	protected ZoneId databaseZoneId = ZoneId.systemDefault();
	protected ZoneId importDataZoneId = ZoneId.systemDefault();

	protected List<String> dbTableColumnsListToInsert = null;
	protected Map<String, Tuple<String, String>> mapping = null;

	// Default optional parameters
	protected File logFile = null;
	protected Charset textFileEncoding = StandardCharsets.UTF_8;
	protected boolean updateWithNullValues = true;

	protected long dataItemsDone = 0;
	protected long validItems = 0;
	protected long duplicatesItems = 0;
	protected List<Integer> invalidItems = new ArrayList<>();
	protected List<String> invalidItemsReasons = new ArrayList<>();
	protected long importedDataAmount = 0;
	protected long singleImportModeBlocks = 0;
	protected long deletedItems = 0;
	protected long insertedItems = 0;
	protected long updatedItems = 0;
	protected long countItems = 0;
	protected long deletedDuplicatesInDB = 0;

	protected boolean analyseDataOnly = false;
	protected List<String> availableDataPropertyNames = null;

	protected String additionalInsertValues = null;
	protected String additionalUpdateValues = null;

	protected boolean logErroneousData = false;
	protected File erroneousDataFile = null;

	protected String dateFormatPattern = null;
	protected String dateTimeFormatPattern = null;

	private DateTimeFormatter dateFormatterCache = null;
	private DateTimeFormatter dateTimeFormatterCache = null;

	private int batchBlockSize = 1000;
	private boolean preventBatchFallbackToSingleLineOnErrors = false;

	protected DataProvider dataProvider = null;

	public DbImportWorker(final WorkerParentSimple parent, final DbDefinition dbDefinition, final String tableName, final String dateFormatPattern, final String dateTimeFormatPattern) {
		super(parent);

		this.dbDefinition = dbDefinition;
		this.tableName = tableName;
		this.dateFormatPattern = dateFormatPattern;
		this.dateTimeFormatPattern = dateTimeFormatPattern;
	}

	public void setAnalyseDataOnly(final boolean analyseDataOnly) {
		this.analyseDataOnly = analyseDataOnly;
	}

	public void setLogFile(final File logFile) {
		this.logFile = logFile;
	}

	public void setTextFileEncoding(final Charset textFileEncoding) {
		this.textFileEncoding = textFileEncoding;
	}

	public void setDataProvider(final DataProvider dataProvider) {
		this.dataProvider = dataProvider;
	}

	public void setDatabaseTimeZone(final String databaseTimeZone) {
		databaseZoneId = ZoneId.of(databaseTimeZone);
	}

	public void setImportDataTimeZone(final String importDataTimeZone) {
		importDataZoneId = ZoneId.of(importDataTimeZone);
	}

	public void setBatchBlockSize(final int batchBlockSize) {
		this.batchBlockSize = batchBlockSize;
	}

	public void setPreventBatchFallbackToSingleLineOnErrors(final boolean preventBatchFallbackToSingleLineOnErrors) {
		this.preventBatchFallbackToSingleLineOnErrors = preventBatchFallbackToSingleLineOnErrors;
	}

	public void setMapping(final String mappingString) throws IOException, Exception {
		if (Utilities.isNotBlank(mappingString)) {
			mapping = DbImportMappingDialog.parseMappingString(mappingString);
			dbTableColumnsListToInsert = new ArrayList<>();
			for (final String dbColumn : mapping.keySet()) {
				dbTableColumnsListToInsert.add(DbUtilities.unescapeVendorReservedNames(dbDefinition.getDbVendor(), dbColumn));
			}
		} else {
			mapping = null;
		}
	}

	public Map<String, Tuple<String, String>> getMapping() throws Exception {
		if (mapping == null) {
			mapping = new HashMap<>();
			for (final String propertyName : dataProvider.getAvailableDataPropertyNames()) {
				mapping.put(propertyName.toLowerCase(), new Tuple<>(propertyName, ""));
			}
			dbTableColumnsListToInsert = new ArrayList<>();
			for (final String dbColumn : mapping.keySet()) {
				dbTableColumnsListToInsert.add(DbUtilities.unescapeVendorReservedNames(dbDefinition.getDbVendor(), dbColumn));
			}
		}
		return mapping;
	}

	protected void checkMapping(final Map<String, DbColumnType> dbColumns) throws Exception, DbImportException {
		final List<String> dataPropertyNames = dataProvider.getAvailableDataPropertyNames();
		if (mapping != null) {
			for (String dbColumnToInsert : dbTableColumnsListToInsert) {
				dbColumnToInsert = DbUtilities.unescapeVendorReservedNames(dbDefinition.getDbVendor(), dbColumnToInsert);
				if (!dbColumns.containsKey(dbColumnToInsert)) {
					throw new DbImportException("Database table does not contain mapped column: " + dbColumnToInsert);
				}
			}

			final Set<String> mappedDbColumns = new CaseInsensitiveSet();
			for (final Entry<String, Tuple<String, String>> mappingEntry : mapping.entrySet()) {
				if (Utilities.isNotBlank(mappingEntry.getKey()) && !mappedDbColumns.add(mappingEntry.getKey())) {
					throw new DbImportException("Mapping contains database column multiple times: " + mappingEntry.getKey());
				} else if (!dataPropertyNames.contains(mappingEntry.getValue().getFirst())) {
					throw new DbImportException("Data does not contain mapped property: " + mappingEntry.getValue().getFirst());
				}
			}
		} else {
			// Create default mapping
			mapping = new HashMap<>();
			dbTableColumnsListToInsert = new ArrayList<>();
			for (final String dbColumn : dbColumns.keySet()) {
				for (final String dataPropertyName : dataPropertyNames) {
					if (dbColumn.equalsIgnoreCase(dataPropertyName)) {
						mapping.put(dbColumn, new Tuple<>(dataPropertyName, ""));
						dbTableColumnsListToInsert.add(dbColumn);
						break;
					}
				}
			}
		}

		if (keyColumns != null && keyColumns.size() > 0) {
			for (final String keyColumn : keyColumns) {
				boolean isIncluded = false;
				for (final Entry<String, Tuple<String, String>> entry : mapping.entrySet()) {
					if (DbUtilities.unescapeVendorReservedNames(dbDefinition.getDbVendor(), keyColumn).equals(DbUtilities.unescapeVendorReservedNames(dbDefinition.getDbVendor(), entry.getKey()))) {
						isIncluded = true;
						break;
					}
				}
				if (!isIncluded) {
					throw new DbImportException("Mapping doesn't include the defined keycolumn: " + keyColumn);
				}
			}
		}
	}

	public void setImportMode(final ImportMode importMode) {
		this.importMode = importMode;
	}

	public void setDuplicateMode(final DuplicateMode duplicateMode) throws Exception {
		this.duplicateMode = duplicateMode;
	}

	public void setKeycolumns(final List<String> keyColumnList) {
		if (Utilities.isNotEmpty(keyColumnList)) {
			Map<String, String> columnFunctions = new CaseInsensitiveMap<>();

			// Remove the optional functions from keycolumns
			for (String keyColumn : keyColumnList) {
				keyColumn = keyColumn.trim();
				if (Utilities.isNotEmpty(keyColumn)) {
					String function = null;
					if (keyColumn.contains("(") && keyColumn.endsWith(")")) {
						function = keyColumn.substring(0, keyColumn.indexOf("(")).trim().toUpperCase();
						keyColumn = keyColumn.substring(keyColumn.indexOf("(") + 1, keyColumn.length() - 1).trim();
					}

					columnFunctions.put(keyColumn, function);
				}
			}

			if (columnFunctions.size() > 0) {
				columnFunctions = Utilities.sortMap(columnFunctions);
				keyColumns = new ArrayList<>();
				keyColumnsWithFunctions = new ArrayList<>();
				for (final String keyColumn : columnFunctions.keySet()) {
					keyColumns.add(keyColumn);
					if (columnFunctions.get(keyColumn) != null) {
						keyColumnsWithFunctions.add(columnFunctions.get(keyColumn).toUpperCase() + "(" + keyColumn + ")");
					} else {
						keyColumnsWithFunctions.add(keyColumn);
					}
				}
			}
		}
	}

	public void setCompleteCommit(final boolean commitOnFullSuccessOnly) throws Exception {
		this.commitOnFullSuccessOnly = commitOnFullSuccessOnly;
	}

	public void setCreateNewIndexIfNeeded(final boolean createNewIndexIfNeeded) {
		this.createNewIndexIfNeeded = createNewIndexIfNeeded;
	}

	public void setDeactivateForeignKeyConstraints(final boolean deactivateForeignKeyConstraints) {
		this.deactivateForeignKeyConstraints = deactivateForeignKeyConstraints;
	}

	public void setDeactivateTriggers(final boolean deactivateTriggers) {
		this.deactivateTriggers = deactivateTriggers;
	}

	public void setAdditionalInsertValues(final String additionalInsertValues) {
		this.additionalInsertValues = additionalInsertValues;
	}

	public void setAdditionalUpdateValues(final String additionalUpdateValues) {
		this.additionalUpdateValues = additionalUpdateValues;
	}

	public void setUpdateNullData(final boolean updateWithNullValues) throws Exception {
		this.updateWithNullValues = updateWithNullValues;
	}

	public void setCreateTableIfNotExists(final boolean createTableIfNotExists) {
		this.createTableIfNotExists = createTableIfNotExists;
	}

	public void setStructureFilePath(final String structureFilePath) {
		this.structureFilePath = structureFilePath;
	}

	public void setLogErroneousData(final boolean logErroneousData) {
		this.logErroneousData = logErroneousData;
	}

	@Override
	public Boolean work() throws Exception {
		dataItemsDone = 0;
		validItems = 0;
		duplicatesItems = 0;
		invalidItems = new ArrayList<>();
		invalidItemsReasons = new ArrayList<>();
		importedDataAmount = 0;
		singleImportModeBlocks = 0;
		deletedItems = 0;
		insertedItems = 0;
		updatedItems = 0;
		countItems = 0;
		deletedDuplicatesInDB = 0;

		signalUnlimitedProgress();

		if (analyseDataOnly) {
			parent.changeTitle(LangResources.get("analyseData"));
			availableDataPropertyNames = dataProvider.getAvailableDataPropertyNames();
		} else {
			OutputStream logOutputStream = null;
			Connection connection = null;
			boolean previousAutoCommit = false;
			String tempTableName = null;
			boolean constraintsWereDeactivated = false;
			boolean triggersWereDeactivated = false;
			try {
				if (dbDefinition.getDbVendor() == DbVendor.Derby || (dbDefinition.getDbVendor() == DbVendor.HSQL && Utilities.isBlank(dbDefinition.getHostnameAndPort())) || dbDefinition.getDbVendor() == DbVendor.SQLite) {
					try {
						connection = DbUtilities.createConnection(dbDefinition, true);
					} catch (@SuppressWarnings("unused") final DbNotExistsException e) {
						connection = DbUtilities.createNewDatabase(dbDefinition.getDbVendor(), dbDefinition.getDbName());
					}
				} else {
					connection = DbUtilities.createConnection(dbDefinition, false);
				}

				previousAutoCommit = connection.getAutoCommit();
				connection.setAutoCommit(false);

				if (logFile != null) {
					logOutputStream = new FileOutputStream(logFile);
					logToFile(logOutputStream, getConfigurationLogString());
				}

				logToFile(logOutputStream, "Start: " + DateUtilities.formatDate(DateUtilities.getDateTimeFormatWithSecondsPattern(Locale.getDefault()), getStartTime()));

				if (deactivateForeignKeyConstraints) {
					parent.changeTitle(LangResources.get("deactivateForeignKeyConstraints"));
					constraintsWereDeactivated = true;
					DbUtilities.setForeignKeyConstraintStatus(dbDefinition.getDbVendor(), connection, false);
					connection.commit();
				}
				if (deactivateTriggers) {
					parent.changeTitle(LangResources.get("deactivateTriggers"));
					triggersWereDeactivated = true;
					DbUtilities.setTriggerStatus(dbDefinition.getDbVendor(), connection, false);
					connection.commit();
				}

				final boolean tableWasCreated = createTableIfNeeded(connection, tableName, keyColumns);
				if (tableWasCreated) {
					try {
						logToFile(logOutputStream, "Created table '" + tableName + "'");
					} catch (final Exception e1) {
						e1.printStackTrace();
					}
				}

				final Map<String, DbColumnType> dbColumns = DbUtilities.getColumnDataTypes(connection, tableName);
				checkMapping(dbColumns);

				if (dbTableColumnsListToInsert.size() == 0) {
					throw new DbImportException("Invalid empty mapping");
				}

				if (!DbUtilities.checkTableAndColumnsExist(connection, tableName, keyColumns == null ? null : keyColumns.toArray(new String[0]))) {
					throw new DbImportException("Some keycolumn is not included in table");
				}

				if (importMode == ImportMode.CLEARINSERT) {
					parent.changeTitle(LangResources.get("clearTable"));
					deletedItems = DbUtilities.clearTable(connection, tableName);
					connection.commit();
				}

				parent.changeTitle(LangResources.get("readData"));
				itemsToDo = dataProvider.getItemsAmountToImport();
				itemsUnitSign = dataProvider.getItemsUnitSign();
				if (itemsUnitSign == null) {
					logToFile(logOutputStream, "Items to import: " + itemsToDo);
				} else {
					logToFile(logOutputStream, "Data to import: " + itemsToDo + " " + itemsUnitSign);
				}

				if ((importMode == ImportMode.CLEARINSERT || importMode == ImportMode.INSERT) && Utilities.isEmpty(keyColumns)) {
					// Just import in the destination table
					insertIntoTable(connection, tableName, dbColumns, null, additionalInsertValues, getMapping());
					insertedItems = validItems;
				} else {
					// Make table entries unique
					if (duplicateMode == DuplicateMode.MAKE_UNIQUE_DROP) {
						deletedDuplicatesInDB = DbUtilities.dropDuplicates(connection, tableName, keyColumnsWithFunctions);
					} else if (duplicateMode == DuplicateMode.MAKE_UNIQUE_JOIN) {
						deletedDuplicatesInDB = DbUtilities.joinDuplicates(connection, tableName, keyColumnsWithFunctions, updateWithNullValues);
					}

					// Create temp table
					String dateSuffix = DateUtilities.formatDate(DateUtilities.YYYYMMDDHHMMSS, getStartTime());
					tempTableName = "tmp_" + dateSuffix;
					int i = 0;
					while (DbUtilities.checkTableExist(connection, tempTableName) && i < 10) {
						Thread.sleep(1000);
						i++;
						dateSuffix = DateUtilities.formatDate(DateUtilities.YYYYMMDDHHMMSS, LocalDateTime.now());
						tempTableName = "tmp_" + dateSuffix;
					}
					if (i >= 10) {
						tempTableName = null;
						throw new Exception("Cannot create temp table");
					}

					final String tempItemIndexColumn;
					DbUtilities.copyTableStructure(connection, tableName, dbTableColumnsListToInsert, keyColumns, tempTableName);
					if (Utilities.isNotEmpty(keyColumns)) {
						final Boolean hasIndexedKeyColumns = DbUtilities.checkForIndex(connection, tableName, keyColumns);
						if ((hasIndexedKeyColumns == null || !hasIndexedKeyColumns) && createNewIndexIfNeeded) {
							try {
								newIndexName = DbUtilities.createIndex(connection, tableName, keyColumns);
							} catch (final Exception e) {
								System.err.println("Cannot create index for table '" + tableName + "' on columns '" + Utilities.join(keyColumns, ", ") + "': " + e.getMessage());
							}
						}
					}
					tempItemIndexColumn = DbUtilities.addIndexedIntegerColumn(connection, tempTableName, "import_item");
					connection.commit();

					// Insert in temp table
					insertIntoTable(connection, tempTableName, dbColumns, tempItemIndexColumn, null, getMapping());

					DbUtilities.gatherTableStats(connection, tempTableName);

					itemsDone = 0;
					parent.changeTitle(LangResources.get("dropDuplicates"));

					// Handle duplicates in import data
					if (duplicateMode == DuplicateMode.NO_CHECK) {
						// Do not check for duplicates
					} else if (duplicateMode == DuplicateMode.CKECK_SOURCE_ONLY_DROP || duplicateMode == DuplicateMode.UPDATE_FIRST_DROP || duplicateMode == DuplicateMode.UPDATE_ALL_DROP || duplicateMode == DuplicateMode.MAKE_UNIQUE_DROP) {
						duplicatesItems = DbUtilities.dropDuplicates(connection, tempTableName, keyColumns);
					} else if (duplicateMode == DuplicateMode.CKECK_SOURCE_ONLY_JOIN || duplicateMode == DuplicateMode.UPDATE_FIRST_JOIN || duplicateMode == DuplicateMode.UPDATE_ALL_JOIN || duplicateMode == DuplicateMode.MAKE_UNIQUE_JOIN) {
						duplicatesItems = DbUtilities.joinDuplicates(connection, tempTableName, keyColumns, updateWithNullValues);
					} else {
						throw new DbImportException("Invalid duplicate mode");
					}

					if (cancel) {
						return false;
					}

					if (importMode == ImportMode.CLEARINSERT) {
						parent.changeTitle(LangResources.get("insertData"));

						insertedItems = DbUtilities.insertNotExistingItems(connection, tempTableName, tableName, dbTableColumnsListToInsert, keyColumnsWithFunctions, additionalInsertValues);
					} else if (importMode == ImportMode.INSERT) {
						if (duplicateMode == DuplicateMode.NO_CHECK || duplicateMode == DuplicateMode.CKECK_SOURCE_ONLY_DROP || duplicateMode == DuplicateMode.CKECK_SOURCE_ONLY_JOIN) {
							parent.changeTitle(LangResources.get("insertData"));

							// Insert all entries
							insertedItems = DbUtilities.insertAllItems(connection, tempTableName, tableName, dbTableColumnsListToInsert, additionalInsertValues);
						} else {
							parent.changeTitle(LangResources.get("dropDuplicates"));

							// Insert only not existing entries
							duplicatesItems += DbUtilities.dropDuplicatesCrossTable(connection, tableName, tempTableName, keyColumnsWithFunctions);

							if (cancel) {
								return false;
							}

							parent.changeTitle(LangResources.get("insertData"));

							insertedItems = DbUtilities.insertNotExistingItems(connection, tempTableName, tableName, dbTableColumnsListToInsert, keyColumnsWithFunctions, additionalInsertValues);
						}
					} else if (importMode == ImportMode.UPDATE) {
						if (duplicateMode == DuplicateMode.NO_CHECK || duplicateMode == DuplicateMode.CKECK_SOURCE_ONLY_DROP || duplicateMode == DuplicateMode.CKECK_SOURCE_ONLY_JOIN) {
							// Do nothing
						} else if (DbUtilities.detectDuplicates(connection, tableName, keyColumnsWithFunctions) > 0 && (duplicateMode == DuplicateMode.UPDATE_FIRST_DROP || duplicateMode == DuplicateMode.UPDATE_FIRST_JOIN)) {
							if (cancel) {
								return false;
							}

							parent.changeTitle(LangResources.get("updateData"));

							// Update only the first occurrence
							updatedItems = DbUtilities.updateFirstExistingItems(connection, tempTableName, tableName, dbTableColumnsListToInsert, keyColumns, tempItemIndexColumn, updateWithNullValues, additionalUpdateValues);
						} else {
							if (cancel) {
								return false;
							}

							parent.changeTitle(LangResources.get("updateData"));

							// Update destination table
							updatedItems = DbUtilities.updateAllExistingItems(connection, tempTableName, tableName, dbTableColumnsListToInsert, keyColumns, tempItemIndexColumn, updateWithNullValues, additionalUpdateValues);
						}
					} else if (importMode == ImportMode.UPSERT) {
						if (duplicateMode == DuplicateMode.NO_CHECK || duplicateMode == DuplicateMode.CKECK_SOURCE_ONLY_DROP || duplicateMode == DuplicateMode.CKECK_SOURCE_ONLY_JOIN) {
							parent.changeTitle(LangResources.get("insertData"));

							// Insert all entries
							insertedItems = DbUtilities.insertAllItems(connection, tempTableName, tableName, dbTableColumnsListToInsert, additionalInsertValues);
						} else if (DbUtilities.detectDuplicates(connection, tableName, keyColumnsWithFunctions) > 0 && (duplicateMode == DuplicateMode.UPDATE_FIRST_DROP || duplicateMode == DuplicateMode.UPDATE_FIRST_JOIN)) {
							if (cancel) {
								return false;
							}

							parent.changeTitle(LangResources.get("updateData"));

							// Update only the first occurrence
							updatedItems = DbUtilities.updateFirstExistingItems(connection, tempTableName, tableName, dbTableColumnsListToInsert, keyColumns, tempItemIndexColumn, updateWithNullValues, additionalUpdateValues);

							if (cancel) {
								return false;
							}
							parent.changeTitle(LangResources.get("insertData"));

							// Insert into destination table
							insertedItems = DbUtilities.insertNotExistingItems(connection, tempTableName, tableName, dbTableColumnsListToInsert, keyColumnsWithFunctions, additionalInsertValues);
						} else {
							if (cancel) {
								return false;
							}

							parent.changeTitle(LangResources.get("updateData"));

							// Update destination table
							updatedItems = DbUtilities.updateAllExistingItems(connection, tempTableName, tableName, dbTableColumnsListToInsert, keyColumns, tempItemIndexColumn, updateWithNullValues, additionalUpdateValues);

							if (cancel) {
								return false;
							}

							parent.changeTitle(LangResources.get("insertData"));

							// Insert into destination table
							insertedItems = DbUtilities.insertNotExistingItems(connection, tempTableName, tableName, dbTableColumnsListToInsert, keyColumnsWithFunctions, additionalInsertValues);
						}
					} else {
						throw new DbImportException("Invalid import mode");
					}
				}

				connection.commit();

				itemsDone = itemsToDo;
				signalProgress(true);

				countItems = DbUtilities.getTableEntriesCount(connection, tableName);

				if (logErroneousData & invalidItems.size() > 0) {
					erroneousDataFile = dataProvider.filterDataItems(invalidItems, DateUtilities.formatDate(DateUtilities.DD_MM_YYYY_HH_MM_SS_ForFileName, getStartTime()) + ".errors");
				}

				importedDataAmount += dataProvider.getImportDataAmount();
			} catch (final SQLException sqle) {
				try {
					logToFile(logOutputStream, "SQL Error: " + sqle.getMessage());
					if (logOutputStream != null) {
						try (PrintStream printStream = new PrintStream(logOutputStream)) {
							sqle.printStackTrace(printStream);
						}
					}
				} catch (final Exception e1) {
					e1.printStackTrace();
				}
				throw new DbImportException("SQL error: " + sqle.getMessage());
			} catch (final Exception e) {
				try {
					logToFile(logOutputStream, "Error: " + e.getMessage());
					if (logOutputStream != null) {
						try (PrintStream printStream = new PrintStream(logOutputStream)) {
							e.printStackTrace(printStream);
						}
					}
				} catch (final Exception e1) {
					e1.printStackTrace();
				}
				throw e;
			} finally {
				if (deactivateForeignKeyConstraints && constraintsWereDeactivated && connection != null) {
					try {
						parent.changeTitle(LangResources.get("reactivateForeignKeyConstraints"));
						DbUtilities.setForeignKeyConstraintStatus(dbDefinition.getDbVendor(), connection, true);
						connection.commit();
					} catch (final Exception e) {
						System.err.println("Cannot reactivate foreign key constraints");
						e.printStackTrace();
					}
				}
				if (deactivateTriggers && triggersWereDeactivated && connection != null) {
					try {
						parent.changeTitle(LangResources.get("reactivateTriggers"));
						DbUtilities.setTriggerStatus(dbDefinition.getDbVendor(), connection, true);
						connection.commit();
					} catch (final Exception e) {
						System.err.println("Cannot reactivate triggers");
						e.printStackTrace();
					}
				}

				dataProvider.close();

				setEndTime(LocalDateTime.now());

				// Drop temp table
				DbUtilities.dropTableIfExists(connection, tempTableName);

				if (connection != null) {
					if (!connection.isClosed()) {
						connection.rollback();
						connection.setAutoCommit(previousAutoCommit);
						connection.close();
					}
					connection = null;
					if (dbDefinition.getDbVendor() == DbVendor.Derby) {
						DbUtilities.shutDownDerbyDb(dbDefinition.getDbName());
					}
				}

				logToFile(logOutputStream, getResultStatistics());

				if (getStartTime() != null && getEndTime() != null) {
					final long elapsedTimeInSeconds = Duration.between(getStartTime(), getEndTime()).toSeconds();
					if (elapsedTimeInSeconds > 0) {
						final long itemsPerSecond = validItems / elapsedTimeInSeconds;
						logToFile(logOutputStream, "Import speed: " + itemsPerSecond + " items/second");
					} else {
						logToFile(logOutputStream, "Import speed: immediately");
					}
					logToFile(logOutputStream, "End: " + DateUtilities.formatDate(DateUtilities.getDateTimeFormatWithSecondsPattern(Locale.getDefault()), getEndTime()));
					logToFile(logOutputStream, "Time elapsed: " + DateUtilities.getHumanReadableTimespanEnglish(Duration.between(getStartTime(), getEndTime()), true));
				}

				if (cancel) {
					logToFile(logOutputStream, "Import was canceled");
				}

				Utilities.closeQuietly(logOutputStream);
			}
		}

		return !cancel;
	}

	public String getConfigurationLogString() throws Exception {
		return dataProvider.getConfigurationLogString()
				+ "CommitOnFullSuccessOnly: " + commitOnFullSuccessOnly + "\n"
				+ "CreateNewIndexIfNeeded: " + createNewIndexIfNeeded + "\n"
				+ "Table name: " + tableName + "\n"
				+ "Import mode: " + importMode + "\n"
				+ "Duplicate mode: " + duplicateMode + "\n"
				+ "Key columns: " + Utilities.join(keyColumns, ", ") + "\n"
				+ (createTableIfNotExists ? "Create table if not exists: " + createTableIfNotExists + "\n" : "")
				+ (structureFilePath != null ? "Structure file: " + structureFilePath + "\n" : "")
				+ "Mapping: \n" + TextUtilities.addLeadingTab(convertMappingToString(getMapping())) + "\n"
				+ (Utilities.isNotBlank(additionalInsertValues) ? "Additional insert values: " + additionalInsertValues + "\n" : "")
				+ (Utilities.isNotBlank(additionalUpdateValues) ? "Additional update values: " + additionalUpdateValues + "\n" : "")
				+ (Utilities.isNotBlank(dateFormatPattern) ? "DateFormatPattern: " + dateFormatPattern + "\n" : "")
				+ (Utilities.isNotBlank(dateTimeFormatPattern) ? "DateTimeFormatPattern: " + dateTimeFormatPattern + "\n" : "")
				+ (databaseZoneId != null && !databaseZoneId.equals(importDataZoneId) ? "DatabaseZoneId: " + databaseZoneId + "\nImportDataZoneId: " + importDataZoneId + "\n" : "")
				+ "Update with null values: " + updateWithNullValues + "\n"
				+ "BatchBlockSize: " + batchBlockSize + "\n"
				+ "PreventBatchFallbackToSingleLineOnErrors: " + preventBatchFallbackToSingleLineOnErrors + "\n";
	}

	protected boolean createTableIfNeeded(final Connection connection, final String tableNameToUse, final List<String> keyColumnsToUse) throws Exception, DbImportException, SQLException {
		if (!DbUtilities.checkTableExist(connection, tableNameToUse)) {
			if (createTableIfNotExists) {
				if (structureFilePath != null && Utilities.isNotBlank(structureFilePath)) {
					try {
						final boolean tableWasCreated = createTableFromStructureFile(connection, tableNameToUse, structureFilePath);
						return tableWasCreated;
					} catch (final Exception e) {
						throw new DbImportException("Cannot create new table '" + tableNameToUse + "' by structure file: " + e.getMessage(), e);
					}
				} else {
					final Map<String, DbColumnType> importDataTypes = dataProvider.scanDataPropertyTypes(mapping);
					final Map<String, DbColumnType> dbDataTypes = new HashMap<>();
					for (final Entry<String, DbColumnType> importDataType : importDataTypes.entrySet()) {
						if (getMapping() != null) {
							for (final Entry<String,Tuple<String,String>> mappingEntry : getMapping().entrySet()) {
								if (mappingEntry.getValue().getFirst().equals(importDataType.getKey())) {
									dbDataTypes.put(mappingEntry.getKey(), importDataTypes.get(importDataType.getKey()));
									break;
								}
							}
						} else {
							if (!Pattern.matches("[_a-zA-Z0-9]{1,30}", importDataType.getKey())) {
								throw new DbImportException("Cannot create table without mapping for data propertyname: " + importDataType.getKey());
							}
							dbDataTypes.put(importDataType.getKey(), importDataTypes.get(importDataType.getKey()));
						}
					}
					if (dbDefinition.getDbVendor() == DbVendor.PostgreSQL) {
						// Close a maybe open transaction to allow DDL-statement
						connection.rollback();
					}
					try {
						DbUtilities.createTable(connection, tableNameToUse, dbDataTypes, keyColumnsToUse);
					} catch (final Exception e) {
						throw new DbImportException("Cannot create new table '" + tableNameToUse + "': " + e.getMessage(), e);
					}
					if (dbDefinition.getDbVendor() == DbVendor.PostgreSQL) {
						// Commit DDL-statement
						connection.commit();
					}
					return true;
				}
			} else {
				return false;
			}
		} else {
			return false;
		}
	}

	private boolean createTableFromStructureFile(final Connection connection, final String tableNameToUse, final String structureFilePathToLookIn) throws Exception {
		try (FileInputStream jsonStructureDataInputStream = new FileInputStream(structureFilePathToLookIn);
				JsonReader jsonReader = new JsonReader(jsonStructureDataInputStream)) {
			parent.changeTitle(LangResources.get("creatingMissingTablesAndColumns"));

			final JsonNode dbStructureJsonNode = jsonReader.read();

			if (!dbStructureJsonNode.isJsonObject()) {
				throw new DbImportException("Invalid database structure file. Must contain JsonObject with table properties");
			}

			final JsonObject dbStructureJsonObject = (JsonObject) dbStructureJsonNode.getValue();

			itemsToDo = dbStructureJsonObject.size();
			itemsUnitSign = null;
			itemsDone = 0;

			JsonObject foundTableJsonObject = null;
			for (final Entry<String, Object> tableEntry : dbStructureJsonObject.entrySet()) {
				final String currentTableName = tableEntry.getKey();
				final JsonObject tableJsonObject = (JsonObject) tableEntry.getValue();
				if (currentTableName.equalsIgnoreCase(tableNameToUse)) {
					foundTableJsonObject = tableJsonObject;
					break;
				}
			}

			boolean tableWasCreated;
			if (foundTableJsonObject != null) {
				parent.changeTitle(LangResources.get("workingOnTable", tableNameToUse));
				tableWasCreated = createTable(connection, tableNameToUse, foundTableJsonObject);
			} else {
				throw new Exception("Cannot find definition of table '" + tableNameToUse + "' in structure file '" + structureFilePathToLookIn + "'");
			}

			signalProgress(true);
			setEndTime(LocalDateTime.now());
			return tableWasCreated;
		} catch (final Exception e) {
			throw new Exception("Cannot create table '" + tableNameToUse + "': " + e.getMessage(), e);
		}
	}

	private boolean createTable(final Connection connection, final String tableNameToCreate, final JsonObject tableJsonObject) throws Exception {
		if (tableJsonObject == null) {
			throw new DbImportException("Cannot create table without table definition");
		}

		final JsonArray columnsJsonArray = (JsonArray) tableJsonObject.get("columns");
		if (columnsJsonArray == null) {
			throw new DbImportException("Cannot create table without columns definition");
		}

		try (Statement statement = connection.createStatement()) {
			String columnsPart = "";
			for (final Object columnObject : columnsJsonArray) {
				final JsonObject columnJsonObject = (JsonObject) columnObject;

				if (columnsPart.length() > 0) {
					columnsPart += ", ";
				}

				columnsPart = columnsPart + getColumnNameAndType(columnJsonObject);
			}

			List<String> keyColumnsToSet = null;
			if (tableJsonObject.containsPropertyKey("keycolumns")) {
				keyColumnsToSet = ((JsonArray) tableJsonObject.get("keycolumns")).stream().map(String.class::cast).collect(Collectors.toList());
			}

			String primaryKeyPart = "";
			if (keyColumnsToSet != null && Utilities.isNotEmpty(keyColumnsToSet)) {
				primaryKeyPart = ", PRIMARY KEY (" + DbUtilities.joinColumnVendorEscaped(dbDefinition.getDbVendor(), keyColumnsToSet) + ")";
			}
			final String sqlStatementString = "CREATE TABLE " + tableNameToCreate + " (" + columnsPart + primaryKeyPart + ")";
			// TODO remove
			System.out.println("Creating: " + sqlStatementString);
			statement.execute(sqlStatementString);
			if (dbDefinition.getDbVendor() == DbVendor.Derby || dbDefinition.getDbVendor() == DbVendor.MsSQL) {
				connection.commit();
			}
			// TODO remove
			System.out.println("Created: " + sqlStatementString);
			return true;
		}
	}

	private String getColumnNameAndType(final JsonObject columnJsonObject) throws Exception {
		final String name = DbUtilities.escapeVendorReservedNames(dbDefinition.getDbVendor(), (String) columnJsonObject.get("name"));
		final SimpleDataType simpleDataType = SimpleDataType.getSimpleDataTypeByName((String) columnJsonObject.get("datatype"));
		int characterByteSize = -1;
		if (columnJsonObject.containsPropertyKey("datasize")) {
			characterByteSize = (Integer) columnJsonObject.get("datasize");
		}

		Object defaultvalue = null;
		if (columnJsonObject.containsPropertyKey("defaultvalue")) {
			defaultvalue = columnJsonObject.get("defaultvalue");
		}
		String defaultvaluePart = "";
		if (defaultvalue != null) {
			if (simpleDataType == SimpleDataType.String) {
				defaultvaluePart = defaultvalue.toString();
				if (!defaultvaluePart.startsWith("'") || !defaultvaluePart.endsWith("'")) {
					defaultvaluePart = "'" + defaultvaluePart + "'";
				}
			} else {
				defaultvaluePart = defaultvalue.toString();
			}

			defaultvaluePart = " DEFAULT " + defaultvaluePart;
		}

		// "databasevendorspecific_datatype"

		if (simpleDataType == SimpleDataType.String && characterByteSize < 0) {
			// Fallback for extremely large VARCHAR in PostgreSQL. Maybe better use CLOB
			characterByteSize = 4000;
		}

		return name + " " + DbUtilities.getDataType(dbDefinition.getDbVendor(), simpleDataType) + (characterByteSize > -1 ? "(" + characterByteSize + ")" : "") + defaultvaluePart;
	}

	public String getResultStatistics() {
		final StringBuilder statistics = new StringBuilder();

		statistics.append("Found items: " + dataItemsDone + "\n");

		statistics.append("Valid items: " + validItems + "\n");

		statistics.append("Invalid items: " + invalidItems.size() + "\n");
		if (invalidItems.size() > 0) {
			final List<String> errorList = new ArrayList<>();
			for (int i = 0; i < Math.min(10, invalidItems.size()); i++) {
				errorList.add(Integer.toString(invalidItems.get(i)) + ": " + invalidItemsReasons.get(i));
			}
			if (invalidItems.size() > 10) {
				errorList.add("...");
			}
			statistics.append("Indices of invalid items: \n" + Utilities.join(errorList, " \n") + "\n");
			if (erroneousDataFile != null) {
				statistics.append("Erroneous data logged in file: " + erroneousDataFile + "\n");
			}
		}

		if (duplicatesItems > 0) {
			statistics.append("Duplicate items: " + duplicatesItems + "\n");
		}

		statistics.append("Imported data amount: " + Utilities.getHumanReadableNumber(importedDataAmount, "Byte", false, 5, false, Locale.ENGLISH) + "\n");

		if (singleImportModeBlocks > 0) {
			statistics.append("Number of blocks using fallback to single item mode: " + singleImportModeBlocks + "\n");
		}

		if (importMode == ImportMode.CLEARINSERT) {
			statistics.append("Deleted items from db: " + deletedItems + "\n");
		}

		if (duplicateMode == DuplicateMode.MAKE_UNIQUE_JOIN || duplicateMode == DuplicateMode.MAKE_UNIQUE_DROP) {
			statistics.append("Deleted duplicate items in db: " + deletedDuplicatesInDB + "\n");
		}

		if (importMode == ImportMode.CLEARINSERT || importMode == ImportMode.INSERT || importMode == ImportMode.UPSERT) {
			statistics.append("Inserted items: " + insertedItems + "\n");
		}

		if (importMode == ImportMode.UPDATE || importMode == ImportMode.UPSERT) {
			statistics.append("Updated items: " + updatedItems + "\n");
		}

		if (newIndexName != null) {
			statistics.append("Newly created index: " + newIndexName + "\n");
		}

		statistics.append("Count items after import: " + countItems + "\n");

		return statistics.toString();
	}

	private void insertIntoTable(final Connection connection, final String tableNameToUse, final Map<String, DbColumnType> dbColumns, final String itemIndexColumn, final String additionalInsertValuesToUse, final Map<String, Tuple<String, String>> mappingToUse) throws SQLException, Exception {
		final List<Closeable> itemsToCloseAfterwards = new ArrayList<>();

		String additionalInsertValuesSqlColumns = "";
		String additionalInsertValuesSqlValues = "";
		if (Utilities.isNotBlank(additionalInsertValuesToUse)) {
			for (final String line : Utilities.splitAndTrimListQuoted(additionalInsertValuesToUse, '\n', '\r', ';')) {
				final String columnName = line.substring(0, line.indexOf("=")).trim();
				final String columnvalue = line.substring(line.indexOf("=") + 1).trim();
				additionalInsertValuesSqlColumns += columnName + ", ";
				additionalInsertValuesSqlValues += columnvalue + ", ";
			}
		}

		String statementString;
		if (Utilities.isBlank(itemIndexColumn)) {
			statementString = "INSERT INTO " + tableNameToUse + " (" + additionalInsertValuesSqlColumns + DbUtilities.joinColumnVendorEscaped(dbDefinition.getDbVendor(), dbTableColumnsListToInsert) + ") VALUES (" + additionalInsertValuesSqlValues + Utilities.repeat("?", dbTableColumnsListToInsert.size(), ", ") + ")";
		} else {
			statementString = "INSERT INTO " + tableNameToUse + " (" + additionalInsertValuesSqlColumns + DbUtilities.joinColumnVendorEscaped(dbDefinition.getDbVendor(), dbTableColumnsListToInsert) + ", " + itemIndexColumn + ") VALUES (" + additionalInsertValuesSqlValues + Utilities.repeat("?", dbTableColumnsListToInsert.size(), ", ") + ", ?)";
		}

		PreparedStatement preparedStatement = null;
		try {
			preparedStatement = connection.prepareStatement(statementString);

			long maxBlobSize = -1;
			if (dbDefinition.getDbVendor() == DbVendor.MySQL) {
				maxBlobSize = DbUtilities.getMysqlConnectionNumericVariable(connection, "max_allowed_packet");
			} else if (dbDefinition.getDbVendor() == DbVendor.MariaDB) {
				maxBlobSize = DbUtilities.getMariaDBConnectionNumericVariable(connection, "max_allowed_packet");
			}

			final List<List<Object>> batchValues = new ArrayList<>();
			Map<String, Object> itemData;
			while ((itemData = dataProvider.getNextItemData()) != null) {
				if (cancel) {
					break;
				}

				final List<Object> batchValueEntry = new ArrayList<>();
				batchValues.add(batchValueEntry);

				try {
					int i = 1;
					for (final String dbColumnToInsert : dbTableColumnsListToInsert) {
						final SimpleDataType simpleDataType = dbColumns.get(dbColumnToInsert).getSimpleDataType();
						final String unescapedDbColumnToInsert = DbUtilities.unescapeVendorReservedNames(dbDefinition.getDbVendor(), dbColumnToInsert);
						final Object dataValue = itemData.get(mappingToUse.get(unescapedDbColumnToInsert).getFirst());
						final String formatInfo = mappingToUse.get(unescapedDbColumnToInsert).getSecond();

						final Closeable itemToClose = validateAndSetParameter(preparedStatement, i++, dbColumnToInsert, simpleDataType, dbColumns.get(dbColumnToInsert).isNullable(), dataValue, formatInfo, batchValueEntry, maxBlobSize);
						if (itemToClose != null) {
							itemsToCloseAfterwards.add(itemToClose);
						}
					}

					if (Utilities.isNotBlank(itemIndexColumn)) {
						// Add additional integer value to identify data item index
						final long dataValue = dataItemsDone + 1;
						preparedStatement.setLong(i++, dataValue);
						batchValueEntry.add(dataValue);
					}

					preparedStatement.addBatch();

					validItems++;
					signalProgress();
				} catch (final Exception e) {
					invalidItems.add((int) dataItemsDone + 1);
					invalidItemsReasons.add(e.getClass().getSimpleName() + ": " + e.getMessage());
					if (commitOnFullSuccessOnly) {
						connection.rollback();
						throw new DbImportException(e.getClass().getSimpleName() + " error in item index " + (dataItemsDone + 1) + ": " + e.getMessage(), e);
					} else {
						if (dbDefinition.getDbVendor() == DbVendor.SQLite) {
							// SQLite seems to not react on preparedStatement.clearParameters() calls
							for (int i = 1; i <= dbTableColumnsListToInsert.size(); i++) {
								preparedStatement.setObject(i, null);
							}
						} else {
							preparedStatement.clearParameters();
						}
					}
				}
				dataItemsDone++;
				if ("B".equals(itemsUnitSign)) {
					itemsDone = dataProvider.getReadDataSize();
				} else {
					itemsDone = dataItemsDone;
				}

				if (validItems > 0) {
					if (validItems % batchBlockSize == 0) {
						try {
							final int[] results = preparedStatement.executeBatch();
							for (final Closeable itemToClose : itemsToCloseAfterwards) {
								Utilities.closeQuietly(itemToClose);
							}
							itemsToCloseAfterwards.clear();
							for (int i = 0; i < results.length; i++) {
								if (results[i] != 1 && results[i] != Statement.SUCCESS_NO_INFO) {
									invalidItems.add((int) (dataItemsDone - batchBlockSize) + i);
									invalidItemsReasons.add("DB import data error");
								}
							}
							if (!commitOnFullSuccessOnly) {
								connection.commit();
								if (dbDefinition.getDbVendor() == DbVendor.Firebird) {
									preparedStatement.close();
									preparedStatement = connection.prepareStatement(statementString);
								}
							}
						} catch (final BatchUpdateException e) {
							connection.rollback();
							if (commitOnFullSuccessOnly || preventBatchFallbackToSingleLineOnErrors) {
								throw e;
							} else {
								validItems -= executeSingleStepUpdates(connection, preparedStatement, batchValues, (dataItemsDone - (dataItemsDone % batchBlockSize)));
								connection.commit();
							}
						}
						batchValues.clear();
						signalProgress();
					}
				}
			}

			if (batchValues.size() > 0) {
				try {
					final int[] results = preparedStatement.executeBatch();
					for (final Closeable itemToClose : itemsToCloseAfterwards) {
						Utilities.closeQuietly(itemToClose);
					}
					itemsToCloseAfterwards.clear();
					for (int i = 0; i < results.length; i++) {
						if (results[i] != 1 && results[i] != Statement.SUCCESS_NO_INFO) {
							invalidItems.add((int) (dataItemsDone - (dataItemsDone % batchBlockSize)) + i);
							invalidItemsReasons.add("DB import data error");
						}
					}
					if (!commitOnFullSuccessOnly) {
						connection.commit();
					}
				} catch (final BatchUpdateException e) {
					connection.rollback();
					if (commitOnFullSuccessOnly || preventBatchFallbackToSingleLineOnErrors) {
						throw e;
					} else {
						validItems -= executeSingleStepUpdates(connection, preparedStatement, batchValues, (dataItemsDone - (dataItemsDone % batchBlockSize)));
						connection.commit();
					}
				}
				batchValues.clear();
				signalProgress();
			}

			if (commitOnFullSuccessOnly) {
				if (invalidItems.size() == 0) {
					connection.commit();
				} else {
					connection.rollback();
				}
			}
		} catch (final Exception e) {
			connection.rollback();
			throw e;
		} finally {
			for (final Closeable itemToClose : itemsToCloseAfterwards) {
				Utilities.closeQuietly(itemToClose);
			}
			itemsToCloseAfterwards.clear();
			if (preparedStatement != null) {
				preparedStatement.close();
			}
		}
	}

	protected Closeable validateAndSetParameter(
			final PreparedStatement preparedStatement,
			final int columnIndex,
			final String columnName,
			final SimpleDataType simpleDataType,
			final boolean isNullable,
			final Object dataValue,
			final String formatInfo,
			final List<Object> batchValueItem,
			final long maxBlobSize) throws Exception {
		Closeable itemToCloseAfterwards = null;
		if (dataValue == null) {
			if (!isNullable) {
				throw new DbImportException("Column '" + columnName + "' is not nullable but receives null value");
			} else {
				setNullParameter(preparedStatement, columnIndex, simpleDataType);
				batchValueItem.add(null);
			}
		} else if (dataValue instanceof String) {
			if (Utilities.isNotBlank(formatInfo)) {
				String valueString = (String) dataValue;

				if (".".equals(formatInfo)) {
					valueString = valueString.replace(",", "");
					if (valueString.contains(".")) {
						double value;
						try {
							value = Double.parseDouble(valueString);
						} catch (@SuppressWarnings("unused") final NumberFormatException e) {
							throw new DbImportException("Invalid value for numeric column '" + columnName + "': " + valueString);
						}
						preparedStatement.setDouble(columnIndex, value);
						batchValueItem.add(value);
					} else if (simpleDataType == SimpleDataType.Integer) {
						int value;
						try {
							value = Integer.parseInt(valueString);
						} catch (@SuppressWarnings("unused") final NumberFormatException e) {
							throw new DbImportException("Invalid value for integer column '" + columnName + "': " + valueString);
						}
						preparedStatement.setInt(columnIndex, value);
						batchValueItem.add(value);
					} else if (simpleDataType == SimpleDataType.BigInteger) {
						long value;
						try {
							value = Long.parseLong(valueString);
						} catch (@SuppressWarnings("unused") final NumberFormatException e) {
							throw new DbImportException("Invalid value for big integer column '" + columnName + "': " + valueString);
						}
						preparedStatement.setLong(columnIndex, value);
						batchValueItem.add(value);
					} else {
						throw new DbImportException("Invalid value for " + simpleDataType.name() + " data column '" + columnName + "': " + valueString);
					}
				} else if (",".equals(formatInfo)) {
					valueString = valueString.replace(".", "").replace(",", ".");
					if (valueString.contains(".")) {
						double value;
						try {
							value = Double.parseDouble(valueString);
						} catch (@SuppressWarnings("unused") final NumberFormatException e) {
							throw new DbImportException("Invalid value for numeric column '" + columnName + "': " + valueString);
						}
						preparedStatement.setDouble(columnIndex, value);
						batchValueItem.add(value);
					} else if (simpleDataType == SimpleDataType.Integer) {
						int value;
						try {
							value = Integer.parseInt(valueString);
						} catch (@SuppressWarnings("unused") final NumberFormatException e) {
							throw new DbImportException("Invalid value for integer column '" + columnName + "': " + valueString);
						}
						preparedStatement.setInt(columnIndex, value);
						batchValueItem.add(value);
					} else if (simpleDataType == SimpleDataType.BigInteger) {
						long value;
						try {
							value = Long.parseLong(valueString);
						} catch (@SuppressWarnings("unused") final NumberFormatException e) {
							throw new DbImportException("Invalid value for big integer column '" + columnName + "': " + valueString);
						}
						preparedStatement.setLong(columnIndex, value);
						batchValueItem.add(value);
					} else {
						throw new DbImportException("Invalid value for " + simpleDataType.name() + " data column '" + columnName + "': " + valueString);
					}
				} else if ("file".equalsIgnoreCase(formatInfo)) {
					if (!new File(valueString).exists()) {
						throw new DbImportException("File does not exist for column '" + columnName + "': " + valueString);
					} else if (maxBlobSize >= 0 && maxBlobSize < new File(valueString).length()) {
						if (dbDefinition.getDbVendor() == DbVendor.MySQL) {
							throw new Exception("File size is too big for current database settings. Please adjust MySQL server variable 'max_allowed_packet' to at least " + new File(valueString).length());
						} else if (dbDefinition.getDbVendor() == DbVendor.MariaDB) {
							throw new Exception("File size is too big for current database settings. Please adjust MariaDB server variable 'max_allowed_packet' to at least " + new File(valueString).length());
						}
					} else if (simpleDataType == SimpleDataType.Blob) {
						InputStream inputStream;
						if (Utilities.endsWithIgnoreCase(valueString, ".zip")) {
							if (dataProvider.getZipPassword() != null) {
								if (Zip4jUtilities.getZipFileEntries(new File(valueString), dataProvider.getZipPassword()).size() != 1) {
									throw new DbImportException("Compressed import file does not contain a single compressed file for column '" + columnName + "': " + new File(valueString).getAbsolutePath());
								} else {
									inputStream = new CountingInputStream(Zip4jUtilities.openPasswordSecuredZipFile(new File(valueString).getAbsolutePath(), dataProvider.getZipPassword()));
								}
							} else {
								if (ZipUtilities.getZipFileEntries(new File(valueString)).size() != 1) {
									throw new DbImportException("Compressed import file does not contain a single compressed file for column '" + columnName + "': " + new File(valueString).getAbsolutePath());
								} else {
									inputStream = new CountingInputStream(ZipUtilities.openZipFile(new File(valueString).getAbsolutePath()));
								}
							}
						} else if (Utilities.endsWithIgnoreCase(valueString, ".tar.gz")) {
							if (TarGzUtilities.getFilesCount(new File(valueString)) != 1) {
								throw new DbImportException("Compressed import file does not contain a single compressed file for column '" + columnName + "': " + valueString);
							} else {
								inputStream = new CountingInputStream(TarGzUtilities.openCompressedFile(new File(valueString)));
							}
						} else if (Utilities.endsWithIgnoreCase(valueString, ".tgz")) {
							if (TarGzUtilities.getFilesCount(new File(valueString)) != 1) {
								throw new DbImportException("Compressed import file does not contain a single compressed file for column '" + columnName + "': " + valueString);
							} else {
								inputStream = new CountingInputStream(TarGzUtilities.openCompressedFile(new File(valueString)));
							}
						} else if (Utilities.endsWithIgnoreCase(valueString, ".gz")) {
							inputStream = new CountingInputStream(new GZIPInputStream(new FileInputStream(new File(valueString))));
						} else {
							inputStream = new CountingInputStream(new InputStreamWithOtherItemsToClose(new FileInputStream(new File(valueString)), valueString));
						}

						if (dbDefinition.getDbVendor() == DbVendor.SQLite) {
							// SQLite ignores "setBinaryStream"
							final byte[] data = IoUtilities.toByteArray(inputStream);
							preparedStatement.setBytes(columnIndex, data);
							batchValueItem.add(data);
						} else {
							itemToCloseAfterwards = inputStream;
							preparedStatement.setBinaryStream(columnIndex, inputStream);
							batchValueItem.add(itemToCloseAfterwards);
						}
						importedDataAmount += new File(valueString).length();
					} else {
						InputStream inputStream;
						if (Utilities.endsWithIgnoreCase(valueString, ".zip")) {
							if (dataProvider.getZipPassword() != null) {
								if (Zip4jUtilities.getZipFileEntries(new File(valueString), dataProvider.getZipPassword()).size() != 1) {
									throw new DbImportException("Compressed import file does not contain a single compressed file for column '" + columnName + "': " + new File(valueString).getAbsolutePath());
								} else {
									inputStream = new CountingInputStream(Zip4jUtilities.openPasswordSecuredZipFile(new File(valueString).getAbsolutePath(), dataProvider.getZipPassword()));
								}
							} else {
								if (ZipUtilities.getZipFileEntries(new File(valueString)).size() != 1) {
									throw new DbImportException("Compressed import file does not contain a single compressed file for column '" + columnName + "': " + new File(valueString).getAbsolutePath());
								} else {
									inputStream = new CountingInputStream(ZipUtilities.openZipFile(new File(valueString).getAbsolutePath()));
								}
							}
						} else if (Utilities.endsWithIgnoreCase(valueString, ".tar.gz")) {
							if (TarGzUtilities.getFilesCount(new File(valueString)) != 1) {
								throw new DbImportException("Compressed import file does not contain a single compressed file for column '" + columnName + "': " + valueString);
							} else {
								inputStream = new CountingInputStream(TarGzUtilities.openCompressedFile(new File(valueString)));
							}
						} else if (Utilities.endsWithIgnoreCase(valueString, ".tgz")) {
							if (TarGzUtilities.getFilesCount(new File(valueString)) != 1) {
								throw new DbImportException("Compressed import file does not contain a single compressed file for column '" + columnName + "': " + valueString);
							} else {
								inputStream = new CountingInputStream(TarGzUtilities.openCompressedFile(new File(valueString)));
							}
						} else if (Utilities.endsWithIgnoreCase(valueString, ".gz")) {
							inputStream = new CountingInputStream(new GZIPInputStream(new FileInputStream(new File(valueString))));
						} else {
							inputStream = new CountingInputStream(new InputStreamWithOtherItemsToClose(new FileInputStream(new File(valueString)), valueString));
						}

						if (dbDefinition.getDbVendor() == DbVendor.SQLite) {
							// SQLite does not read the stream
							final byte[] data = IoUtilities.toByteArray(inputStream);
							preparedStatement.setString(columnIndex, new String(data, textFileEncoding));
							batchValueItem.add(data);
						} else if (dbDefinition.getDbVendor() == DbVendor.PostgreSQL) {
							// PostgreSQL does not read the stream
							final byte[] data = IoUtilities.toByteArray(inputStream);
							preparedStatement.setNString(columnIndex, new String(data, textFileEncoding));
							batchValueItem.add(data);
						} else {
							itemToCloseAfterwards = new InputStreamReader(inputStream, textFileEncoding);
							preparedStatement.setCharacterStream(columnIndex, (InputStreamReader) itemToCloseAfterwards);
							batchValueItem.add(itemToCloseAfterwards);
						}
						importedDataAmount += new File(valueString).length();
					}
				} else if ("lc".equalsIgnoreCase(formatInfo)) {
					valueString = valueString.toLowerCase();
					if (dbDefinition.getDbVendor() == DbVendor.SQLite || dbDefinition.getDbVendor() == DbVendor.Derby) {
						preparedStatement.setString(columnIndex, valueString);
					} else {
						preparedStatement.setNString(columnIndex, valueString);
					}
					batchValueItem.add(valueString);
				} else if ("uc".equalsIgnoreCase(formatInfo)) {
					valueString = valueString.toUpperCase();
					if (dbDefinition.getDbVendor() == DbVendor.SQLite || dbDefinition.getDbVendor() == DbVendor.Derby) {
						preparedStatement.setString(columnIndex, valueString);
					} else {
						preparedStatement.setNString(columnIndex, valueString);
					}
					batchValueItem.add(valueString);
				} else if ("email".equalsIgnoreCase(formatInfo)) {
					valueString = valueString.toLowerCase().trim();
					if (!NetworkUtilities.isValidEmail(valueString)) {
						throw new DbImportException("Invalid email address for column '" + columnName + "': " + valueString);
					}
					if (dbDefinition.getDbVendor() == DbVendor.SQLite || dbDefinition.getDbVendor() == DbVendor.Derby) {
						preparedStatement.setString(columnIndex, valueString);
					} else {
						preparedStatement.setNString(columnIndex, valueString);
					}
					batchValueItem.add(valueString);
				} else if (simpleDataType == SimpleDataType.DateTime) {
					final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(formatInfo);
					dateTimeFormatter.withResolverStyle(ResolverStyle.STRICT);
					dateTimeFormatter.withZone(importDataZoneId);
					final LocalDateTime localDateTimeValueFromData = LocalDateTime.parse(valueString.trim(), dateTimeFormatter);
					final LocalDateTime localDateTimeValueForDb = localDateTimeValueFromData.atZone(importDataZoneId).withZoneSameInstant(databaseZoneId).toLocalDateTime();
					final Timestamp value = Timestamp.valueOf(localDateTimeValueForDb);
					preparedStatement.setTimestamp(columnIndex, value);
					batchValueItem.add(value);
				} else if (simpleDataType == SimpleDataType.Date) {
					final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(formatInfo);
					dateTimeFormatter.withResolverStyle(ResolverStyle.STRICT);
					dateTimeFormatter.withZone(importDataZoneId);
					final LocalDate localDateValue = LocalDate.parse(valueString.trim(), dateTimeFormatter);
					final java.sql.Date value = java.sql.Date.valueOf(localDateValue);
					preparedStatement.setDate(columnIndex, value);
					batchValueItem.add(value);
				} else {
					throw new DbImportException("Unknown data type for column '" + columnName + "': " + simpleDataType);
				}
			} else if (simpleDataType == SimpleDataType.DateTime) {
				final String valueString = ((String) dataValue).trim();
				LocalDateTime localDateTimeValueForDb;
				if (Utilities.isBlank(valueString)) {
					if (!isNullable) {
						throw new DbImportException("Column '" + columnName + "' is not nullable but receives null value");
					} else {
						setNullParameter(preparedStatement, columnIndex, SimpleDataType.DateTime);
						batchValueItem.add(null);
					}
				} else {
					if (Utilities.isNotBlank(dateTimeFormatPattern)) {
						final LocalDateTime localDateTimeValueFromData = LocalDateTime.parse(valueString.trim(), getConfiguredDateTimeFormatter());
						localDateTimeValueForDb = localDateTimeValueFromData.atZone(importDataZoneId).withZoneSameInstant(databaseZoneId).toLocalDateTime();
					} else {
						try {
							final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(DateUtilities.getDateTimeFormatWithSecondsPattern(Locale.getDefault()));
							dateTimeFormatter.withResolverStyle(ResolverStyle.STRICT);
							dateTimeFormatter.withZone(importDataZoneId);
							final LocalDateTime localDateTimeValueFromData = LocalDateTime.parse(valueString.trim(), dateTimeFormatter);
							localDateTimeValueForDb = localDateTimeValueFromData.atZone(importDataZoneId).withZoneSameInstant(databaseZoneId).toLocalDateTime();
						} catch (@SuppressWarnings("unused") final DateTimeParseException e) {
							try {
								final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(DateUtilities.getDateTimeFormatPattern(Locale.getDefault()));
								dateTimeFormatter.withResolverStyle(ResolverStyle.STRICT);
								dateTimeFormatter.withZone(importDataZoneId);
								final LocalDateTime localDateTimeValueFromData = LocalDateTime.parse(valueString.trim(), dateTimeFormatter);
								localDateTimeValueForDb = localDateTimeValueFromData.atZone(importDataZoneId).withZoneSameInstant(databaseZoneId).toLocalDateTime();
							} catch (@SuppressWarnings("unused") final DateTimeParseException e1) {
								try {
									final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(DateUtilities.getDateFormatPattern(Locale.getDefault()));
									dateTimeFormatter.withResolverStyle(ResolverStyle.STRICT);
									localDateTimeValueForDb = LocalDate.parse(valueString.trim(), dateTimeFormatter).atStartOfDay();
								} catch (@SuppressWarnings("unused") final DateTimeParseException e2) {
									try {
										final LocalDateTime localDateTimeValueFromData = DateUtilities.parseIso8601DateTimeString(valueString, importDataZoneId).toLocalDateTime();
										localDateTimeValueForDb = localDateTimeValueFromData.atZone(importDataZoneId).withZoneSameInstant(databaseZoneId).toLocalDateTime();
									} catch (@SuppressWarnings("unused") final DateTimeException e3) {
										final ZonedDateTime zonedDateTimeValueFromData = DateUtilities.parseUnknownDateFormat(valueString, importDataZoneId);
										localDateTimeValueForDb = zonedDateTimeValueFromData.withZoneSameInstant(databaseZoneId).toLocalDateTime();
									}
								}
							}
						}
					}
					final Timestamp value = Timestamp.valueOf(localDateTimeValueForDb);
					preparedStatement.setTimestamp(columnIndex, value);
					batchValueItem.add(value);
				}
			} else if (simpleDataType == SimpleDataType.Date) {
				final String valueString = ((String) dataValue).trim();
				LocalDateTime localDateTimeValueForDb;
				if (Utilities.isBlank(valueString)) {
					if (!isNullable) {
						throw new DbImportException("Column '" + columnName + "' is not nullable but receives null value");
					} else {
						setNullParameter(preparedStatement, columnIndex, SimpleDataType.DateTime);
						batchValueItem.add(null);
					}
				} else {
					if (Utilities.isNotBlank(dateFormatPattern)) {
						try {
							final LocalDateTime localDateTimeValueFromData = LocalDate.parse(valueString.trim(), getConfiguredDateFormatter()).atStartOfDay();
							localDateTimeValueForDb = localDateTimeValueFromData;
						} catch (final DateTimeParseException e) {
							// Try fallback to DateTime format if set, because some databases export dates with time even for DATE datatype (e.g. Oracle)
							if (Utilities.isNotBlank(dateTimeFormatPattern)) {
								try {
									final LocalDateTime localDateTimeValueFromData = LocalDateTime.parse(valueString.trim(), getConfiguredDateTimeFormatter());
									localDateTimeValueForDb = localDateTimeValueFromData.atZone(importDataZoneId).withZoneSameInstant(databaseZoneId).toLocalDateTime();
								} catch (@SuppressWarnings("unused") final Exception e1) {
									throw e;
								}
							} else {
								throw e;
							}
						}
					} else {
						try {
							final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(DateUtilities.getDateTimeFormatWithSecondsPattern(Locale.getDefault()));
							dateTimeFormatter.withResolverStyle(ResolverStyle.STRICT);
							dateTimeFormatter.withZone(importDataZoneId);
							final LocalDateTime localDateTimeValueFromData = LocalDateTime.parse(valueString.trim(), dateTimeFormatter);
							localDateTimeValueForDb = localDateTimeValueFromData.atZone(importDataZoneId).withZoneSameInstant(databaseZoneId).toLocalDateTime();
						} catch (@SuppressWarnings("unused") final DateTimeParseException e) {
							try {
								final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(DateUtilities.getDateTimeFormatPattern(Locale.getDefault()));
								dateTimeFormatter.withResolverStyle(ResolverStyle.STRICT);
								dateTimeFormatter.withZone(importDataZoneId);
								final LocalDateTime localDateTimeValueFromData = LocalDateTime.parse(valueString.trim(), dateTimeFormatter);
								localDateTimeValueForDb = localDateTimeValueFromData.atZone(importDataZoneId).withZoneSameInstant(databaseZoneId).toLocalDateTime();
							} catch (@SuppressWarnings("unused") final DateTimeParseException e1) {
								try {
									final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(DateUtilities.getDateFormatPattern(Locale.getDefault()));
									dateTimeFormatter.withResolverStyle(ResolverStyle.STRICT);
									localDateTimeValueForDb = LocalDate.parse(valueString.trim(), dateTimeFormatter).atStartOfDay();
								} catch (@SuppressWarnings("unused") final DateTimeParseException e2) {
									try {
										final LocalDateTime localDateTimeValueFromData = DateUtilities.parseIso8601DateTimeString(valueString, importDataZoneId).toLocalDateTime();
										localDateTimeValueForDb = localDateTimeValueFromData.atZone(importDataZoneId).withZoneSameInstant(databaseZoneId).toLocalDateTime();
									} catch (@SuppressWarnings("unused") final DateTimeException e3) {
										final ZonedDateTime zonedDateTimeValueFromData = DateUtilities.parseUnknownDateFormat(valueString, importDataZoneId);
										localDateTimeValueForDb = zonedDateTimeValueFromData.withZoneSameInstant(databaseZoneId).toLocalDateTime();
									}
								}
							}
						}
					}
					final Timestamp value = Timestamp.valueOf(localDateTimeValueForDb);
					preparedStatement.setTimestamp(columnIndex, value);
					batchValueItem.add(value);
				}
			} else if (simpleDataType == SimpleDataType.Blob) {
				final byte[] value = Utilities.decodeBase64((String) dataValue);
				preparedStatement.setBytes(columnIndex, value);
				batchValueItem.add(value);
			} else if (simpleDataType == SimpleDataType.Float) {
				final String valueString = ((String) dataValue).trim();
				if (valueString.contains(".")) {
					double value;
					try {
						value = Double.parseDouble(valueString);
					} catch (@SuppressWarnings("unused") final NumberFormatException e) {
						throw new DbImportException("Invalid value for numeric column '" + columnName + "': " + valueString);
					}
					preparedStatement.setDouble(columnIndex, value);
					batchValueItem.add(value);
				} else {
					int value;
					try {
						value = Integer.parseInt(valueString);
					} catch (@SuppressWarnings("unused") final NumberFormatException e) {
						throw new DbImportException("Invalid value for numeric column '" + columnName + "': " + valueString);
					}
					preparedStatement.setInt(columnIndex, value);
					batchValueItem.add(value);
				}
			} else if (simpleDataType == SimpleDataType.Integer) {
				final String valueString = ((String) dataValue).trim();
				if (valueString.equalsIgnoreCase("true")) {
					preparedStatement.setInt(columnIndex, 1);
					batchValueItem.add(1);
				} else if (valueString.equalsIgnoreCase("false")) {
					preparedStatement.setInt(columnIndex, 0);
					batchValueItem.add(0);
				} else if (valueString.contains(".")) {
					double value;
					try {
						value = Double.parseDouble(valueString);
					} catch (@SuppressWarnings("unused") final NumberFormatException e) {
						throw new DbImportException("Invalid value for numeric column '" + columnName + "': " + valueString);
					}
					preparedStatement.setDouble(columnIndex, value);
					batchValueItem.add(value);
				} else {
					int value;
					try {
						value = Integer.parseInt(valueString);
					} catch (@SuppressWarnings("unused") final NumberFormatException e) {
						throw new DbImportException("Invalid value for integer column '" + columnName + "': " + valueString);
					}
					preparedStatement.setInt(columnIndex, value);
					batchValueItem.add(value);
				}
			} else if (simpleDataType == SimpleDataType.BigInteger) {
				final String valueString = ((String) dataValue).trim();
				if (valueString.contains(".")) {
					double value;
					try {
						value = Double.parseDouble(valueString);
					} catch (@SuppressWarnings("unused") final NumberFormatException e) {
						throw new DbImportException("Invalid value for numeric column '" + columnName + "': " + valueString);
					}
					preparedStatement.setDouble(columnIndex, value);
					batchValueItem.add(value);
				} else {
					long value;
					try {
						value = Long.parseLong(valueString);
					} catch (@SuppressWarnings("unused") final NumberFormatException e) {
						throw new DbImportException("Value is invalid for big integer column '" + columnName + "': " + valueString);
					}
					preparedStatement.setLong(columnIndex, value);
					batchValueItem.add(value);
				}
			} else if (simpleDataType == SimpleDataType.String || simpleDataType == SimpleDataType.Clob) {
				if (dbDefinition.getDbVendor() == DbVendor.SQLite || dbDefinition.getDbVendor() == DbVendor.Derby) {
					preparedStatement.setString(columnIndex, (String) dataValue);
				} else {
					preparedStatement.setNString(columnIndex, (String) dataValue);
				}
				batchValueItem.add(dataValue);
			} else if (simpleDataType == SimpleDataType.DateTime) {
				throw new DbImportException("Date field to insert without mapping date format for column '" + columnName + "'");
			} else if (simpleDataType == SimpleDataType.Date) {
				throw new DbImportException("Date field to insert without mapping date format for column '" + columnName + "'");
			} else {
				throw new DbImportException("Unknown data type field to insert without mapping format for column '" + columnName + "'");
			}
		} else if (dataValue instanceof ZonedDateTime) {
			final LocalDateTime localDateTimeValueForDb = ((ZonedDateTime) dataValue).withZoneSameInstant(databaseZoneId).toLocalDateTime();
			final Timestamp value = Timestamp.valueOf(localDateTimeValueForDb);
			preparedStatement.setTimestamp(columnIndex, value);
			batchValueItem.add(value);
		} else if (dataValue instanceof LocalDateTime) {
			final Timestamp value = Timestamp.valueOf((LocalDateTime) dataValue);
			preparedStatement.setTimestamp(columnIndex, value);
			batchValueItem.add(value);
		} else if (dataValue instanceof Number && simpleDataType == SimpleDataType.Float) {
			// Keep the right precision when inserting a float value to a double column
			double value;
			try {
				value = Double.parseDouble(dataValue.toString());
			} catch (@SuppressWarnings("unused") final NumberFormatException e) {
				throw new DbImportException("Invalid value for numeric column '" + columnName + "': " + dataValue.toString());
			}
			preparedStatement.setDouble(columnIndex, value);
			batchValueItem.add(value);
		} else if (dataValue instanceof MonthDay && simpleDataType == SimpleDataType.String) {
			final String value = dataValue.toString();
			if (dbDefinition.getDbVendor() == DbVendor.SQLite || dbDefinition.getDbVendor() == DbVendor.Derby) {
				preparedStatement.setString(columnIndex, value);
			} else {
				preparedStatement.setNString(columnIndex, value);
			}
			batchValueItem.add(value);
		} else {
			preparedStatement.setObject(columnIndex, dataValue);
			batchValueItem.add(dataValue);
		}
		return itemToCloseAfterwards;
	}

	private void setNullParameter(final PreparedStatement preparedStatement, final int columnIndex, final SimpleDataType simpleDataType) throws SQLException {
		if (simpleDataType == SimpleDataType.String) {
			preparedStatement.setNull(columnIndex, java.sql.Types.VARCHAR);
		} else if (dbDefinition.getDbVendor() == DbVendor.Oracle) {
			if (simpleDataType == SimpleDataType.Date) {
				preparedStatement.setNull(columnIndex, java.sql.Types.TIMESTAMP);
			} else if (simpleDataType == SimpleDataType.DateTime) {
				preparedStatement.setNull(columnIndex, java.sql.Types.TIMESTAMP);
			} else if (simpleDataType == SimpleDataType.Float) {
				preparedStatement.setNull(columnIndex, java.sql.Types.DOUBLE);
			} else if (simpleDataType == SimpleDataType.Integer) {
				preparedStatement.setNull(columnIndex, java.sql.Types.INTEGER);
			} else if (simpleDataType == SimpleDataType.BigInteger) {
				preparedStatement.setNull(columnIndex, java.sql.Types.BIGINT);
			} else if (simpleDataType == SimpleDataType.Clob) {
				preparedStatement.setNull(columnIndex, java.sql.Types.CLOB);
			} else if (simpleDataType == SimpleDataType.Blob) {
				preparedStatement.setNull(columnIndex, java.sql.Types.BLOB);
			} else {
				preparedStatement.setNull(columnIndex, 0);
			}
		} else {
			preparedStatement.setNull(columnIndex, 0);
		}
	}

	private DateTimeFormatter getConfiguredDateTimeFormatter() {
		if (dateTimeFormatterCache == null) {
			final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(dateTimeFormatPattern);
			dateTimeFormatter.withResolverStyle(ResolverStyle.STRICT);
			dateTimeFormatter.withZone(importDataZoneId);
			dateTimeFormatterCache = dateTimeFormatter;
		}
		return dateTimeFormatterCache;
	}

	private DateTimeFormatter getConfiguredDateFormatter() {
		if (dateFormatterCache == null) {
			final DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern(dateFormatPattern);
			dateFormatter.withResolverStyle(ResolverStyle.STRICT);
			dateFormatter.withZone(importDataZoneId);
			dateFormatterCache = dateFormatter;
		}
		return dateFormatterCache;
	}

	private int executeSingleStepUpdates(final Connection connection, final PreparedStatement preparedStatement, final List<List<Object>> batchValues, final long startingDataEntryIndex) throws Exception {
		singleImportModeBlocks++;
		preparedStatement.clearBatch();
		int notImportedBySingleMode = 0;
		for (int entryIndex = 0; entryIndex < batchValues.size(); entryIndex++) {
			final List<Object> batchValueEntry = batchValues.get(entryIndex);
			for (int i = 0; i < batchValueEntry.size(); i++) {
				final Object dataValue = batchValueEntry.get(i);
				if (dataValue != null
						&& !(dataValue instanceof Timestamp)
						&& !(dataValue instanceof java.sql.Date)
						&& !(dataValue instanceof Date)
						&& !(dataValue instanceof Integer)
						&& !(dataValue instanceof Long)
						&& !(dataValue instanceof Double)
						&& !(dataValue instanceof Float)
						&& !(dataValue instanceof String)) {
					throw new Exception("Unexpected datatype: " + dataValue.getClass().getSimpleName());
				}
				if (dataValue != null && dataValue instanceof String) {
					if (dbDefinition.getDbVendor() == DbVendor.SQLite || dbDefinition.getDbVendor() == DbVendor.Derby) {
						preparedStatement.setString(i + 1, (String) dataValue);
					} else {
						preparedStatement.setNString(i + 1, (String) dataValue);
					}
				} else {
					preparedStatement.setObject(i + 1, dataValue);
				}
			}
			try {
				preparedStatement.execute();
				connection.commit();
			} catch (final SQLException e) {
				connection.rollback();
				notImportedBySingleMode++;
				preparedStatement.clearBatch();
				invalidItems.add((int) startingDataEntryIndex + entryIndex);
				invalidItemsReasons.add("DB import data error: " + e.getMessage());
			}
		}
		return notImportedBySingleMode;
	}

	protected static void logToFile(final OutputStream logOutputStream, final String message) throws Exception {
		if (logOutputStream != null) {
			logOutputStream.write((message.trim() + "\n").getBytes(StandardCharsets.UTF_8));
		}
	}

	public long getDeletedItems() {
		return deletedItems;
	}

	public long getUpdatedItems() {
		return updatedItems;
	}

	public long getImportedItems() {
		return validItems;
	}

	public String getCreatedNewIndexName() {
		return newIndexName;
	}

	public List<Integer> getNotImportedItems() {
		return invalidItems;
	}

	public List<String> getNotImportedItemsReasons() {
		return invalidItemsReasons;
	}

	public long getImportedDataItems() {
		return dataItemsDone;
	}

	public long getImportedDataAmount() {
		return importedDataAmount;
	}

	public long getIgnoredDuplicates() {
		return duplicatesItems;
	}

	public long getInsertedItems() {
		return insertedItems;
	}

	public List<String> getDataPropertyNames() {
		return availableDataPropertyNames;
	}

	private static String convertMappingToString(final Map<String, Tuple<String, String>> mapping) {
		final StringBuilder returnValue = new StringBuilder();

		if (mapping != null) {
			final List<Entry<String,Tuple<String,String>>> sortedListOfMappingEntries = mapping.entrySet().stream().sorted(Map.Entry.<String, Tuple<String, String>>comparingByKey()).collect(Collectors.toList());
			for (final Entry<String, Tuple<String, String>> entry : sortedListOfMappingEntries) {
				returnValue.append(entry.getKey() + "=\"" + entry.getValue().getFirst() + "\"");
				if (Utilities.isNotBlank(entry.getValue().getSecond())) {
					returnValue.append(" " + entry.getValue().getSecond());
				}
				returnValue.append("\n");
			}
		}

		return returnValue.toString().trim();
	}

	@Override
	public String getResultText() {
		return getResultStatistics();
	}
}
