package de.soderer.dbimport;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.DateTimeException;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Pattern;

import de.soderer.dbimport.DbImportDefinition.DuplicateMode;
import de.soderer.dbimport.DbImportDefinition.ImportMode;
import de.soderer.dbimport.dataprovider.DataProvider;
import de.soderer.utilities.DateUtilities;
import de.soderer.utilities.DbColumnType;
import de.soderer.utilities.DbColumnType.SimpleDataType;
import de.soderer.utilities.DbNotExistsException;
import de.soderer.utilities.DbUtilities;
import de.soderer.utilities.DbUtilities.DbVendor;
import de.soderer.utilities.LangResources;
import de.soderer.utilities.NetworkUtilities;
import de.soderer.utilities.TextUtilities;
import de.soderer.utilities.Tuple;
import de.soderer.utilities.Utilities;
import de.soderer.utilities.collection.CaseInsensitiveMap;
import de.soderer.utilities.collection.CaseInsensitiveSet;
import de.soderer.utilities.worker.WorkerParentSimple;
import de.soderer.utilities.worker.WorkerSimple;

public class DbImportWorker extends WorkerSimple<Boolean> {
	// Mandatory parameters
	protected DbUtilities.DbVendor dbVendor = null;
	protected ImportMode importMode = ImportMode.INSERT;
	protected DuplicateMode duplicateMode = DuplicateMode.UPDATE_ALL_JOIN;
	protected List<String> keyColumns = null;
	protected List<String> keyColumnsWithFunctions = null;
	protected String hostname;
	protected String dbName;
	protected String username;
	protected char[] password;
	protected boolean secureConnection;
	protected String trustStoreFilePath;
	protected char[] trustStorePassword;
	protected String tableName;
	protected boolean createTableIfNotExists = false;
	protected boolean tableWasCreated = false;
	protected boolean commitOnFullSuccessOnly = true;
	protected boolean createNewIndexIfNeeded = true;
	protected boolean deactivateForeignKeyConstraints = false;
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

	protected DataProvider dataProvider = null;

	public DbImportWorker(final WorkerParentSimple parent, final DbVendor dbVendor, final String hostname, final String dbName, final String username, final char[] password, final boolean secureConnection, final String trustStoreFilePath, final char[] trustStorePassword, final String tableName) throws Exception {
		super(parent);

		this.dbVendor = dbVendor;
		this.hostname = hostname;
		this.dbName = dbName;
		this.username = username;
		this.password = password;
		this.secureConnection = secureConnection;
		this.trustStoreFilePath = trustStoreFilePath;
		this.trustStorePassword = trustStorePassword;
		this.tableName = tableName;
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

	public void setMapping(final String mappingString) throws IOException, Exception {
		if (Utilities.isNotBlank(mappingString)) {
			mapping = DbImportMappingDialog.parseMappingString(mappingString);
			dbTableColumnsListToInsert = new ArrayList<>();
			for (final String dbColumn : mapping.keySet()) {
				dbTableColumnsListToInsert.add(DbUtilities.unescapeVendorReservedNames(dbVendor, dbColumn));
			}
		} else {
			mapping = null;
		}
	}

	public Map<String, Tuple<String, String>> getMapping() throws Exception {
		if (mapping == null && (! (this instanceof DbSqlWorker))) {
			mapping = new HashMap<>();
			for (final String propertyName : dataProvider.getAvailableDataPropertyNames()) {
				mapping.put(propertyName.toLowerCase(), new Tuple<>(propertyName, ""));
			}
			dbTableColumnsListToInsert = new ArrayList<>();
			for (final String dbColumn : mapping.keySet()) {
				dbTableColumnsListToInsert.add(DbUtilities.unescapeVendorReservedNames(dbVendor, dbColumn));
			}
		}
		return mapping;
	}

	protected void checkMapping(final Map<String, DbColumnType> dbColumns) throws Exception, DbImportException {
		final List<String> dataPropertyNames = dataProvider.getAvailableDataPropertyNames();
		if (mapping != null) {
			for (String dbColumnToInsert : dbTableColumnsListToInsert) {
				dbColumnToInsert = DbUtilities.unescapeVendorReservedNames(dbVendor, dbColumnToInsert);
				if (!dbColumns.containsKey(dbColumnToInsert)) {
					throw new DbImportException("DB table does not contain mapped column: " + dbColumnToInsert);
				}
			}

			final Set<String> mappedDbColumns = new CaseInsensitiveSet();
			for (final Entry<String, Tuple<String, String>> mappingEntry : mapping.entrySet()) {
				if (Utilities.isNotBlank(mappingEntry.getKey()) && !mappedDbColumns.add(mappingEntry.getKey())) {
					throw new DbImportException("Mapping contains db column multiple times: " + mappingEntry.getKey());
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
					if (DbUtilities.unescapeVendorReservedNames(dbVendor, keyColumn).equals(DbUtilities.unescapeVendorReservedNames(dbVendor, entry.getKey()))) {
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

	public void setLogErroneousData(final boolean logErroneousData) {
		this.logErroneousData = logErroneousData;
	}

	@SuppressWarnings("resource")
	@Override
	public Boolean work() throws Exception {
		showUnlimitedProgress();

		if (analyseDataOnly) {
			parent.changeTitle(LangResources.get("analyseData"));
			availableDataPropertyNames = dataProvider.getAvailableDataPropertyNames();
		} else {
			OutputStream logOutputStream = null;
			Connection connection = null;
			boolean previousAutoCommit = false;
			String tempTableName = null;
			boolean constraintWereDeactivated = false;
			try {
				if (dbVendor == DbVendor.Derby || (dbVendor == DbVendor.HSQL && Utilities.isBlank(hostname)) || dbVendor == DbVendor.SQLite) {
					try {
						connection = DbUtilities.createConnection(dbVendor, hostname, dbName, username, (password == null ? null : password), false, null, null, true);
					} catch (@SuppressWarnings("unused") final DbNotExistsException e) {
						connection = DbUtilities.createNewDatabase(dbVendor, dbName);
					}
				} else {
					connection = DbUtilities.createConnection(dbVendor, hostname, dbName, username, (password == null ? null : password), secureConnection, Utilities.isNotBlank(trustStoreFilePath) ? new File(trustStoreFilePath) : null, trustStorePassword, false);
				}

				previousAutoCommit = connection.getAutoCommit();
				connection.setAutoCommit(false);

				validItems = 0;
				invalidItems = new ArrayList<>();
				invalidItemsReasons = new ArrayList<>();

				if (logFile != null) {
					logOutputStream = new FileOutputStream(logFile);
					logToFile(logOutputStream, getConfigurationLogString());
				}

				logToFile(logOutputStream, "Start: " + DateUtilities.formatDate(DateUtilities.getDateTimeFormatWithSecondsPattern(Locale.getDefault()), getStartTime()));

				if (deactivateForeignKeyConstraints) {
					constraintWereDeactivated = true;
					DbUtilities.setForeignKeyConstraintStatus(dbVendor, connection, false);
					connection.commit();
				}

				createTableIfNeeded(connection, tableName, keyColumns);

				final Map<String, DbColumnType> dbColumns = DbUtilities.getColumnDataTypes(connection, tableName);
				checkMapping(dbColumns);

				if (dbTableColumnsListToInsert.size() == 0) {
					throw new DbImportException("Invalid empty mapping");
				}

				if (!DbUtilities.checkTableAndColumnsExist(connection, tableName, keyColumns == null ? null : keyColumns.toArray(new String[0]))) {
					throw new DbImportException("Some keycolumn is not included in table");
				}

				if (importMode == ImportMode.CLEARINSERT) {
					deletedItems = DbUtilities.clearTable(connection, tableName);
				}

				parent.changeTitle(LangResources.get("readData"));
				itemsToDo = dataProvider.getItemsAmountToImport();
				logToFile(logOutputStream, "Items to import: " + itemsToDo);
				showProgress(true);

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

					itemsToDo = 4;
					itemsDone = 0;
					showProgress(true);
					parent.changeTitle(LangResources.get("dropDuplicates"));

					// Handle duplicates in import data
					if (duplicateMode == DuplicateMode.NO_CHECK) {
						// Do not check for duplicates
					} else if (duplicateMode == DuplicateMode.CKECK_SOURCE_ONLY_DROP || duplicateMode == DuplicateMode.UPDATE_FIRST_DROP || duplicateMode == DuplicateMode.UPDATE_ALL_DROP || duplicateMode == DuplicateMode.MAKE_UNIQUE_DROP) {
						duplicatesItems = DbUtilities.dropDuplicates(connection, tempTableName, keyColumns);
					} else if (duplicateMode == DuplicateMode.CKECK_SOURCE_ONLY_JOIN || duplicateMode == DuplicateMode.UPDATE_FIRST_JOIN || duplicateMode == DuplicateMode.UPDATE_ALL_JOIN || duplicateMode == DuplicateMode.MAKE_UNIQUE_JOIN) {
						duplicatesItems = DbUtilities.joinDuplicates(connection, tempTableName, keyColumns, updateWithNullValues);
					} else {
						throw new Exception("Invalid duplicate mode");
					}

					itemsDone = 1;
					showProgress(true);

					if (cancel) {
						return false;
					}

					if (importMode == ImportMode.CLEARINSERT) {
						parent.changeTitle(LangResources.get("insertData"));

						insertedItems = DbUtilities.insertNotExistingItems(connection, tempTableName, tableName, dbTableColumnsListToInsert, keyColumnsWithFunctions, additionalInsertValues);

						itemsDone = 2;
						showProgress(true);
					} else if (importMode == ImportMode.INSERT) {
						if (duplicateMode == DuplicateMode.NO_CHECK || duplicateMode == DuplicateMode.CKECK_SOURCE_ONLY_DROP || duplicateMode == DuplicateMode.CKECK_SOURCE_ONLY_JOIN) {
							parent.changeTitle(LangResources.get("insertData"));

							// Insert all entries
							insertedItems = DbUtilities.insertAllItems(connection, tempTableName, tableName, dbTableColumnsListToInsert, additionalInsertValues);

							itemsDone = 2;
							showProgress(true);
						} else {
							parent.changeTitle(LangResources.get("dropDuplicates"));

							// Insert only not existing entries
							duplicatesItems += DbUtilities.dropDuplicatesCrossTable(connection, tableName, tempTableName, keyColumnsWithFunctions);

							if (cancel) {
								return false;
							}

							itemsDone = 2;
							showProgress(true);
							parent.changeTitle(LangResources.get("insertData"));

							insertedItems = DbUtilities.insertNotExistingItems(connection, tempTableName, tableName, dbTableColumnsListToInsert, keyColumnsWithFunctions, additionalInsertValues);

							itemsDone = 3;
							showProgress(true);
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

							itemsDone = 2;
							showProgress(true);
						} else {
							if (cancel) {
								return false;
							}

							parent.changeTitle(LangResources.get("updateData"));

							// Update destination table
							updatedItems = DbUtilities.updateAllExistingItems(connection, tempTableName, tableName, dbTableColumnsListToInsert, keyColumns, tempItemIndexColumn, updateWithNullValues, additionalUpdateValues);

							itemsDone = 2;
							showProgress(true);
						}
					} else if (importMode == ImportMode.UPSERT) {
						if (duplicateMode == DuplicateMode.NO_CHECK || duplicateMode == DuplicateMode.CKECK_SOURCE_ONLY_DROP || duplicateMode == DuplicateMode.CKECK_SOURCE_ONLY_JOIN) {
							parent.changeTitle(LangResources.get("insertData"));

							// Insert all entries
							insertedItems = DbUtilities.insertAllItems(connection, tempTableName, tableName, dbTableColumnsListToInsert, additionalInsertValues);

							itemsDone = 2;
							showProgress(true);
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

							itemsDone = 2;
							showProgress(true);
							parent.changeTitle(LangResources.get("insertData"));

							// Insert into destination table
							insertedItems = DbUtilities.insertNotExistingItems(connection, tempTableName, tableName, dbTableColumnsListToInsert, keyColumnsWithFunctions, additionalInsertValues);

							itemsDone = 3;
							showProgress(true);
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

							itemsDone = 2;
							showProgress(true);
							parent.changeTitle(LangResources.get("insertData"));

							// Insert into destination table
							insertedItems = DbUtilities.insertNotExistingItems(connection, tempTableName, tableName, dbTableColumnsListToInsert, keyColumnsWithFunctions, additionalInsertValues);

							itemsDone = 3;
							showProgress(true);
						}
					} else {
						throw new Exception("Invalid import mode");
					}
				}

				connection.commit();

				parent.changeTitle(LangResources.get("collectResult"));

				countItems = DbUtilities.getTableEntriesCount(connection, tableName);

				itemsDone = 4;

				itemsDone = dataItemsDone;
				showProgress(true);

				if (logErroneousData & invalidItems.size() > 0) {
					erroneousDataFile = dataProvider.filterDataItems(invalidItems, DateUtilities.formatDate(DateUtilities.DD_MM_YYYY_HH_MM_SS_ForFileName, getStartTime()) + ".errors");
				}

				importedDataAmount += dataProvider.getImportDataAmount();
			} catch (final SQLException sqle) {
				try {
					logToFile(logOutputStream, "SQL Error: " + sqle.getMessage());
				} catch (final Exception e1) {
					e1.printStackTrace();
				}
				throw new DbImportException("SQL error: " + sqle.getMessage());
			} catch (final Exception e) {
				try {
					logToFile(logOutputStream, "Error: " + e.getMessage());
				} catch (final Exception e1) {
					e1.printStackTrace();
				}
				throw e;
			} finally {
				if (deactivateForeignKeyConstraints && constraintWereDeactivated && connection != null) {
					DbUtilities.setForeignKeyConstraintStatus(dbVendor, connection, true);
					connection.commit();
				}

				dataProvider.close();

				setEndTime(LocalDateTime.now());

				// Drop temp table
				DbUtilities.dropTableIfExists(connection, tempTableName);

				if (connection != null) {
					connection.rollback();
					connection.setAutoCommit(previousAutoCommit);
					connection.close();
					connection = null;
					if (dbVendor == DbVendor.Derby) {
						DbUtilities.shutDownDerbyDb(dbName);
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
					logToFile(logOutputStream, "Time elapsed: " + DateUtilities.getHumanReadableTimespan(Duration.between(getStartTime(), getEndTime()), true));
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
				+ (createTableIfNotExists ? "New table was created: " + tableWasCreated + "\n" : "")
				+ "Mapping: \n" + TextUtilities.addLeadingTab(convertMappingToString(getMapping())) + "\n"
				+ (Utilities.isNotBlank(additionalInsertValues) ? "Additional insert values: " + additionalInsertValues + "\n" : "")
				+ (Utilities.isNotBlank(additionalUpdateValues) ? "Additional update values: " + additionalUpdateValues + "\n" : "")
				+ "Update with null values: " + updateWithNullValues + "\n";
	}

	protected void createTableIfNeeded(final Connection connection, final String tableNameToUse, final List<String> keyColumnsToUse) throws Exception, DbImportException, SQLException {
		if (!DbUtilities.checkTableExist(connection, tableNameToUse)) {
			if (createTableIfNotExists) {
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
				if (dbVendor == DbVendor.PostgreSQL) {
					// Close a maybe open transaction to allow DDL-statement
					connection.rollback();
				}
				try {
					DbUtilities.createTable(connection, tableNameToUse, dbDataTypes, keyColumnsToUse);
					tableWasCreated = true;
				} catch (final Exception e) {
					throw new DbImportException("Cannot create new table '" + tableNameToUse + "': " + e.getMessage(), e);
				}
				if (dbVendor == DbVendor.PostgreSQL) {
					// Commit DDL-statement
					connection.commit();
				}
			} else {
				throw new DbImportException("Table does not exist: " + tableNameToUse);
			}
		}
	}

	public String getResultStatistics() {
		final StringBuilder statistics = new StringBuilder();

		statistics.append("Found items: " + dataItemsDone + "\n");

		statistics.append("Valid items: " + validItems + "\n");

		statistics.append("Invalid items: " + invalidItems.size() + "\n");
		if (invalidItems.size() > 0) {
			final List<String> errorList = new ArrayList<>();
			for (int i = 0; i < Math.min(10, invalidItems.size()); i++) {
				errorList.add(Integer.toString(invalidItems.get(i)));
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

	protected void insertIntoTable(final Connection connection, final String tableNameToUse, final Map<String, DbColumnType> dbColumns, final String itemIndexColumn, final String additionalInsertValuesToUse, final Map<String, Tuple<String, String>> mappingToUse) throws SQLException, Exception {
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
			statementString = "INSERT INTO " + tableNameToUse + " (" + additionalInsertValuesSqlColumns + DbUtilities.joinColumnVendorEscaped(dbVendor, dbTableColumnsListToInsert) + ") VALUES (" + additionalInsertValuesSqlValues + Utilities.repeat("?", dbTableColumnsListToInsert.size(), ", ") + ")";
		} else {
			statementString = "INSERT INTO " + tableNameToUse + " (" + additionalInsertValuesSqlColumns + DbUtilities.joinColumnVendorEscaped(dbVendor, dbTableColumnsListToInsert) + ", " + itemIndexColumn + ") VALUES (" + additionalInsertValuesSqlValues + Utilities.repeat("?", dbTableColumnsListToInsert.size(), ", ") + ", ?)";
		}

		@SuppressWarnings("resource")
		PreparedStatement preparedStatement = null;
		try {
			preparedStatement = connection.prepareStatement(statementString);

			final int batchBlockSize = 1000;
			boolean hasUnexecutedData = false;

			Map<String, Object> itemData;
			while ((itemData = dataProvider.getNextItemData()) != null) {
				if (cancel) {
					break;
				}

				try {
					int i = 1;
					for (final String dbColumnToInsert : dbTableColumnsListToInsert) {
						final SimpleDataType simpleDataType = dbColumns.get(dbColumnToInsert).getSimpleDataType();
						final String unescapedDbColumnToInsert = DbUtilities.unescapeVendorReservedNames(dbVendor, dbColumnToInsert);
						final Object dataValue = itemData.get(mappingToUse.get(unescapedDbColumnToInsert).getFirst());
						final String formatInfo = mappingToUse.get(unescapedDbColumnToInsert).getSecond();

						@SuppressWarnings("resource")
						final Closeable itemToClose = setParameter(preparedStatement, i++, simpleDataType, dataValue, formatInfo);
						itemsToCloseAfterwards.add(itemToClose);
					}

					if (Utilities.isNotBlank(itemIndexColumn)) {
						// Add additional integer value to identify data item index
						setParameter(preparedStatement, i++, SimpleDataType.Integer, itemsDone + 1);
					}

					preparedStatement.addBatch();

					validItems++;
					showProgress();
				} catch (final Exception e) {
					invalidItems.add((int) itemsDone + 1);
					invalidItemsReasons.add(e.getClass().getSimpleName() + ": " + e.getMessage());
					if (commitOnFullSuccessOnly) {
						connection.rollback();
						throw new DbImportException(e.getClass().getSimpleName() + " error in item index " + (itemsDone + 1) + ": " + e.getMessage(), e);
					} else {
						if (dbVendor == DbVendor.SQLite) {
							// SQLite seems to not react on preparedStatement.clearParameters() calls
							for (int i = 1; i <= dbTableColumnsListToInsert.size(); i++) {
								preparedStatement.setObject(i, null);
							}
						} else {
							preparedStatement.clearParameters();
						}
					}
				}
				itemsDone++;

				if (validItems > 0) {
					if (validItems % batchBlockSize == 0) {
						final int[] results = preparedStatement.executeBatch();
						for (final Closeable itemToClose : itemsToCloseAfterwards) {
							Utilities.closeQuietly(itemToClose);
						}
						itemsToCloseAfterwards.clear();
						for (int i = 0; i < results.length; i++) {
							if (results[i] != 1 && results[i] != Statement.SUCCESS_NO_INFO) {
								invalidItems.add((int) (itemsDone - batchBlockSize) + i);
								invalidItemsReasons.add("Db import data error");
							}
						}
						if (!commitOnFullSuccessOnly) {
							connection.commit();
							if (dbVendor == DbVendor.Firebird) {
								preparedStatement.close();
								preparedStatement = connection.prepareStatement(statementString);
							}
						}
						hasUnexecutedData = false;
						showProgress();
					} else {
						hasUnexecutedData = true;
					}
				}
			}

			if (hasUnexecutedData) {
				final int[] results = preparedStatement.executeBatch();
				for (final Closeable itemToClose : itemsToCloseAfterwards) {
					Utilities.closeQuietly(itemToClose);
				}
				itemsToCloseAfterwards.clear();
				for (int i = 0; i < results.length; i++) {
					if (results[i] != 1 && results[i] != Statement.SUCCESS_NO_INFO) {
						invalidItems.add((int) (itemsDone - (itemsDone % batchBlockSize)) + i);
						invalidItemsReasons.add("Db import data error");
					}
				}
				if (!commitOnFullSuccessOnly) {
					connection.commit();
				}
			}

			if (commitOnFullSuccessOnly) {
				if (invalidItems.size() == 0) {
					connection.commit();
				} else {
					connection.rollback();
				}
			}

			dataItemsDone = itemsDone;
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

	protected Closeable setParameter(final PreparedStatement preparedStatement, final int columnIndex, final SimpleDataType simpleDataType, final Object dataValue, final String formatInfo) throws Exception {
		Closeable itemToCloseAfterwards = null;
		if (dataValue == null) {
			if (simpleDataType == SimpleDataType.String) {
				preparedStatement.setNull(columnIndex, java.sql.Types.VARCHAR);
			} else {
				preparedStatement.setNull(columnIndex, 0);
			}
		} else if (dataValue instanceof String && Utilities.isNotBlank(formatInfo)) {
			String valueString = (String) dataValue;

			if (".".equals(formatInfo)) {
				valueString = valueString.replace(",", "");
				if (valueString.contains(".")) {
					preparedStatement.setDouble(columnIndex, Double.parseDouble(valueString));
				} else {
					preparedStatement.setInt(columnIndex, Integer.parseInt(valueString));
				}
			} else if (",".equals(formatInfo)) {
				valueString = valueString.replace(".", "").replace(",", ".");
				if (valueString.contains(".")) {
					preparedStatement.setDouble(columnIndex, Double.parseDouble(valueString));
				} else {
					preparedStatement.setInt(columnIndex, Integer.parseInt(valueString));
				}
			} else if ("file".equalsIgnoreCase(formatInfo)) {
				if (!new File(valueString).exists()) {
					throw new Exception("File does not exist: " + valueString);
				} else if (simpleDataType == SimpleDataType.Blob) {
					if (dbVendor == DbVendor.SQLite) {
						// SQLite ignores "setBinaryStream"
						final byte[] data = Utilities.readFileToByteArray(new File(valueString));
						preparedStatement.setBytes(columnIndex, data);
					} else {
						itemToCloseAfterwards = new FileInputStream(valueString);
						preparedStatement.setBinaryStream(columnIndex, (FileInputStream) itemToCloseAfterwards);
					}
					importedDataAmount += new File(valueString).length();
				} else {
					if (dbVendor == DbVendor.SQLite || dbVendor == DbVendor.PostgreSQL) {
						// PostgreSQL and SQLite do not read the stream
						final byte[] data = Utilities.readFileToByteArray(new File(valueString));
						preparedStatement.setString(columnIndex, new String(data, textFileEncoding));
					} else {
						itemToCloseAfterwards = new InputStreamReader(new FileInputStream(valueString), textFileEncoding);
						preparedStatement.setCharacterStream(columnIndex, (InputStreamReader) itemToCloseAfterwards);
					}
					importedDataAmount += new File(valueString).length();
				}
			} else if ("lc".equalsIgnoreCase(formatInfo)) {
				valueString = valueString.toLowerCase();
				preparedStatement.setString(columnIndex, valueString);
			} else if ("uc".equalsIgnoreCase(formatInfo)) {
				valueString = valueString.toUpperCase();
				preparedStatement.setString(columnIndex, valueString);
			} else if ("email".equalsIgnoreCase(formatInfo)) {
				valueString = valueString.toLowerCase().trim();
				if (!NetworkUtilities.isValidEmail(valueString)) {
					throw new Exception("Invalid email address: " + valueString);
				}
				preparedStatement.setString(columnIndex, valueString);
			} else if (simpleDataType == SimpleDataType.DateTime) {
				final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(formatInfo);
				dateTimeFormatter.withResolverStyle(ResolverStyle.STRICT);
				dateTimeFormatter.withZone(importDataZoneId);
				final LocalDateTime localDateTimeValueFromData = LocalDateTime.parse(valueString.trim(), dateTimeFormatter);
				final LocalDateTime localDateTimeValueForDb = localDateTimeValueFromData.atZone(importDataZoneId).withZoneSameInstant(databaseZoneId).toLocalDateTime();
				preparedStatement.setTimestamp(columnIndex, Timestamp.valueOf(localDateTimeValueForDb));
			} else if (simpleDataType == SimpleDataType.Date) {
				final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(formatInfo);
				dateTimeFormatter.withResolverStyle(ResolverStyle.STRICT);
				dateTimeFormatter.withZone(importDataZoneId);
				final LocalDate localDateValue = LocalDate.parse(valueString.trim(), dateTimeFormatter);
				preparedStatement.setDate(columnIndex, java.sql.Date.valueOf(localDateValue));
			} else {
				throw new Exception("Unknown data type: " + simpleDataType);
			}
		} else if (dataValue instanceof String && simpleDataType == SimpleDataType.DateTime) {
			final String valueString = ((String) dataValue).trim();
			LocalDateTime localDateTimeValueForDb;
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
						localDateTimeValueForDb = LocalDate.parse(valueString.trim(), dateTimeFormatter).atTime(0, 0);
					} catch (@SuppressWarnings("unused") final DateTimeParseException e2) {
						try {
							final LocalDateTime localDateTimeValueFromData = DateUtilities.parseIso8601DateTimeString(valueString, importDataZoneId).toLocalDateTime();
							localDateTimeValueForDb = localDateTimeValueFromData.atZone(importDataZoneId).withZoneSameInstant(databaseZoneId).toLocalDateTime();
						} catch (@SuppressWarnings("unused") final DateTimeException e3) {
							final LocalDateTime localDateTimeValueFromData = DateUtilities.parseUnknownDateFormat(valueString);
							localDateTimeValueForDb = localDateTimeValueFromData.atZone(importDataZoneId).withZoneSameInstant(databaseZoneId).toLocalDateTime();
						}
					}
				}
			}
			preparedStatement.setTimestamp(columnIndex, Timestamp.valueOf(localDateTimeValueForDb));
		} else if (dataValue instanceof String && simpleDataType == SimpleDataType.Date) {
			final String valueString = ((String) dataValue).trim();
			LocalDateTime localDateTimeValueForDb;
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
						localDateTimeValueForDb = LocalDate.parse(valueString.trim(), dateTimeFormatter).atTime(0, 0);
					} catch (@SuppressWarnings("unused") final DateTimeParseException e2) {
						try {
							final LocalDateTime localDateTimeValueFromData = DateUtilities.parseIso8601DateTimeString(valueString, importDataZoneId).toLocalDateTime();
							localDateTimeValueForDb = localDateTimeValueFromData.atZone(importDataZoneId).withZoneSameInstant(databaseZoneId).toLocalDateTime();
						} catch (@SuppressWarnings("unused") final DateTimeException e3) {
							final LocalDateTime localDateTimeValueFromData = DateUtilities.parseUnknownDateFormat(valueString);
							localDateTimeValueForDb = localDateTimeValueFromData.atZone(importDataZoneId).withZoneSameInstant(databaseZoneId).toLocalDateTime();
						}
					}
				}
			}
			preparedStatement.setTimestamp(columnIndex, Timestamp.valueOf(localDateTimeValueForDb));
		} else {
			setParameter(preparedStatement, columnIndex, simpleDataType, dataValue);
		}
		return itemToCloseAfterwards;
	}

	protected static void setParameter(final PreparedStatement preparedStatement, final int columnIndex, final SimpleDataType simpleDataType, final Object dataValue) throws SQLException, Exception {
		if (dataValue == null) {
			if (simpleDataType == SimpleDataType.String) {
				preparedStatement.setNull(columnIndex, java.sql.Types.VARCHAR);
			} else {
				preparedStatement.setNull(columnIndex, 0);
			}
		} else if (dataValue instanceof String) {
			if (simpleDataType == SimpleDataType.Blob) {
				preparedStatement.setBytes(columnIndex, Utilities.decodeBase64((String) dataValue));
			} else if (simpleDataType == SimpleDataType.Double) {
				final String valueString = ((String) dataValue).trim();
				preparedStatement.setDouble(columnIndex, Double.parseDouble(valueString));
			} else if (simpleDataType == SimpleDataType.Integer) {
				final String valueString = ((String) dataValue).trim();
				if (valueString.contains(".")) {
					preparedStatement.setDouble(columnIndex, Double.parseDouble(valueString));
				} else {
					preparedStatement.setInt(columnIndex, Integer.parseInt(valueString));
				}
			} else if (simpleDataType == SimpleDataType.String || simpleDataType == SimpleDataType.Clob) {
				preparedStatement.setString(columnIndex, (String) dataValue);
			} else if (simpleDataType == SimpleDataType.DateTime) {
				throw new Exception("Date field to insert without mapping date format");
			} else if (simpleDataType == SimpleDataType.Date) {
				throw new Exception("Date field to insert without mapping date format");
			} else {
				throw new Exception("Unknown data type field to insert without mapping format");
			}
		} else if (simpleDataType == SimpleDataType.Double && dataValue instanceof Float) {
			// Keep the right precision when inserting a float value to a double column
			preparedStatement.setDouble(columnIndex, Double.parseDouble(dataValue.toString()));
		} else {
			preparedStatement.setObject(columnIndex, dataValue);
		}
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

	public static String convertMappingToString(final Map<String, Tuple<String, String>> mapping) {
		final StringBuilder returnValue = new StringBuilder();

		if (mapping != null) {
			for (final Entry<String, Tuple<String, String>> entry : mapping.entrySet()) {
				returnValue.append(entry.getKey() + "=\"" + entry.getValue().getFirst() + "\"");
				if (Utilities.isNotBlank(entry.getValue().getSecond())) {
					returnValue.append(" " + entry.getValue().getSecond());
				}
				returnValue.append("\n");
			}
		}

		return returnValue.toString().trim();
	}
}