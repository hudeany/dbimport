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
import de.soderer.json.JsonArray;
import de.soderer.json.JsonNode;
import de.soderer.json.JsonObject;
import de.soderer.json.JsonReader;
import de.soderer.network.NetworkUtilities;
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
import de.soderer.utilities.db.DbUtilities;
import de.soderer.utilities.db.data.DbColumnType;
import de.soderer.utilities.db.data.DbConnectionDefinition;
import de.soderer.utilities.db.data.DbSimpleDataType;
import de.soderer.utilities.db.data.DbVendor;
import de.soderer.utilities.db.exception.DbNotExistsException;
import de.soderer.utilities.worker.WorkerParentSimple;
import de.soderer.utilities.worker.WorkerSimple;
import de.soderer.zip.TarGzUtilities;
import de.soderer.zip.Zip4jUtilities;
import de.soderer.zip.ZipUtilities;

/**
 * Worker that imports the data items of a {@link DataProvider} into a database table.
 *
 * <p>
 * The data is written in batch updates. If a batch fails, it is repeated item by item ("single mode") to find
 * and skip the erroneous data items, unless this is prevented. Depending on the import and duplicate mode, existing
 * data is detected via the key columns and a temporary table.
 * </p>
 */
public class DbImportWorker extends WorkerSimple<Boolean> {
	// Mandatory parameters
	/**
	 * The connection parameters of the database.
	 */
	protected DbConnectionDefinition dbDefinition = null;
	/**
	 * The import mode.
	 */
	protected ImportMode importMode = ImportMode.INSERT;
	/**
	 * The duplicate mode.
	 */
	protected DuplicateMode duplicateMode = DuplicateMode.UPDATE_ALL_JOIN;
	/**
	 * The key columns (without functions), sorted by name.
	 */
	protected List<String> keyColumns = null;
	/**
	 * The key columns including their optional functions, e.g. "LOWER(email)", in the order of {@link #keyColumns}.
	 */
	protected List<String> keyColumnsWithFunctions = null;
	/**
	 * The table to import into.
	 */
	protected String tableName;
	/**
	 * Whether a missing table is created suitable for the import data.
	 */
	protected boolean createTableIfNotExists = false;
	/**
	 * The path of the JSON file describing the database structure for a new table, or null.
	 */
	protected String structureFilePath;
	/**
	 * Whether the data is only committed, if the whole import succeeds.
	 */
	protected boolean commitOnFullSuccessOnly = true;
	/**
	 * Whether a new index on the key columns is created, if it speeds up the import.
	 */
	protected boolean createNewIndexIfNeeded = true;
	/**
	 * Whether foreign key constraints are deactivated during the import.
	 */
	protected boolean deactivateForeignKeyConstraints = false;
	/**
	 * Whether triggers are deactivated during the import.
	 */
	protected boolean deactivateTriggers = false;
	/**
	 * The name of the index created by the import, or null.
	 */
	protected String newIndexName = null;

	/**
	 * The time zone of the database.
	 */
	protected ZoneId databaseZoneId = ZoneId.systemDefault();
	/**
	 * The time zone of the import data.
	 */
	protected ZoneId importDataZoneId = ZoneId.systemDefault();

	/**
	 * The database columns to fill, according to the mapping.
	 */
	protected List<String> dbTableColumnsListToInsert = null;
	/**
	 * The mapping of database columns to data properties (first) and their format info (second).
	 */
	protected Map<String, Tuple<String, String>> mapping = null;

	// Default optional parameters
	/**
	 * The log file, or null for no logging.
	 */
	protected File logFile = null;
	/**
	 * The character encoding of clob files.
	 */
	protected Charset textFileEncoding = StandardCharsets.UTF_8;
	/**
	 * Whether existing values are overwritten by null values of the import data.
	 */
	protected boolean updateWithNullValues = true;

	/**
	 * The number of read data items.
	 */
	protected long dataItemsDone = 0;
	/**
	 * The number of valid (imported) data items.
	 */
	protected long validItems = 0;
	/**
	 * The number of duplicate data items in the import data.
	 */
	protected long duplicatesItems = 0;
	/**
	 * The 1-based indices of the invalid data items.
	 */
	protected List<Integer> invalidItems = new ArrayList<>();
	/**
	 * The reasons of the invalid data items, in the order of {@link #invalidItems}.
	 */
	protected List<String> invalidItemsReasons = new ArrayList<>();
	/**
	 * The amount of imported data in bytes.
	 */
	protected long importedDataAmount = 0;
	/**
	 * The number of batch blocks, which were repeated item by item because of errors.
	 */
	protected long singleImportModeBlocks = 0;
	/**
	 * The number of deleted database rows (import mode CLEARINSERT).
	 */
	protected long deletedItems = 0;
	/**
	 * The number of inserted database rows.
	 */
	protected long insertedItems = 0;
	/**
	 * The number of updated database rows.
	 */
	protected long updatedItems = 0;
	/**
	 * The number of rows in the destination table after the import.
	 */
	protected long countItems = 0;
	/**
	 * The number of deleted duplicate database rows (duplicate modes MAKE_UNIQUE_*).
	 */
	protected long deletedDuplicatesInDB = 0;

	/**
	 * Whether the data is only analysed (e.g. to create a mapping) without importing it.
	 */
	protected boolean analyseDataOnly = false;
	/**
	 * The property names of the import data, filled by the data analysis.
	 */
	protected List<String> availableDataPropertyNames = null;

	/**
	 * Additional values for inserted items, or null.
	 */
	protected String additionalInsertValues = null;
	/**
	 * Additional values for updated items, or null.
	 */
	protected String additionalUpdateValues = null;

	/**
	 * Whether erroneous data items are written to a file next to the import file.
	 */
	protected boolean logErroneousData = false;
	/**
	 * The file containing the erroneous data items, or null.
	 */
	protected File erroneousDataFile = null;

	/**
	 * The fallback date format pattern, or null.
	 */
	protected String dateFormatPattern = null;
	/**
	 * The fallback date time format pattern, or null.
	 */
	protected String dateTimeFormatPattern = null;

	private DateTimeFormatter dateFormatterCache = null;
	private DateTimeFormatter dateTimeFormatterCache = null;

	private int batchBlockSize = 1000;
	private boolean preventBatchFallbackToSingleLineOnErrors = false;

	/**
	 * The reader of the import data.
	 */
	protected DataProvider dataProvider = null;

	/**
	 * Creates the worker. The data provider and the import options are set via the setter methods.
	 *
	 * @param parent parent to signal the progress to
	 * @param dbDefinition the connection parameters of the database
	 * @param tableName the table to import into
	 * @param dateFormatPattern fallback date format pattern, which is overridden by mapping formats, or null
	 * @param dateTimeFormatPattern fallback date time format pattern, which is overridden by mapping formats, or null
	 */
	public DbImportWorker(final WorkerParentSimple parent, final DbConnectionDefinition dbDefinition, final String tableName, final String dateFormatPattern, final String dateTimeFormatPattern) {
		super(parent);

		this.dbDefinition = dbDefinition;
		this.tableName = tableName;
		this.dateFormatPattern = dateFormatPattern;
		this.dateTimeFormatPattern = dateTimeFormatPattern;
	}

	/**
	 * Sets whether the data is only analysed (e.g. to create a mapping) without importing it (default: false).
	 *
	 * @param analyseDataOnly true if the data is only analysed (e.g. to create a mapping) without importing it
	 */
	public void setAnalyseDataOnly(final boolean analyseDataOnly) {
		this.analyseDataOnly = analyseDataOnly;
	}

	/**
	 * Sets the log file. Null means no logging.
	 *
	 * @param logFile the log file
	 */
	public void setLogFile(final File logFile) {
		this.logFile = logFile;
	}

	/**
	 * Sets the character encoding of clob files. Default is UTF-8.
	 *
	 * @param textFileEncoding the character encoding of clob files
	 */
	public void setTextFileEncoding(final Charset textFileEncoding) {
		this.textFileEncoding = textFileEncoding;
	}

	/**
	 * Sets the reader of the import data.
	 *
	 * @param dataProvider the reader of the import data
	 */
	public void setDataProvider(final DataProvider dataProvider) {
		this.dataProvider = dataProvider;
	}

	/**
	 * Sets the time zone of the database.
	 *
	 * @param databaseTimeZone the time zone ID, e.g. "Europe/Berlin"
	 */
	public void setDatabaseTimeZone(final String databaseTimeZone) {
		databaseZoneId = ZoneId.of(databaseTimeZone);
	}

	/**
	 * Sets the time zone of the import data.
	 *
	 * @param importDataTimeZone the time zone ID, e.g. "Europe/Berlin"
	 */
	public void setImportDataTimeZone(final String importDataTimeZone) {
		importDataZoneId = ZoneId.of(importDataTimeZone);
	}

	/**
	 * Sets the number of data items per batch update. Default is 1000.
	 *
	 * @param batchBlockSize the number of data items per batch update
	 */
	public void setBatchBlockSize(final int batchBlockSize) {
		this.batchBlockSize = batchBlockSize;
	}

	/**
	 * Sets whether a failed batch update is not repeated item by item to find the erroneous items (default: false).
	 *
	 * @param preventBatchFallbackToSingleLineOnErrors true if a failed batch update is not repeated item by item to find the erroneous items
	 */
	public void setPreventBatchFallbackToSingleLineOnErrors(final boolean preventBatchFallbackToSingleLineOnErrors) {
		this.preventBatchFallbackToSingleLineOnErrors = preventBatchFallbackToSingleLineOnErrors;
	}

	/**
	 * Sets the mapping of database columns to data properties.
	 *
	 * @param mappingString the mapping (see {@link DbImportMappingDialog#parseMappingString(String)}), blank for automapping by name
	 * @throws IOException if the mapping cannot be read
	 * @throws Exception if a mapping line is invalid
	 */
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

	/**
	 * Returns the mapping of database columns to data properties. Without a configured mapping, each data property
	 * is mapped to the database column with the same name in lower case.
	 *
	 * @return the mapping by database column, with the data property as first and the format info as second value
	 * @throws Exception if the data properties cannot be read
	 */
	public Map<String, Tuple<String, String>> getMapping() throws Exception {
		if (mapping == null) {
			mapping = new HashMap<>();
			for (final String propertyName : dataProvider.getAvailableDataPropertyNames()) {
				// Columns without name (e.g. Excel columns without header) cannot be mapped
				if (propertyName != null) {
					mapping.put(propertyName.toLowerCase(Locale.ROOT), new Tuple<>(propertyName, ""));
				}
			}
			dbTableColumnsListToInsert = new ArrayList<>();
			for (final String dbColumn : mapping.keySet()) {
				dbTableColumnsListToInsert.add(DbUtilities.unescapeVendorReservedNames(dbDefinition.getDbVendor(), dbColumn));
			}
		}
		return mapping;
	}

	/**
	 * Checks the mapping against the columns of the destination table and the data properties. Without a configured
	 * mapping, a mapping of the columns and data properties with equal names (case insensitive) is created.
	 *
	 * @param dbColumns the columns of the destination table
	 * @throws Exception if the data properties cannot be read
	 * @throws DbImportException if a mapped column or data property does not exist, or a key column is not mapped
	 */
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

	/**
	 * Sets the import mode. Default is INSERT.
	 *
	 * @param importMode the import mode
	 */
	public void setImportMode(final ImportMode importMode) {
		this.importMode = importMode;
	}

	/**
	 * Sets the duplicate mode (default: UPDATE_ALL_JOIN).
	 *
	 * @param duplicateMode the duplicate mode
	 * @throws Exception if the duplicate mode is not supported by this worker (see {@link DbNoSqlImportWorker})
	 */
	public void setDuplicateMode(final DuplicateMode duplicateMode) throws Exception {
		this.duplicateMode = duplicateMode;
	}

	/**
	 * Sets the key columns used to detect existing data and duplicates. A key column may contain a function,
	 * e.g. "LOWER(email)".
	 *
	 * @param keyColumnList the key columns, or null for none
	 */
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

	/**
	 * Sets whether the data is only committed, if the whole import succeeds (default: true).
	 *
	 * @param commitOnFullSuccessOnly true if the data is only committed, if the whole import succeeds
	 * @throws Exception if the value is not supported by this worker (see {@link DbNoSqlImportWorker})
	 */
	public void setCompleteCommit(final boolean commitOnFullSuccessOnly) throws Exception {
		this.commitOnFullSuccessOnly = commitOnFullSuccessOnly;
	}

	/**
	 * Sets whether a new index on the key columns is created, if it speeds up the import (default: true).
	 *
	 * @param createNewIndexIfNeeded true if a new index on the key columns is created, if it speeds up the import
	 */
	public void setCreateNewIndexIfNeeded(final boolean createNewIndexIfNeeded) {
		this.createNewIndexIfNeeded = createNewIndexIfNeeded;
	}

	/**
	 * Sets whether foreign key constraints are deactivated during the import (default: false).
	 *
	 * @param deactivateForeignKeyConstraints true if foreign key constraints are deactivated during the import
	 */
	public void setDeactivateForeignKeyConstraints(final boolean deactivateForeignKeyConstraints) {
		this.deactivateForeignKeyConstraints = deactivateForeignKeyConstraints;
	}

	/**
	 * Sets whether triggers are deactivated during the import (default: false).
	 *
	 * @param deactivateTriggers true if triggers are deactivated during the import
	 */
	public void setDeactivateTriggers(final boolean deactivateTriggers) {
		this.deactivateTriggers = deactivateTriggers;
	}

	/**
	 * Sets additional values for inserted items, which are not part of the import data. Semicolon separated, e.g. "id=test_seq.NEXTVAL;flag='abc'".
	 *
	 * @param additionalInsertValues additional values for inserted items, which are not part of the import data
	 */
	public void setAdditionalInsertValues(final String additionalInsertValues) {
		this.additionalInsertValues = additionalInsertValues;
	}

	/**
	 * Sets additional values for updated items, which are not part of the import data. Semicolon separated, e.g. "changed=current_timestamp;flag='abc'".
	 *
	 * @param additionalUpdateValues additional values for updated items, which are not part of the import data
	 */
	public void setAdditionalUpdateValues(final String additionalUpdateValues) {
		this.additionalUpdateValues = additionalUpdateValues;
	}

	/**
	 * Sets whether existing values are overwritten by null values of the import data (default: true).
	 *
	 * @param updateWithNullValues true if existing values are overwritten by null values of the import data
	 * @throws Exception if the value is not supported by this worker (see {@link DbNoSqlImportWorker})
	 */
	public void setUpdateNullData(final boolean updateWithNullValues) throws Exception {
		this.updateWithNullValues = updateWithNullValues;
	}

	/**
	 * Sets whether a missing table is created suitable for the import data (default: false).
	 *
	 * @param createTableIfNotExists true if a missing table is created suitable for the import data
	 */
	public void setCreateTableIfNotExists(final boolean createTableIfNotExists) {
		this.createTableIfNotExists = createTableIfNotExists;
	}

	/**
	 * Sets the path of the JSON file describing the database structure for a new table. Null means, that the table is created from the scanned data types.
	 *
	 * @param structureFilePath the path of the JSON file describing the database structure for a new table
	 */
	public void setStructureFilePath(final String structureFilePath) {
		this.structureFilePath = structureFilePath;
	}

	/**
	 * Sets whether erroneous data items are written to a file next to the import file (default: false).
	 *
	 * @param logErroneousData true if erroneous data items are written to a file next to the import file
	 */
	public void setLogErroneousData(final boolean logErroneousData) {
		this.logErroneousData = logErroneousData;
	}

	/**
	 * Executes the import (or only the data analysis).
	 *
	 * @return true if the import was not canceled
	 * @throws Exception if the data cannot be read, the configuration is invalid or the database access fails
	 */
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
					} else if (duplicateMode == DuplicateMode.CHECK_SOURCE_ONLY_DROP || duplicateMode == DuplicateMode.UPDATE_FIRST_DROP || duplicateMode == DuplicateMode.UPDATE_ALL_DROP || duplicateMode == DuplicateMode.MAKE_UNIQUE_DROP) {
						duplicatesItems = DbUtilities.dropDuplicates(connection, tempTableName, keyColumns);
					} else if (duplicateMode == DuplicateMode.CHECK_SOURCE_ONLY_JOIN || duplicateMode == DuplicateMode.UPDATE_FIRST_JOIN || duplicateMode == DuplicateMode.UPDATE_ALL_JOIN || duplicateMode == DuplicateMode.MAKE_UNIQUE_JOIN) {
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
						if (duplicateMode == DuplicateMode.NO_CHECK || duplicateMode == DuplicateMode.CHECK_SOURCE_ONLY_DROP || duplicateMode == DuplicateMode.CHECK_SOURCE_ONLY_JOIN) {
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
						if (duplicateMode == DuplicateMode.NO_CHECK || duplicateMode == DuplicateMode.CHECK_SOURCE_ONLY_DROP || duplicateMode == DuplicateMode.CHECK_SOURCE_ONLY_JOIN) {
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
						if (duplicateMode == DuplicateMode.NO_CHECK || duplicateMode == DuplicateMode.CHECK_SOURCE_ONLY_DROP || duplicateMode == DuplicateMode.CHECK_SOURCE_ONLY_JOIN) {
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
						// Not closed: closing the PrintStream would also close the log file, so the later log writes
						// in the finally block would fail and hide this error behind a "Stream Closed" IOException
						final PrintStream printStream = new PrintStream(logOutputStream);
						sqle.printStackTrace(printStream);
						printStream.flush();
					}
				} catch (final Exception e1) {
					e1.printStackTrace();
				}
				throw new DbImportException("SQL error: " + sqle.getMessage());
			} catch (final Exception e) {
				try {
					logToFile(logOutputStream, "Error: " + e.getMessage());
					if (logOutputStream != null) {
						// Not closed, see above
						final PrintStream printStream = new PrintStream(logOutputStream);
						e.printStackTrace(printStream);
						printStream.flush();
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
				if (connection != null) {
					DbUtilities.dropTableIfExists(connection, tempTableName);
				}

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

	/**
	 * Returns the configuration of this import for the import log.
	 *
	 * @return the configuration as text with one line per parameter
	 * @throws Exception if the mapping cannot be determined
	 */
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

	/**
	 * Creates the destination table, if it does not exist and the creation is activated. The table is created from the
	 * structure file or from the scanned data types of the import data.
	 *
	 * @param connection the database connection
	 * @param tableNameToUse the name of the table
	 * @param keyColumnsToUse the key columns for the primary key of a new table, or null
	 * @return true if the table was created
	 * @throws Exception if the data cannot be scanned
	 * @throws DbImportException if the table cannot be created
	 * @throws SQLException if the database access fails
	 */
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

			final JsonObject dbStructureJsonObject = (JsonObject) dbStructureJsonNode;

			itemsToDo = dbStructureJsonObject.size();
			itemsUnitSign = null;
			itemsDone = 0;

			JsonObject foundTableJsonObject = null;
			for (final Entry<String, Object> tableEntry : dbStructureJsonObject) {
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
			if (tableJsonObject.containsKey("keycolumns")) {
				keyColumnsToSet = ((JsonArray) tableJsonObject.get("keycolumns")).items().stream().map(String.class::cast).collect(Collectors.toList());
			}

			String primaryKeyPart = "";
			if (keyColumnsToSet != null && Utilities.isNotEmpty(keyColumnsToSet)) {
				primaryKeyPart = ", PRIMARY KEY (" + DbUtilities.joinColumnVendorEscaped(dbDefinition.getDbVendor(), keyColumnsToSet) + ")";
			}
			final String sqlStatementString = "CREATE TABLE " + tableNameToCreate + " (" + columnsPart + primaryKeyPart + ")";
			statement.execute(sqlStatementString);
			if (dbDefinition.getDbVendor() == DbVendor.Derby || dbDefinition.getDbVendor() == DbVendor.MsSQL) {
				connection.commit();
			}
			return true;
		}
	}

	private String getColumnNameAndType(final JsonObject columnJsonObject) throws Exception {
		final String name = DbUtilities.escapeVendorReservedNames(dbDefinition.getDbVendor(), (String) columnJsonObject.getSimpleValue("name"));
		final DbSimpleDataType simpleDataType = DbSimpleDataType.getSimpleDataTypeByName((String) columnJsonObject.getSimpleValue("datatype"));
		int characterByteSize = -1;
		if (columnJsonObject.containsKey("datasize")) {
			// Via Number, the JSON reader does not necessarily return an Integer
			characterByteSize = ((Number) columnJsonObject.getSimpleValue("datasize")).intValue();
		}

		Object defaultvalue = null;
		if (columnJsonObject.containsKey("defaultvalue")) {
			defaultvalue = columnJsonObject.get("defaultvalue");
		}
		String defaultvaluePart = "";
		if (defaultvalue != null) {
			if (simpleDataType == DbSimpleDataType.String) {
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

		if (simpleDataType == DbSimpleDataType.String && characterByteSize < 0) {
			// Fallback for extremely large VARCHAR in PostgreSQL. Maybe better use CLOB
			characterByteSize = 4000;
		}

		return name + " " + DbUtilities.getDataType(dbDefinition.getDbVendor(), simpleDataType) + (characterByteSize > -1 ? "(" + characterByteSize + ")" : "") + defaultvaluePart;
	}

	/**
	 * Returns the statistics of the import, e.g. the numbers of valid, invalid, inserted and updated items.
	 *
	 * @return the statistics as text
	 */
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
						final DbSimpleDataType simpleDataType = dbColumns.get(dbColumnToInsert).getSimpleDataType();
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
					// This item was not added to the JDBC batch, so it must not stay in the values used by the
					// single item fallback mode (it would be inserted there with partly stale parameter values)
					batchValues.remove(batchValues.size() - 1);
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
									invalidItems.add((int) (dataItemsDone - batchBlockSize) + i + 1);
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
							invalidItems.add((int) (dataItemsDone - (dataItemsDone % batchBlockSize)) + i + 1);
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

	/**
	 * Converts a data value to the data type of its column and sets it as parameter of the statement.
	 *
	 * @param preparedStatement the statement
	 * @param columnIndex the 1-based parameter index
	 * @param columnName the name of the column (for error messages)
	 * @param simpleDataType the data type of the column
	 * @param isNullable whether the column accepts null values
	 * @param dataValue the data value
	 * @param formatInfo the format info of the mapping (e.g. a date pattern, decimal separator, "file", "lc", "uc" or "email"), or null
	 * @param batchValueItem list, to which the converted value is added (to repeat the item in single mode)
	 * @param maxBlobSize maximum size of blob data in bytes, or a negative value for unlimited
	 * @return a stream, which must be closed after the statement is executed (e.g. for file data), or null
	 * @throws Exception if the value is invalid for the column
	 */
	protected Closeable validateAndSetParameter(
			final PreparedStatement preparedStatement,
			final int columnIndex,
			final String columnName,
			final DbSimpleDataType simpleDataType,
			final boolean isNullable,
			final Object dataValue,
			final String formatInfo,
			final List<Object> batchValueItem,
			final long maxBlobSize) throws Exception {
		Closeable itemToCloseAfterwards = null;
		if (dataValue == null) {
			if (!isNullable) {
				if (simpleDataType == DbSimpleDataType.String) {
					// If String column is not nullable, but data was an empty string which, was interpreted as null, then an empty String is used
					if (dbDefinition.getDbVendor() == DbVendor.SQLite || dbDefinition.getDbVendor() == DbVendor.Derby || dbDefinition.getDbVendor() == DbVendor.PostgreSQL) {
						preparedStatement.setString(columnIndex, "");
					} else {
						preparedStatement.setNString(columnIndex, "");
					}
					batchValueItem.add("");
				} else {
					throw new DbImportException("Column '" + columnName + "' is not nullable but receives null value");
				}
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
					} else if (simpleDataType == DbSimpleDataType.Integer) {
						int value;
						try {
							value = Integer.parseInt(valueString);
						} catch (@SuppressWarnings("unused") final NumberFormatException e) {
							throw new DbImportException("Invalid value for integer column '" + columnName + "': " + valueString);
						}
						preparedStatement.setInt(columnIndex, value);
						batchValueItem.add(value);
					} else if (simpleDataType == DbSimpleDataType.BigInteger) {
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
					} else if (simpleDataType == DbSimpleDataType.Integer) {
						int value;
						try {
							value = Integer.parseInt(valueString);
						} catch (@SuppressWarnings("unused") final NumberFormatException e) {
							throw new DbImportException("Invalid value for integer column '" + columnName + "': " + valueString);
						}
						preparedStatement.setInt(columnIndex, value);
						batchValueItem.add(value);
					} else if (simpleDataType == DbSimpleDataType.BigInteger) {
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
						} else {
							throw new Exception("File size is too big for current database settings for column '" + columnName + "': " + new File(valueString).length());
						}
					} else if (simpleDataType == DbSimpleDataType.Blob) {
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
							preparedStatement.setString(columnIndex, new String(data, textFileEncoding));
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
					if (dbDefinition.getDbVendor() == DbVendor.SQLite || dbDefinition.getDbVendor() == DbVendor.Derby || dbDefinition.getDbVendor() == DbVendor.PostgreSQL) {
						preparedStatement.setString(columnIndex, valueString);
					} else {
						preparedStatement.setNString(columnIndex, valueString);
					}
					batchValueItem.add(valueString);
				} else if ("uc".equalsIgnoreCase(formatInfo)) {
					valueString = valueString.toUpperCase();
					if (dbDefinition.getDbVendor() == DbVendor.SQLite || dbDefinition.getDbVendor() == DbVendor.Derby || dbDefinition.getDbVendor() == DbVendor.PostgreSQL) {
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
					if (dbDefinition.getDbVendor() == DbVendor.SQLite || dbDefinition.getDbVendor() == DbVendor.Derby || dbDefinition.getDbVendor() == DbVendor.PostgreSQL) {
						preparedStatement.setString(columnIndex, valueString);
					} else {
						preparedStatement.setNString(columnIndex, valueString);
					}
					batchValueItem.add(valueString);
				} else if (simpleDataType == DbSimpleDataType.DateTime) {
					DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(formatInfo.replace("yyyy", "uuuu"));
					dateTimeFormatter = dateTimeFormatter.withResolverStyle(ResolverStyle.STRICT);
					dateTimeFormatter = dateTimeFormatter.withZone(importDataZoneId);
					final LocalDateTime localDateTimeValueFromData = LocalDateTime.parse(valueString.trim(), dateTimeFormatter);
					final LocalDateTime localDateTimeValueForDb = localDateTimeValueFromData.atZone(importDataZoneId).withZoneSameInstant(databaseZoneId).toLocalDateTime();
					final Timestamp value = Timestamp.valueOf(localDateTimeValueForDb);
					preparedStatement.setTimestamp(columnIndex, value);
					batchValueItem.add(value);
				} else if (simpleDataType == DbSimpleDataType.Date) {
					DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(formatInfo.replace("yyyy", "uuuu"));
					dateTimeFormatter = dateTimeFormatter.withResolverStyle(ResolverStyle.STRICT);
					dateTimeFormatter = dateTimeFormatter.withZone(importDataZoneId);
					LocalDate localDateValue;
					try {
						localDateValue = LocalDate.parse(valueString.trim(), dateTimeFormatter);
					} catch (@SuppressWarnings("unused") final DateTimeParseException e2) {
						// The configured format may itself capture a trailing time-of-day. Under strict resolving,
						// extracting a plain LocalDate then fails even though the pattern matched the whole string,
						// because leftover resolved time fields are not silently discarded. Retry with the same
						// pattern as a LocalDateTime and just take the date part, instead of failing.
						localDateValue = LocalDateTime.parse(valueString.trim(), dateTimeFormatter).toLocalDate();
					}
					final java.sql.Date value = java.sql.Date.valueOf(localDateValue);
					preparedStatement.setDate(columnIndex, value);
					batchValueItem.add(value);
				} else {
					throw new DbImportException("Unknown data type for column '" + columnName + "': " + simpleDataType);
				}
			} else if (simpleDataType == DbSimpleDataType.DateTime) {
				final String valueString = ((String) dataValue).trim();
				LocalDateTime localDateTimeValueForDb;
				if (Utilities.isBlank(valueString)) {
					if (!isNullable) {
						throw new DbImportException("Column '" + columnName + "' is not nullable but receives null value");
					} else {
						setNullParameter(preparedStatement, columnIndex, DbSimpleDataType.DateTime);
						batchValueItem.add(null);
					}
				} else {
					if (Utilities.isNotBlank(dateTimeFormatPattern)) {
						final LocalDateTime localDateTimeValueFromData = LocalDateTime.parse(valueString.trim(), getConfiguredDateTimeFormatter());
						localDateTimeValueForDb = localDateTimeValueFromData.atZone(importDataZoneId).withZoneSameInstant(databaseZoneId).toLocalDateTime();
					} else {
						try {
							final DateTimeFormatter dateTimeFormatter = DateUtilities.getDateTimeFormatterWithSeconds(Locale.getDefault(), importDataZoneId);
							final LocalDateTime localDateTimeValueFromData = LocalDateTime.parse(valueString.trim(), dateTimeFormatter);
							localDateTimeValueForDb = localDateTimeValueFromData.atZone(importDataZoneId).withZoneSameInstant(databaseZoneId).toLocalDateTime();
						} catch (@SuppressWarnings("unused") final DateTimeParseException e) {
							try {
								final DateTimeFormatter dateTimeFormatter = DateUtilities.getDateTimeFormatter(Locale.getDefault(), importDataZoneId);
								final LocalDateTime localDateTimeValueFromData = LocalDateTime.parse(valueString.trim(), dateTimeFormatter);
								localDateTimeValueForDb = localDateTimeValueFromData.atZone(importDataZoneId).withZoneSameInstant(databaseZoneId).toLocalDateTime();
							} catch (@SuppressWarnings("unused") final DateTimeParseException e1) {
								try {
									final DateTimeFormatter dateTimeFormatter = DateUtilities.getDateFormatter(Locale.getDefault());
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
			} else if (simpleDataType == DbSimpleDataType.Date) {
				final String valueString = ((String) dataValue).trim();
				LocalDateTime localDateTimeValueForDb;
				if (Utilities.isBlank(valueString)) {
					if (!isNullable) {
						throw new DbImportException("Column '" + columnName + "' is not nullable but receives null value");
					} else {
						setNullParameter(preparedStatement, columnIndex, DbSimpleDataType.DateTime);
						batchValueItem.add(null);
					}
				} else {
					if (Utilities.isNotBlank(dateFormatPattern)) {
						try {
							LocalDate parsedLocalDate;
							try {
								parsedLocalDate = LocalDate.parse(valueString.trim(), getConfiguredDateFormatter());
							} catch (@SuppressWarnings("unused") final DateTimeParseException e2) {
								// The configured date pattern may itself capture a trailing time-of-day (e.g. because
								// the same pattern is also used for datetime-like source data). Under strict resolving,
								// extracting a plain LocalDate then fails even though the pattern matched the whole
								// string, because leftover resolved time fields are not silently discarded. Retry with
								// the same pattern as a LocalDateTime and just take the date part, instead of failing.
								parsedLocalDate = LocalDateTime.parse(valueString.trim(), getConfiguredDateFormatter()).toLocalDate();
							}
							localDateTimeValueForDb = parsedLocalDate.atStartOfDay();
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
							final DateTimeFormatter dateTimeFormatter = DateUtilities.getDateTimeFormatterWithSeconds(Locale.getDefault(), importDataZoneId);
							final LocalDateTime localDateTimeValueFromData = LocalDateTime.parse(valueString.trim(), dateTimeFormatter);
							localDateTimeValueForDb = localDateTimeValueFromData.atZone(importDataZoneId).withZoneSameInstant(databaseZoneId).toLocalDateTime();
						} catch (@SuppressWarnings("unused") final DateTimeParseException e) {
							try {
								final DateTimeFormatter dateTimeFormatter = DateUtilities.getDateTimeFormatter(Locale.getDefault(), importDataZoneId);
								final LocalDateTime localDateTimeValueFromData = LocalDateTime.parse(valueString.trim(), dateTimeFormatter);
								localDateTimeValueForDb = localDateTimeValueFromData.atZone(importDataZoneId).withZoneSameInstant(databaseZoneId).toLocalDateTime();
							} catch (@SuppressWarnings("unused") final DateTimeParseException e1) {
								try {
									final DateTimeFormatter dateTimeFormatter = DateUtilities.getDateFormatter(Locale.getDefault());
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
			} else if (simpleDataType == DbSimpleDataType.Blob) {
				final byte[] value = Utilities.decodeBase64((String) dataValue);
				preparedStatement.setBytes(columnIndex, value);
				batchValueItem.add(value);
			} else if (simpleDataType == DbSimpleDataType.Float) {
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
			} else if (simpleDataType == DbSimpleDataType.Integer) {
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
			} else if (simpleDataType == DbSimpleDataType.BigInteger) {
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
			} else if (simpleDataType == DbSimpleDataType.String || simpleDataType == DbSimpleDataType.Clob) {
				if (dbDefinition.getDbVendor() == DbVendor.SQLite || dbDefinition.getDbVendor() == DbVendor.Derby || dbDefinition.getDbVendor() == DbVendor.PostgreSQL) {
					preparedStatement.setString(columnIndex, (String) dataValue);
				} else {
					preparedStatement.setNString(columnIndex, (String) dataValue);
				}
				batchValueItem.add(dataValue);
			} else if (simpleDataType == DbSimpleDataType.Boolean) {
				preparedStatement.setBoolean(columnIndex, Utilities.interpretAsBool((String) dataValue));
				batchValueItem.add(dataValue);
			} else if (simpleDataType == DbSimpleDataType.DateTime) {
				throw new DbImportException("Date field to insert without mapping date format for column '" + columnName + "'");
			} else if (simpleDataType == DbSimpleDataType.Date) {
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
			final LocalDateTime localDateTimeValueForDb = ((LocalDateTime) dataValue).atZone(importDataZoneId).withZoneSameInstant(databaseZoneId).toLocalDateTime();
			final Timestamp value = Timestamp.valueOf(localDateTimeValueForDb);
			preparedStatement.setTimestamp(columnIndex, value);
			batchValueItem.add(value);
		} else if (dataValue instanceof Number && simpleDataType == DbSimpleDataType.Float) {
			// Keep the right precision when inserting a float value to a double column
			double value;
			try {
				value = Double.parseDouble(dataValue.toString());
			} catch (@SuppressWarnings("unused") final NumberFormatException e) {
				throw new DbImportException("Invalid value for numeric column '" + columnName + "': " + dataValue.toString());
			}
			preparedStatement.setDouble(columnIndex, value);
			batchValueItem.add(value);
		} else if (dataValue instanceof MonthDay && simpleDataType == DbSimpleDataType.String) {
			final String value = dataValue.toString();
			if (dbDefinition.getDbVendor() == DbVendor.SQLite || dbDefinition.getDbVendor() == DbVendor.Derby || dbDefinition.getDbVendor() == DbVendor.PostgreSQL) {
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

	private void setNullParameter(final PreparedStatement preparedStatement, final int columnIndex, final DbSimpleDataType simpleDataType) throws SQLException {
		if (simpleDataType == DbSimpleDataType.String) {
			preparedStatement.setNull(columnIndex, java.sql.Types.VARCHAR);
		} else if (dbDefinition.getDbVendor() == DbVendor.Oracle) {
			if (simpleDataType == DbSimpleDataType.Date) {
				preparedStatement.setNull(columnIndex, java.sql.Types.TIMESTAMP);
			} else if (simpleDataType == DbSimpleDataType.DateTime) {
				preparedStatement.setNull(columnIndex, java.sql.Types.TIMESTAMP);
			} else if (simpleDataType == DbSimpleDataType.Float) {
				preparedStatement.setNull(columnIndex, java.sql.Types.DOUBLE);
			} else if (simpleDataType == DbSimpleDataType.Integer) {
				preparedStatement.setNull(columnIndex, java.sql.Types.INTEGER);
			} else if (simpleDataType == DbSimpleDataType.BigInteger) {
				preparedStatement.setNull(columnIndex, java.sql.Types.BIGINT);
			} else if (simpleDataType == DbSimpleDataType.Clob) {
				preparedStatement.setNull(columnIndex, java.sql.Types.CLOB);
			} else if (simpleDataType == DbSimpleDataType.Blob) {
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
			DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(dateTimeFormatPattern.replace("yyyy", "uuuu"));
			dateTimeFormatter = dateTimeFormatter.withResolverStyle(ResolverStyle.STRICT);
			dateTimeFormatter = dateTimeFormatter.withZone(importDataZoneId);
			dateTimeFormatterCache = dateTimeFormatter;
		}
		return dateTimeFormatterCache;
	}

	private DateTimeFormatter getConfiguredDateFormatter() {
		if (dateFormatterCache == null) {
			DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern(dateFormatPattern.replace("yyyy", "uuuu"));
			dateFormatter = dateFormatter.withResolverStyle(ResolverStyle.STRICT);
			dateFormatter = dateFormatter.withZone(importDataZoneId);
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
			boolean entryCanBeExecuted = true;
			for (int i = 0; i < batchValueEntry.size(); i++) {
				final Object dataValue = batchValueEntry.get(i);
				if (dataValue instanceof Closeable) {
					// Streams and readers (file data) were already consumed by the failed batch and cannot be sent again
					entryCanBeExecuted = false;
					break;
				}
				if (dataValue != null
						&& !(dataValue instanceof Timestamp)
						&& !(dataValue instanceof java.sql.Date)
						&& !(dataValue instanceof Date)
						&& !(dataValue instanceof Number)
						&& !(dataValue instanceof Boolean)
						&& !(dataValue instanceof byte[])
						&& !(dataValue instanceof String)) {
					throw new Exception("Unexpected datatype: " + dataValue.getClass().getSimpleName());
				}
				if (dataValue instanceof byte[]) {
					preparedStatement.setBytes(i + 1, (byte[]) dataValue);
				} else if (dataValue != null && dataValue instanceof String) {
					if (dbDefinition.getDbVendor() == DbVendor.SQLite || dbDefinition.getDbVendor() == DbVendor.Derby || dbDefinition.getDbVendor() == DbVendor.PostgreSQL) {
						preparedStatement.setString(i + 1, (String) dataValue);
					} else {
						preparedStatement.setNString(i + 1, (String) dataValue);
					}
				} else {
					preparedStatement.setObject(i + 1, dataValue);
				}
			}
			if (!entryCanBeExecuted) {
				notImportedBySingleMode++;
				invalidItems.add((int) startingDataEntryIndex + entryIndex + 1);
				invalidItemsReasons.add("DB import data error: File data cannot be sent again in single item mode");
				continue;
			}
			try {
				preparedStatement.execute();
				connection.commit();
			} catch (final SQLException e) {
				connection.rollback();
				notImportedBySingleMode++;
				preparedStatement.clearBatch();
				invalidItems.add((int) startingDataEntryIndex + entryIndex + 1);
				invalidItemsReasons.add("DB import data error: " + e.getMessage());
			}
		}
		return notImportedBySingleMode;
	}

	/**
	 * Writes a message as a line to the log.
	 *
	 * @param logOutputStream the log stream, or null for no logging
	 * @param message the message
	 * @throws Exception if the log cannot be written
	 */
	protected static void logToFile(final OutputStream logOutputStream, final String message) throws Exception {
		if (logOutputStream != null) {
			logOutputStream.write((message.trim() + "\n").getBytes(StandardCharsets.UTF_8));
		}
	}

	/**
	 * Returns the number of deleted database rows (import mode CLEARINSERT).
	 *
	 * @return the number of deleted database rows
	 */
	public long getDeletedItems() {
		return deletedItems;
	}

	/**
	 * Returns the number of updated database rows.
	 *
	 * @return the number of updated database rows
	 */
	public long getUpdatedItems() {
		return updatedItems;
	}

	/**
	 * Returns the number of valid (imported) data items.
	 *
	 * @return the number of valid data items
	 */
	public long getImportedItems() {
		return validItems;
	}

	/**
	 * Returns the name of the index created by the import.
	 *
	 * @return the index name, or null if no index was created
	 */
	public String getCreatedNewIndexName() {
		return newIndexName;
	}

	/**
	 * Returns the indices of the invalid data items.
	 *
	 * @return the 1-based indices of the invalid data items
	 */
	public List<Integer> getNotImportedItems() {
		return invalidItems;
	}

	/**
	 * Returns the reasons of the invalid data items.
	 *
	 * @return the reasons, in the order of {@link #getNotImportedItems()}
	 */
	public List<String> getNotImportedItemsReasons() {
		return invalidItemsReasons;
	}

	/**
	 * Returns the number of read data items.
	 *
	 * @return the number of read data items
	 */
	public long getImportedDataItems() {
		return dataItemsDone;
	}

	/**
	 * Returns the amount of imported data.
	 *
	 * @return the amount of imported data in bytes
	 */
	public long getImportedDataAmount() {
		return importedDataAmount;
	}

	/**
	 * Returns the number of duplicate data items in the import data.
	 *
	 * @return the number of duplicate data items
	 */
	public long getIgnoredDuplicates() {
		return duplicatesItems;
	}

	/**
	 * Returns the number of inserted database rows.
	 *
	 * @return the number of inserted database rows
	 */
	public long getInsertedItems() {
		return insertedItems;
	}

	/**
	 * Returns the property names of the import data, which are determined by the data analysis.
	 *
	 * @return the property names, or null if no data analysis was done
	 */
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

	/**
	 * Returns the result text of the import.
	 *
	 * @return the statistics of the import (see {@link #getResultStatistics()})
	 */
	@Override
	public String getResultText() {
		return getResultStatistics();
	}
}
