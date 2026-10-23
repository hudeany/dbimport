package de.soderer.dbimport;

import java.io.File;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;

import de.soderer.dbimport.dataprovider.CsvDataProvider;
import de.soderer.dbimport.dataprovider.DataProvider;
import de.soderer.dbimport.dataprovider.ExcelDataProvider;
import de.soderer.dbimport.dataprovider.JsonDataProvider;
import de.soderer.dbimport.dataprovider.KdbxDataProvider;
import de.soderer.dbimport.dataprovider.OdsDataProvider;
import de.soderer.dbimport.dataprovider.VcfDataProvider;
import de.soderer.dbimport.dataprovider.XmlDataProvider;
import de.soderer.dbimport.dataprovider.YamlDataProvider;
import de.soderer.utilities.DateUtilities;
import de.soderer.utilities.FileDataType;
import de.soderer.utilities.FileType;
import de.soderer.utilities.FileUtilities;
import de.soderer.utilities.Utilities;
import de.soderer.utilities.db.data.DbConnectionDefinition;
import de.soderer.utilities.db.data.DbVendor;
import de.soderer.utilities.db.exception.DbDefinitionException;
import de.soderer.utilities.worker.WorkerParentSimple;

/**
 * All parameters of a data import ("import"), as given on the command line, in the console menu or in the GUI.
 *
 * <p>
 * {@link #getConfiguredWorker(WorkerParentSimple, boolean, String, String)} creates the import worker for
 * these parameters.
 * </p>
 */
public class DbImportDefinition extends DbConnectionDefinition {
	/**
	 * Supported import data formats.
	 */
	public enum DataType {
		/**
		 * Comma (or other character) separated values
		 */
		CSV,
		/**
		 * JSON array of objects
		 */
		JSON,
		/**
		 * YAML list of objects
		 */
		YAML,
		/**
		 * XML elements
		 */
		XML,
		/**
		 * SQL script, whose statements are executed
		 */
		SQL,
		/**
		 * Excel spreadsheet (XLS or XLSX)
		 */
		EXCEL,
		/**
		 * OpenDocument spreadsheet
		 */
		ODS,
		/**
		 * vCard file
		 */
		VCF,
		/**
		 * KeePass database file
		 */
		KDBX;

		/**
		 * Returns the data type for its name.
		 *
		 * @param dataTypeString the name of the data type (case insensitive)
		 * @return the data type
		 * @throws RuntimeException if there is no data type with this name
		 */
		public static DataType getFromString(final String dataTypeString) {
			for (final DataType dataType : DataType.values()) {
				if (dataType.toString().equalsIgnoreCase(dataTypeString)) {
					return dataType;
				}
			}
			throw new RuntimeException("Invalid data type: " + dataTypeString);
		}
	}

	/**
	 * Import modes, which define how the import data is applied to the existing data.
	 */
	public enum ImportMode {
		/**
		 * Deletes all existing data before the insert. If key columns are set, duplicates are prevented.
		 */
		CLEARINSERT,
		/**
		 * Inserts all data (default). If key columns are set, only data not already existing is inserted.
		 */
		INSERT,
		/**
		 * Updates only existing data, needs key columns.
		 */
		UPDATE,
		/**
		 * Updates existing and inserts new data, needs key columns.
		 */
		UPSERT;

		/**
		 * Returns the import mode for its name.
		 *
		 * @param importModeString the name of the import mode (case insensitive)
		 * @return the import mode
		 * @throws Exception if there is no import mode with this name
		 */
		public static ImportMode getFromString(final String importModeString) throws Exception {
			for (final ImportMode importMode : ImportMode.values()) {
				if (importMode.toString().equalsIgnoreCase(importModeString)) {
					return importMode;
				}
			}
			throw new DbImportException("Invalid import mode: " + importModeString);
		}
	}

	/**
	 * Duplicate modes, which define how data items with the same key column values are handled.
	 *
	 * <p>
	 * "DROP" means, that only the first of duplicate import data items is used, "JOIN" means, that the values of
	 * duplicate import data items are combined.
	 * </p>
	 */
	public enum DuplicateMode {
		/**
		 * No duplicate check. Only insert.
		 */
		NO_CHECK,
		/**
		 * Check duplicates in import data only. Drop duplicates.
		 */
		CHECK_SOURCE_ONLY_DROP,
		/**
		 * Check duplicates in import data only. Join duplicates.
		 */
		CHECK_SOURCE_ONLY_JOIN,
		/**
		 * Check duplicates in import data and database data. Only update the first occurrence in the database. Drop duplicates.
		 */
		UPDATE_FIRST_DROP,
		/**
		 * Check duplicates in import data and database data. Only update the first occurrence in the database. Join duplicates.
		 */
		UPDATE_FIRST_JOIN,
		/**
		 * Check duplicates in import data and database data. Update all occurrences in the database. Drop duplicates.
		 */
		UPDATE_ALL_DROP,
		/**
		 * Check duplicates in import data and database data. Update all occurrences in the database. Join duplicates (default).
		 */
		UPDATE_ALL_JOIN,
		/**
		 * Check duplicates in import data and database data. Remove duplicate occurrences in the database. Drop duplicates.
		 */
		MAKE_UNIQUE_DROP,
		/**
		 * Check duplicates in import data and database data. Remove duplicate occurrences in the database. Join duplicates.
		 */
		MAKE_UNIQUE_JOIN;

		/**
		 * Returns the duplicate mode for its name.
		 *
		 * @param duplicateModeString the name of the duplicate mode (case insensitive)
		 * @return the duplicate mode
		 * @throws Exception if there is no duplicate mode with this name
		 */
		public static DuplicateMode getFromString(final String duplicateModeString) throws Exception {
			for (final DuplicateMode duplicateMode : DuplicateMode.values()) {
				if (duplicateMode.toString().equalsIgnoreCase(duplicateModeString)) {
					return duplicateMode;
				}
			}
			throw new DbImportException("Invalid duplicate mode: " + duplicateModeString);
		}
	}

	// Mandatory parameters

	/** The tableName. */
	private String tableName = "*";

	/** The importFilePath or data. */
	private String importFilePathOrData = null;

	private boolean isInlineData = false;

	// Default optional parameters

	/** The data type. */
	private DataType dataType = null;

	/** Log activation. */
	private boolean log = false;

	/** The verbose. */
	private boolean verbose = false;

	/** The encoding. */
	private Charset encoding = StandardCharsets.UTF_8;

	/** The separator. */
	private char separator = ';';

	/** The string quote. */
	private Character stringQuote = '"';

	/** The escape string quote character. */
	private char escapeStringQuote = '"';

	/** Interpret escape sequences (e.g. \n, \t) in csv string values, instead of treating them as plain text. */
	private boolean interpretEscapeSequences = true;

	/** The no headers. */
	private boolean noHeaders = false;

	/** The null value string. */
	private String nullValueString = "";

	private boolean completeCommit = false;

	private boolean allowUnderfilledLines = false;

	private boolean removeSurplusEmptyTrailingColumns = false;

	private ImportMode importMode = ImportMode.INSERT;

	private DuplicateMode duplicateMode = DuplicateMode.UPDATE_ALL_JOIN;

	private boolean updateNullData = true;

	private List<String> keycolumns = null;

	private boolean createTable = false;

	private String structureFilePath = null;

	private boolean trimData = false;

	private String mapping = "";

	private String additionalInsertValues = null;

	private String additionalUpdateValues = null;

	private boolean logErroneousData = false;

	private boolean createNewIndexIfNeeded = true;

	private boolean deactivateForeignKeyConstraints = false;

	private boolean deactivateTriggers = false;

	private String dataPath = null;

	private String schemaFilePath = null;

	private char[] zipPassword = null;

	private char[] kdbxPassword = null;

	private String databaseTimeZone = TimeZone.getDefault().getID();

	private String importDataTimeZone = TimeZone.getDefault().getID();

	private String dateFormat = null;

	private String dateTimeFormat = null;

	private int batchBlockSize = 1000;

	private boolean preventBatchFallbackToSingleLineOnErrors = false;

	/**
	 * Creates a definition with the default values, the parameters are set via the setter methods.
	 */
	public DbImportDefinition() {
		// Nothing to initialize
	}

	/**
	 * Returns the name of the table to import into.
	 *
	 * @return the table name, "*" means a table named like each import file
	 */
	public String getTableName() {
		return tableName;
	}

	/**
	 * Sets the name of the table to import into. "*" means a table named like each import file.
	 *
	 * @param tableName the name of the table to import into
	 */
	public void setTableName(final String tableName) {
		this.tableName = tableName;
	}

	/**
	 * Returns the path of the import file (may contain the wildcards "?" and "*") or the inline data.
	 *
	 * @return the import file path or the inline data
	 */
	public String getImportFilePathOrData() {
		return importFilePathOrData;
	}

	/**
	 * Returns whether the import data is given as text instead of a file path.
	 *
	 * @return true if the import data is given as text instead of a file path
	 */
	public boolean isInlineData() {
		return isInlineData;
	}

	/**
	 * Sets whether the import data is given as text instead of a file path (default: false).
	 *
	 * @param isInlineData true if the import data is given as text instead of a file path
	 */
	public void setInlineData(final boolean isInlineData) {
		this.isInlineData = isInlineData;
	}

	/**
	 * Sets the path of the import file or the inline data.
	 *
	 * <p>
	 * A file path may contain the wildcards "?" and "*" in its file name to import several files. A leading "~" is
	 * replaced by the user's home directory. A text, which is no valid file path, is used as inline data.
	 * </p>
	 *
	 * @param importFilePathOrData the import file path or the inline data
	 * @param checkForExistingFiles true to check, that the directory exists and contains matching files (wildcard paths only)
	 * @throws DbImportException if the path contains wildcards in its directory part or there are no matching files
	 */
	public void setImportFilePathOrData(final String importFilePathOrData, final boolean checkForExistingFiles) throws DbImportException {
		this.importFilePathOrData = importFilePathOrData;
		if (this.importFilePathOrData != null && !isInlineData) {
			// Check filepath syntax
			if (getImportFilePathOrData().contains("?") || getImportFilePathOrData().contains("*")) {
				final int lastSeparator = Math.max(getImportFilePathOrData().lastIndexOf("/"), getImportFilePathOrData().lastIndexOf("\\"));
				// A pattern without directory (e.g. "*.csv") refers to the current directory
				String directoryPath = lastSeparator < 0 ? "." : getImportFilePathOrData().substring(0, lastSeparator);
				directoryPath = Utilities.replaceUsersHome(directoryPath);
				final String filePattern = getImportFilePathOrData().substring(lastSeparator + 1);
				if (directoryPath.contains("?") || directoryPath.contains("*")) {
					throw new DbImportException("Import directory path contains wildcards, but wildcards only allowed for filenames: " + (directoryPath));
				} else if (checkForExistingFiles) {
					if (!new File(directoryPath).exists()) {
						throw new DbImportException("Import path does not exist: " + (directoryPath));
					} else if (!new File((directoryPath)).isDirectory()) {
						throw new DbImportException("Import path is not a directory: " + (directoryPath));
					} else {
						if (FileUtilities.getFilesByPattern(new File(directoryPath), filePattern.replace(".", "\\.").replace("?", ".").replace("*", ".*"), false).size() == 0) {
							throw new DbImportException("Import file pattern has no matching files: " + (directoryPath));
						}
					}
				}
			} else {
				try {
					Paths.get(importFilePathOrData);
					isInlineData = false;
				} catch (@SuppressWarnings("unused") final Exception e) {
					isInlineData = true;
				}
				if (!isInlineData) {
					this.importFilePathOrData = Utilities.replaceUsersHome(this.importFilePathOrData.trim());
					if (this.importFilePathOrData.endsWith(File.separator)) {
						this.importFilePathOrData = this.importFilePathOrData.substring(0, this.importFilePathOrData.length() - 1);
					}
					isInlineData = false;
				}
			}
		}
	}

	/**
	 * Returns the format of the import data.
	 *
	 * @return the data format, or null to detect it by the file extension
	 */
	public DataType getDataType() {
		return dataType;
	}

	/**
	 * Sets the format of the import data. Null means, that it is detected by the file extension (CSV, if it cannot be detected).
	 *
	 * @param dataType the format of the import data
	 */
	public void setDataType(final DataType dataType) {
		this.dataType = dataType;
	}

	/**
	 * Returns whether the import information is logged in a log file.
	 *
	 * @return true if the import information is logged in a log file
	 */
	public boolean isLog() {
		return log;
	}

	/**
	 * Sets whether the import information is logged in a log file in the subdirectory "importlogs" (default: false).
	 *
	 * @param log true if the import information is logged in a log file in the subdirectory "importlogs"
	 */
	public void setLog(final boolean log) {
		this.log = log;
	}

	/**
	 * Returns whether the progress is shown on the console.
	 *
	 * @return true if the progress is shown on the console
	 */
	public boolean isVerbose() {
		return verbose;
	}

	/**
	 * Sets whether the progress is shown on the console (default: false).
	 *
	 * @param verbose true if the progress is shown on the console
	 */
	public void setVerbose(final boolean verbose) {
		this.verbose = verbose;
	}

	/**
	 * Returns the character encoding of CSV, JSON and YAML data and of clob files.
	 *
	 * @return the character encoding
	 */
	public Charset getEncoding() {
		return encoding;
	}

	/**
	 * Sets the character encoding of CSV, JSON and YAML data and of clob files. Default is UTF-8.
	 *
	 * @param encoding the character encoding of CSV, JSON and YAML data and of clob files
	 */
	public void setEncoding(final Charset encoding) {
		this.encoding = encoding;
	}

	/**
	 * Returns the CSV value separator.
	 *
	 * @return the CSV value separator
	 */
	public char getSeparator() {
		return separator;
	}

	/**
	 * Sets the CSV value separator. Default is ';'.
	 *
	 * @param separator the CSV value separator
	 */
	public void setSeparator(final char separator) {
		this.separator = separator;
	}

	/**
	 * Returns the CSV string quote character.
	 *
	 * @return the CSV string quote character, or null for none
	 */
	public Character getStringQuote() {
		return stringQuote;
	}

	/**
	 * Sets the CSV string quote character. Null means none, default is '"'.
	 *
	 * @param stringQuote the CSV string quote character
	 */
	public void setStringQuote(final Character stringQuote) {
		this.stringQuote = stringQuote;
	}

	/**
	 * Returns the character to escape the CSV string quote within quoted values.
	 *
	 * @return the escape character for the CSV string quote
	 */
	public char getEscapeStringQuote() {
		return escapeStringQuote;
	}

	/**
	 * Sets the character to escape the CSV string quote within quoted values. Default is '"'.
	 *
	 * @param escapeStringQuote the character to escape the CSV string quote within quoted values
	 */
	public void setEscapeStringQuote(final char escapeStringQuote) {
		this.escapeStringQuote = escapeStringQuote;
	}

	/**
	 * Returns whether escape sequences like \n or \t in CSV string values are interpreted.
	 *
	 * @return true if escape sequences like \n or \t in CSV string values are interpreted
	 */
	public boolean isInterpretEscapeSequences() {
		return interpretEscapeSequences;
	}

	/**
	 * Sets whether escape sequences like \n or \t in CSV string values are interpreted instead of imported as plain text (default: true).
	 *
	 * @param interpretEscapeSequences true if escape sequences like \n or \t in CSV string values are interpreted instead of imported as plain text
	 */
	public void setInterpretEscapeSequences(final boolean interpretEscapeSequences) {
		this.interpretEscapeSequences = interpretEscapeSequences;
	}

	/**
	 * Returns whether the first CSV line contains data instead of the column names.
	 *
	 * @return true if the first CSV line contains data instead of the column names
	 */
	public boolean isNoHeaders() {
		return noHeaders;
	}

	/**
	 * Sets whether the first CSV line contains data instead of the column names (default: false).
	 *
	 * @param noHeaders true if the first CSV line contains data instead of the column names
	 */
	public void setNoHeaders(final boolean noHeaders) {
		this.noHeaders = noHeaders;
	}

	/**
	 * Returns the text, which represents null values in CSV, XML, Excel and ODS data.
	 *
	 * @return the text for null values
	 */
	public String getNullValueString() {
		return nullValueString;
	}

	/**
	 * Sets the text, which represents null values in CSV, XML, Excel and ODS data. Default is an empty text.
	 *
	 * @param nullValueString the text, which represents null values in CSV, XML, Excel and ODS data
	 */
	public void setNullValueString(final String nullValueString) {
		this.nullValueString = nullValueString;
	}

	/**
	 * Returns whether the data is only committed, if the whole import succeeds.
	 *
	 * @return true if the data is only committed, if the whole import succeeds
	 */
	public boolean isCompleteCommit() {
		return completeCommit;
	}

	/**
	 * Sets whether the data is only committed, if the whole import succeeds (rollback on any error) (default: false).
	 *
	 * @param completeCommit true if the data is only committed, if the whole import succeeds (rollback on any error)
	 */
	public void setCompleteCommit(final boolean completeCommit) {
		this.completeCommit = completeCommit;
	}

	/**
	 * Returns whether CSV lines with fewer values than columns are accepted.
	 *
	 * @return true if CSV lines with fewer values than columns are accepted
	 */
	public boolean isAllowUnderfilledLines() {
		return allowUnderfilledLines;
	}

	/**
	 * Sets whether CSV lines with fewer values than columns are accepted (default: false).
	 *
	 * @param allowUnderfilledLines true if CSV lines with fewer values than columns are accepted
	 */
	public void setAllowUnderfilledLines(final boolean allowUnderfilledLines) {
		this.allowUnderfilledLines = allowUnderfilledLines;
	}

	/**
	 * Returns whether empty surplus values at the end of CSV lines are ignored.
	 *
	 * @return true if empty surplus values at the end of CSV lines are ignored
	 */
	public boolean isRemoveSurplusEmptyTrailingColumns() {
		return removeSurplusEmptyTrailingColumns;
	}

	/**
	 * Sets whether empty surplus values at the end of CSV lines are ignored (default: false).
	 *
	 * @param removeSurplusEmptyTrailingColumns true if empty surplus values at the end of CSV lines are ignored
	 */
	public void setRemoveSurplusEmptyTrailingColumns(final boolean removeSurplusEmptyTrailingColumns) {
		this.removeSurplusEmptyTrailingColumns = removeSurplusEmptyTrailingColumns;
	}

	/**
	 * Returns the import mode.
	 *
	 * @return the import mode
	 */
	public ImportMode getImportMode() {
		return importMode;
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
	 * Returns the duplicate mode.
	 *
	 * @return the duplicate mode
	 */
	public DuplicateMode getDuplicateMode() {
		return duplicateMode;
	}

	/**
	 * Sets the duplicate mode. Default is UPDATE_ALL_JOIN.
	 *
	 * @param duplicateMode the duplicate mode
	 */
	public void setDuplicateMode(final DuplicateMode duplicateMode) {
		this.duplicateMode = duplicateMode;
	}

	/**
	 * Returns whether existing values are overwritten by null values of the import data.
	 *
	 * @return true if existing values are overwritten by null values of the import data
	 */
	public boolean isUpdateNullData() {
		return updateNullData;
	}

	/**
	 * Sets whether existing values are overwritten by null values of the import data (default: true).
	 *
	 * @param updateNullData true if existing values are overwritten by null values of the import data
	 */
	public void setUpdateNullData(final boolean updateNullData) {
		this.updateNullData = updateNullData;
	}

	/**
	 * Returns the key columns used to detect existing data and duplicates.
	 *
	 * @return the key columns, or null
	 */
	public List<String> getKeycolumns() {
		return keycolumns;
	}

	/**
	 * Sets the key columns used to detect existing data and duplicates.
	 *
	 * @param keycolumns the key columns used to detect existing data and duplicates
	 */
	public void setKeycolumns(final List<String> keycolumns) {
		this.keycolumns = keycolumns;
	}

	/**
	 * Returns whether a missing table (and SQLite, Derby or HSQL database) is created suitable for the import data.
	 *
	 * @return true if a missing table (and SQLite, Derby or HSQL database) is created suitable for the import data
	 */
	public boolean isCreateTable() {
		return createTable;
	}

	/**
	 * Sets whether a missing table (and SQLite, Derby or HSQL database) is created suitable for the import data (default: false).
	 *
	 * @param createTable true if a missing table (and SQLite, Derby or HSQL database) is created suitable for the import data
	 */
	public void setCreateTable(final boolean createTable) {
		this.createTable = createTable;
	}

	/**
	 * Returns the path of the JSON file describing the database structure to create.
	 *
	 * @return the structure file path, or null
	 */
	public String getStructureFilePath() {
		return structureFilePath;
	}

	/**
	 * Sets the path of the JSON file describing the database structure to create. Only used together with "create table".
	 *
	 * @param structureFilePath the path of the JSON file describing the database structure to create
	 */
	public void setStructureFilePath(final String structureFilePath) {
		this.structureFilePath = structureFilePath;
	}

	/**
	 * Returns the mapping of database columns to data properties.
	 *
	 * @return the mapping (see {@link DbImportMappingDialog#parseMappingString(String)}), empty for automapping by name
	 */
	public String getMapping() {
		return mapping;
	}

	/**
	 * Sets the mapping of database columns to data properties. Empty means automapping by name (see {@link DbImportMappingDialog#parseMappingString(String)}).
	 *
	 * @param mapping the mapping of database columns to data properties
	 */
	public void setMapping(final String mapping) {
		this.mapping = mapping;
	}

	/**
	 * Returns whether the data values are trimmed.
	 *
	 * @return true if the data values are trimmed
	 */
	public boolean isTrimData() {
		return trimData;
	}

	/**
	 * Sets whether the data values are trimmed (default: false).
	 *
	 * @param trimData true if the data values are trimmed
	 */
	public void setTrimData(final boolean trimData) {
		this.trimData = trimData;
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
	 * Returns the additional values for inserted items.
	 *
	 * @return the additional insert values, or null
	 */
	public String getAdditionalInsertValues() {
		return additionalInsertValues;
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
	 * Returns the additional values for updated items.
	 *
	 * @return the additional update values, or null
	 */
	public String getAdditionalUpdateValues() {
		return additionalUpdateValues;
	}

	/**
	 * Returns whether erroneous data items are written to a file next to the import file.
	 *
	 * @return true if erroneous data items are written to a file next to the import file
	 */
	public boolean isLogErroneousData() {
		return logErroneousData;
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
	 * Returns whether a new index on the key columns is created, if it speeds up the import.
	 *
	 * @return true if a new index on the key columns is created, if it speeds up the import
	 */
	public boolean isCreateNewIndexIfNeeded() {
		return createNewIndexIfNeeded;
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
	 * Returns whether foreign key constraints are deactivated during the import.
	 *
	 * @return true if foreign key constraints are deactivated during the import
	 */
	public boolean isDeactivateForeignKeyConstraints() {
		return deactivateForeignKeyConstraints;
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
	 * Returns whether triggers are deactivated during the import.
	 *
	 * @return true if triggers are deactivated during the import
	 */
	public boolean isDeactivateTriggers() {
		return deactivateTriggers;
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
	 * Returns the path of the data within XML (XPath), JSON or YAML data, or the sheet name of Excel or ODS data.
	 *
	 * @return the data path, or null
	 */
	public String getDataPath() {
		return dataPath;
	}

	/**
	 * Sets the path of the data within XML (XPath), JSON or YAML data, or the sheet name of Excel or ODS data.
	 *
	 * @param dataPath the path of the data within XML (XPath), JSON or YAML data, or the sheet name of Excel or ODS data
	 */
	public void setDataPath(final String dataPath) {
		this.dataPath = dataPath;
	}

	/**
	 * Returns the path of the schema file to validate XML (XSD) or JSON data.
	 *
	 * @return the schema file path, or null
	 */
	public String getSchemaFilePath() {
		return schemaFilePath;
	}

	/**
	 * Sets the path of the schema file to validate XML (XSD) or JSON data.
	 *
	 * @param schemaFilePath the path of the schema file to validate XML (XSD) or JSON data
	 */
	public void setSchemaFilePath(final String schemaFilePath) {
		this.schemaFilePath = schemaFilePath;
	}

	/**
	 * Sets the password of a zip import file.
	 *
	 * @param zipPassword the password of a zip import file
	 */
	public void setZipPassword(final char[] zipPassword) {
		this.zipPassword = zipPassword;
	}

	/**
	 * Returns the password of a zip import file.
	 *
	 * @return the zip password, or null
	 */
	public char[] getZipPassword() {
		return zipPassword;
	}

	/**
	 * Sets the password of a KeePass import file.
	 *
	 * @param kdbxPassword the password of a KeePass import file
	 */
	public void setKdbxPassword(final char[] kdbxPassword) {
		this.kdbxPassword = kdbxPassword;
	}

	/**
	 * Returns the password of a KeePass import file.
	 *
	 * @return the KeePass password, or null
	 */
	public char[] getKdbxPassword() {
		return kdbxPassword;
	}

	/**
	 * Sets the time zone of the database. Null means the system's default time zone.
	 *
	 * @param databaseTimeZone the time zone of the database
	 */
	public void setDatabaseTimeZone(final String databaseTimeZone) {
		this.databaseTimeZone = databaseTimeZone;
		if (this.databaseTimeZone == null) {
			this.databaseTimeZone = TimeZone.getDefault().getID();
		}
	}

	/**
	 * Returns the time zone of the database.
	 *
	 * @return the time zone ID, e.g. "Europe/Berlin"
	 */
	public String getDatabaseTimeZone() {
		return databaseTimeZone;
	}

	/**
	 * Sets the time zone of the import data. Null means the system's default time zone.
	 *
	 * @param importDataTimeZone the time zone of the import data
	 */
	public void setImportDataTimeZone(final String importDataTimeZone) {
		this.importDataTimeZone = importDataTimeZone;
		if (this.importDataTimeZone == null) {
			this.importDataTimeZone = TimeZone.getDefault().getID();
		}
	}

	/**
	 * Returns the time zone of the import data.
	 *
	 * @return the time zone ID, e.g. "Europe/Berlin"
	 */
	public String getImportDataTimeZone() {
		return importDataTimeZone;
	}

	/**
	 * Returns the fallback date format pattern.
	 *
	 * @return the date format pattern, or null
	 */
	public String getDateFormat() {
		return dateFormat;
	}

	/**
	 * Sets the fallback date format pattern (Java format characters), which is overridden by mapping formats.
	 *
	 * @param dateFormat the fallback date format pattern (Java format characters), which is overridden by mapping formats
	 */
	public void setDateFormat(final String dateFormat) {
		this.dateFormat = dateFormat;
	}

	/**
	 * Returns the fallback date time format pattern.
	 *
	 * @return the date time format pattern, or null
	 */
	public String getDateTimeFormat() {
		return dateTimeFormat;
	}

	/**
	 * Sets the fallback date time format pattern (Java format characters), which is overridden by mapping formats.
	 *
	 * @param dateTimeFormat the fallback date time format pattern (Java format characters), which is overridden by mapping formats
	 */
	public void setDateTimeFormat(final String dateTimeFormat) {
		this.dateTimeFormat = dateTimeFormat;
	}

	/**
	 * Returns the number of data items per batch update.
	 *
	 * @return the batch block size
	 */
	public int getBatchBlockSize() {
		return batchBlockSize;
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
	 * Returns whether a failed batch update is not repeated item by item to find the erroneous items.
	 *
	 * @return true if a failed batch update is not repeated item by item to find the erroneous items
	 */
	public boolean isPreventBatchFallbackToSingleLineOnErrors() {
		return preventBatchFallbackToSingleLineOnErrors;
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
	 * Checks the parameters and supplies the database driver (it may be asked for interactively, if it is missing).
	 *
	 * @throws Exception if the database driver is not available, or a parameter is missing, invalid or not
	 *         supported for the data format or database vendor
	 */
	@Override
	public void checkParameters() throws Exception {
		if (getDbVendor() != null) {
			try {
				if (!new DbDriverSupplier(null, getDbVendor()).supplyDriver(DbImport.APPLICATION_NAME, DbImport.CONFIGURATION_FILE)) {
					throw new DbDefinitionException("Cannot acquire database driver for database vendor: " + getDbVendor());
				}
			} catch (final Exception e) {
				throw new DbDefinitionException("Cannot acquire database driver for database vendor: " + getDbVendor(), e);
			}
		}

		super.checkParameters();

		if (importFilePathOrData == null) {
			throw new DbImportException("ImportFilePath or data is missing");
		} else if (!isInlineData) {
			if (getDataType() == null) {
				final FileType fileType = FileUtilities.getFileTypeByExtension(getImportFilePathOrData());
				if (fileType.getFileDataType() == FileDataType.CSV) {
					setDataType(DataType.CSV);
				} else if (fileType.getFileDataType() == FileDataType.JSON) {
					setDataType(DataType.JSON);
				} else if (fileType.getFileDataType() == FileDataType.YAML) {
					setDataType(DataType.YAML);
				} else if (fileType.getFileDataType() == FileDataType.EXCEL) {
					setDataType(DataType.EXCEL);
				} else if (fileType.getFileDataType() == FileDataType.ODS) {
					setDataType(DataType.ODS);
				} else if (fileType.getFileDataType() == FileDataType.XML) {
					setDataType(DataType.XML);
				} else if (fileType.getFileDataType() == FileDataType.SQL) {
					setDataType(DataType.SQL);
				} else if (fileType.getFileDataType() == FileDataType.VCF) {
					setDataType(DataType.VCF);
				} else if (fileType.getFileDataType() == FileDataType.KDBX) {
					setDataType(DataType.KDBX);
				} else {
					setDataType(DataType.CSV);
				}
			}

			if (getImportFilePathOrData().contains("?") || getImportFilePathOrData().contains("*")) {
				final int lastSeparator = Math.max(getImportFilePathOrData().lastIndexOf("/"), getImportFilePathOrData().lastIndexOf("\\"));
				// A pattern without directory (e.g. "*.csv") refers to the current directory
				String directoryPath = lastSeparator < 0 ? "." : getImportFilePathOrData().substring(0, lastSeparator);
				directoryPath = Utilities.replaceUsersHome(directoryPath);
				final String filePattern = getImportFilePathOrData().substring(lastSeparator + 1);
				if (directoryPath.contains("?") || directoryPath.contains("*")) {
					throw new DbImportException("Import directory path contains wildcards, but wildcards only allowed for filenames: " + (directoryPath));
				} else if (!new File(directoryPath).exists()) {
					throw new DbImportException("Import path does not exist: " + (directoryPath));
				} else if (!new File((directoryPath)).isDirectory()) {
					throw new DbImportException("Import path is not a directory: " + (directoryPath));
				} else {
					if (FileUtilities.getFilesByPattern(new File(directoryPath), filePattern.replace(".", "\\.").replace("?", ".").replace("*", ".*"), false).size() == 0) {
						throw new DbImportException("Import file pattern has no matching files: " + (directoryPath));
					}
				}
			} else {
				if (!new File(importFilePathOrData).exists()) {
					throw new DbImportException("ImportFilePath does not exist: " + importFilePathOrData);
				} else if (!new File(importFilePathOrData).isFile()) {
					throw new DbImportException("ImportFilePath is not a file: " + importFilePathOrData);
				}
			}
		}

		if (noHeaders && dataType != DataType.CSV && dataType != DataType.EXCEL && dataType != DataType.ODS && dataType != DataType.VCF) {
			throw new DbImportException("NoHeaders is not supported for data format " + dataType);
		}

		if (dataType != DataType.SQL) {
			if ((importMode == ImportMode.UPDATE || importMode == ImportMode.UPSERT)
					&& (keycolumns == null || keycolumns.isEmpty())) {
				throw new DbImportException("Invalid empty key column definition for import mode: " + importMode);
			}
		}

		// Without explicit data type (detection by file extension) the data type is not known yet, so the check is skipped.
		// Before, every data path was rejected in this case (e.g. GUI default "by file extension").
		if (Utilities.isNotEmpty(dataPath) && dataType != null && dataType != DataType.XML && dataType != DataType.JSON && dataType != DataType.YAML && dataType != DataType.EXCEL && dataType != DataType.ODS) {
			throw new DbImportException("DataPath is not supported for data format " + dataType);
		}

		if (Utilities.isNotEmpty(schemaFilePath) && dataType != null && dataType != DataType.XML && dataType != DataType.JSON) {
			throw new DbImportException("SchemaFilePath is not supported for data format " + dataType);
		}

		if (batchBlockSize < 1) {
			throw new DbImportException("Batch blocksize must be at least 1, but was " + batchBlockSize);
		}
	}

	/**
	 * Creates and configures a worker according to the current configuration.
	 *
	 * @param parent parent to signal the progress to
	 * @param analyseDataOnly true to only analyse the data (e.g. to create a mapping) without importing it
	 * @param tableNameForImport the table to import into
	 * @param importFileOrData path of a single import file (no wildcards), or the inline data
	 * @return the configured worker
	 * @throws Exception if the data provider or worker cannot be created or a parameter is invalid
	 */
	public DbImportWorker getConfiguredWorker(final WorkerParentSimple parent, final boolean analyseDataOnly, final String tableNameForImport, final String importFileOrData) throws Exception {
		DbImportWorker worker;
		if (getDbVendor() == DbVendor.Cassandra) {
			worker = new DbNoSqlImportWorker(parent,
					this,
					tableNameForImport);
		} else {
			worker = new DbImportWorker(parent,
					this,
					tableNameForImport,
					getDateFormat(),
					getDateTimeFormat());
		}

		DataType dataTypeToExecute = getDataType();
		if (dataTypeToExecute == null) {
			switch(FileUtilities.getFileTypeByExtension(importFileOrData).getFileDataType()) {
				case CSV:
					dataTypeToExecute = DataType.CSV;
					break;
				case EXCEL:
					dataTypeToExecute = DataType.EXCEL;
					break;
				case JSON:
					dataTypeToExecute = DataType.JSON;
					break;
				case YAML:
					dataTypeToExecute = DataType.YAML;
					break;
				case KDBX:
					dataTypeToExecute = DataType.KDBX;
					break;
				case ODS:
					dataTypeToExecute = DataType.ODS;
					break;
				case SQL:
					dataTypeToExecute = DataType.SQL;
					break;
				case VCF:
					dataTypeToExecute = DataType.VCF;
					break;
				case XML:
					dataTypeToExecute = DataType.XML;
					break;
				default:
					dataTypeToExecute = DataType.CSV;
					break;

			}
		}

		final DataProvider dataProvider;
		switch (dataTypeToExecute) {
			case SQL:
				// The SQL worker was returned without any further configuration, so "-c" (complete commit), "-l" (log file)
				// and "-logerrors" were ignored and it always used its default "commit on full success only"
				final DbSqlWorker sqlWorker = new DbSqlWorker(parent,
						this,
						tableNameForImport,
						isInlineData(),
						importFileOrData,
						getZipPassword());
				sqlWorker.setAnalyseDataOnly(analyseDataOnly);
				if (isLog() && !isInlineData) {
					sqlWorker.setLogFile(createLogFile(importFileOrData));
				}
				sqlWorker.setCompleteCommit(isCompleteCommit());
				sqlWorker.setLogErroneousData(isLogErroneousData() && !isInlineData());
				return sqlWorker;
			case CSV:
				dataProvider = new CsvDataProvider(
						isInlineData(),
						importFileOrData,
						getZipPassword(),
						getEncoding(),
						getSeparator(),
						getStringQuote(),
						getEscapeStringQuote(),
						isInterpretEscapeSequences(),
						isAllowUnderfilledLines(),
						isRemoveSurplusEmptyTrailingColumns(),
						isNoHeaders(),
						getNullValueString(),
						isTrimData());
				break;
			case EXCEL:
				dataProvider = new ExcelDataProvider(
						importFileOrData,
						getZipPassword(),
						isAllowUnderfilledLines(),
						isNoHeaders(),
						getNullValueString(),
						isTrimData(),
						getDataPath());
				break;
			case JSON:
				dataProvider = new JsonDataProvider(
						isInlineData(),
						importFileOrData,
						getZipPassword(),
						getEncoding(),
						getDataPath(),
						getSchemaFilePath());
				break;
			case YAML:
				dataProvider = new YamlDataProvider(
						isInlineData(),
						importFileOrData,
						getZipPassword(),
						getEncoding(),
						getDataPath());
				break;
			case KDBX:
				dataProvider = new KdbxDataProvider(
						importFileOrData,
						getKdbxPassword());
				break;
			case ODS:
				dataProvider = new OdsDataProvider(
						importFileOrData,
						getZipPassword(),
						isAllowUnderfilledLines(),
						isNoHeaders(),
						getNullValueString(),
						isTrimData(),
						getDataPath());
				break;
			case VCF:
				dataProvider = new VcfDataProvider(
						isInlineData(),
						importFileOrData,
						getZipPassword());
				break;
			case XML:
				dataProvider = new XmlDataProvider(
						isInlineData(),
						importFileOrData,
						getZipPassword(),
						getNullValueString(),
						getDataPath(),
						getSchemaFilePath());
				break;
			default:
				// default CSV
				dataProvider = new CsvDataProvider(
						isInlineData(),
						importFileOrData,
						getZipPassword(),
						getEncoding(),
						getSeparator(),
						getStringQuote(),
						getEscapeStringQuote(),
						isInterpretEscapeSequences(),
						isAllowUnderfilledLines(),
						isRemoveSurplusEmptyTrailingColumns(),
						isNoHeaders(),
						getNullValueString(),
						isTrimData());
				break;
		}
		worker.setDataProvider(dataProvider);

		worker.setAnalyseDataOnly(analyseDataOnly);
		if (isLog() && !isInlineData) {
			worker.setLogFile(createLogFile(importFileOrData));
		}
		worker.setTextFileEncoding(getEncoding());
		worker.setMapping(getMapping());
		worker.setImportMode(getImportMode());
		worker.setDuplicateMode(getDuplicateMode());
		worker.setKeycolumns(getKeycolumns());
		worker.setCompleteCommit(isCompleteCommit());
		worker.setCreateNewIndexIfNeeded(isCreateNewIndexIfNeeded());
		worker.setDeactivateForeignKeyConstraints(isDeactivateForeignKeyConstraints());
		worker.setDeactivateTriggers(isDeactivateTriggers());
		worker.setAdditionalInsertValues(getAdditionalInsertValues());
		worker.setAdditionalUpdateValues(getAdditionalUpdateValues());
		worker.setUpdateNullData(isUpdateNullData());
		worker.setCreateTableIfNotExists(isCreateTable());
		worker.setStructureFilePath(getStructureFilePath());
		// Erroneous data items are written to a file next to the import file, which does not exist for inline data
		worker.setLogErroneousData(isLogErroneousData() && !isInlineData());
		worker.setDatabaseTimeZone(databaseTimeZone);
		worker.setImportDataTimeZone(importDataTimeZone);
		worker.setBatchBlockSize(batchBlockSize);
		worker.setPreventBatchFallbackToSingleLineOnErrors(preventBatchFallbackToSingleLineOnErrors);

		return worker;
	}

	/**
	 * Creates the log file for an import file in the subdirectory "importlogs" next to it.
	 *
	 * @param importFilePath path of the import file
	 * @return the log file (not created yet)
	 */
	private static File createLogFile(final String importFilePath) {
		final File logDir = new File(new File(importFilePath).getAbsoluteFile().getParentFile(), "importlogs");
		if (!logDir.exists()) {
			logDir.mkdir();
		}
		return new File(logDir, new File(importFilePath).getName() + "." + DateUtilities.formatDate(DateUtilities.YYYYMMDDHHMMSS, LocalDateTime.now()) + ".import.log");
	}

	/**
	 * Returns the command line parameters for this import, e.g. to show them in the console menu.
	 *
	 * @return the command line parameters
	 */
	public String toParamsString() {
		String params = "";
		params += getDbVendor().name();
		if (getDbVendor() == DbVendor.HSQL) {
			// HSQL: hostname only for a server database. A file database path is marked by a path separator, so it is
			// not taken as hostname when the parameters are read again (see DbImport.isHsqlFileDatabasePath()).
			if (Utilities.isNotBlank(getHostnameAndPort())) {
				params += " " + getHostnameAndPort();
				params += " " + getDbName();
			} else {
				params += " " + (DbImport.isHsqlFileDatabasePath(getDbName()) ? getDbName() : "." + File.separator + getDbName());
			}
		} else {
			if (getDbVendor() != DbVendor.SQLite && getDbVendor() != DbVendor.Derby) {
				params += " " + getHostnameAndPort();
			}
			params += " " + getDbName();
		}
		if (getDbVendor() != DbVendor.SQLite && getDbVendor() != DbVendor.Derby) {
			if (getUsername() != null) {
				params += " " + getUsername();
			}
		}
		params += " -table '" + getTableName().replace("'", "\\'") + "'";
		params += " -import '" + getImportFilePathOrData().replace("'", "\\'") + "'";
		if (getPassword() != null) {
			params += " '" + new String(getPassword()).replace("'", "\\'") + "'";
		}

		if (isInlineData()) {
			params += " " + "-data";
		}
		if (getDataType() != null) {
			params += " " + "-x" + " " + getDataType().name();
		}
		if (isLog()) {
			params += " " + "-l";
		}
		if (isVerbose()) {
			params += " " + "-v";
		}
		if (getZipPassword() != null) {
			params += " " + "-zippassword" + " '" + new String(getZipPassword()).replace("'", "\\'") + "'";
		}
		if (getKdbxPassword() != null && getKdbxPassword().length > 0) {
			params += " " + "-kdbxpassword" + " '" + new String(getKdbxPassword()).replace("'", "\\'") + "'";
		}
		if (getEncoding() != StandardCharsets.UTF_8) {
			params += " " + "-e" + " " + getEncoding().name();
		}
		if (getSeparator() != ';') {
			params += " " + "-s" + " '" + Character.toString(getSeparator()).replace("'", "\\'") + "'";
		}
		if (getStringQuote() == null) {
			// Unboxing the null value for the comparison below threw a NullPointerException
			params += " " + "-q" + " none";
		} else if (getStringQuote() != '"') {
			params += " " + "-q" + " '" + Character.toString(getStringQuote()).replace("'", "\\'") + "'";
		}
		if (getEscapeStringQuote() != '"') {
			params += " " + "-qe" + " '" + Character.toString(getEscapeStringQuote()).replace("'", "\\'") + "'";
		}
		if (isNoHeaders()) {
			params += " " + "-noheaders";
		}
		if (!isInterpretEscapeSequences()) {
			params += " " + "-noescapesequences";
		}
		if (!"".equals(getNullValueString())) {
			params += " " + "-n" + " '" + getNullValueString().replace("'", "\\'") + "'";
		}
		if (isCompleteCommit()) {
			params += " " + "-c";
		}
		if (isAllowUnderfilledLines()) {
			params += " " + "-a";
		}
		if (isRemoveSurplusEmptyTrailingColumns()) {
			params += " " + "-r";
		}
		if (!isUpdateNullData()) {
			params += " " + "-u";
		}
		if (isCreateTable()) {
			params += " " + "-create";
		}
		if (getStructureFilePath() != null && getStructureFilePath().length() > 0) {
			params += " " + "-structure '" + getStructureFilePath().replace("'", "\\'") + "'";
		}
		if (isTrimData()) {
			params += " " + "-t";
		}
		if (isLogErroneousData()) {
			params += " " + "-logerrors";
		}
		if (!isCreateNewIndexIfNeeded()) {
			params += " " + "-nonewindex";
		}
		if (isDeactivateForeignKeyConstraints()) {
			params += " " + "-deactivatefk";
		}
		if (isDeactivateTriggers()) {
			params += " " + "-deactivatetriggers";
		}
		if (getImportMode() != ImportMode.INSERT) {
			params += " " + "-i " + getImportMode().name();
		}
		if (getDuplicateMode() != DuplicateMode.UPDATE_ALL_JOIN) {
			params += " " + "-d " + getDuplicateMode().name();
		}
		if (getKeycolumns() != null && getKeycolumns().size() > 0) {
			final List<String> escapedKeyColumns = new ArrayList<>();
			for (final String keyColumn : getKeycolumns()) {
				escapedKeyColumns.add(keyColumn.replace("'", "\\'"));
			}
			params += " " + "-k '" + Utilities.join(escapedKeyColumns, ", ") + "'";
		}
		if (getMapping() != null && getMapping().length() > 0) {
			params += " " + "-m '" + getMapping().replace("'", "\\'") + "'";
		}
		if (getAdditionalInsertValues() != null && getAdditionalInsertValues().length() > 0) {
			params += " " + "-insvalues '" + getAdditionalInsertValues().replace("'", "\\'") + "'";
		}
		if (getAdditionalUpdateValues() != null && getAdditionalUpdateValues().length() > 0) {
			params += " " + "-updvalues '" + getAdditionalUpdateValues().replace("'", "\\'") + "'";
		}
		if (getDataPath() != null && getDataPath().length() > 0) {
			params += " " + "-dp '" + getDataPath().replace("'", "\\'") + "'";
		}
		if (getSchemaFilePath() != null && getSchemaFilePath().length() > 0) {
			params += " " + "-sp '" + getSchemaFilePath().replace("'", "\\'") + "'";
		}
		if (!TimeZone.getDefault().getID().equals(getDatabaseTimeZone())) {
			params += " " + "-dbtz " + getDatabaseTimeZone() + "";
		}
		if (!TimeZone.getDefault().getID().equals(getImportDataTimeZone())) {
			params += " " + "-idtz " + getImportDataTimeZone() + "";
		}
		if (Utilities.isNotBlank(getDateFormat())) {
			params += " " + "-dateFormat" + " " + getDateFormat();
		}
		if (Utilities.isNotBlank(getDateTimeFormat())) {
			params += " " + "-dateTimeFormat" + " " + getDateTimeFormat();
		}
		if (batchBlockSize != 1000) {
			params += " " + "-batchBlockSize" + " '" + getBatchBlockSize() + "'";
		}
		if (isPreventBatchFallbackToSingleLineOnErrors()) {
			params += " " + "-noSingleMode";
		}
		return params;
	}

	/**
	 * Takes over the parameters of another definition. The import parameters are only taken over from another
	 * {@link DbImportDefinition}, otherwise only the connection parameters.
	 *
	 * @param otherDbConnectionDefinition the definition to take over the parameters from, or null to reset all parameters
	 */
	@Override
	public void importParameters(final DbConnectionDefinition otherDbConnectionDefinition) {
		super.importParameters(otherDbConnectionDefinition);

		if (otherDbConnectionDefinition == null) {
			tableName = "*";
			importFilePathOrData = null;
			isInlineData = false;
			dataType = null;
			log = false;
			verbose = false;
			encoding = StandardCharsets.UTF_8;
			separator = ';';
			stringQuote = '"';
			escapeStringQuote = '"';
			interpretEscapeSequences = true;
			noHeaders = false;
			nullValueString = "";
			completeCommit = false;
			allowUnderfilledLines = false;
			removeSurplusEmptyTrailingColumns = false;
			importMode = ImportMode.INSERT;
			duplicateMode = DuplicateMode.UPDATE_ALL_JOIN;
			updateNullData = true;
			keycolumns = null;
			createTable = false;
			structureFilePath = null;
			trimData = false;
			mapping = "";
			additionalInsertValues = null;
			additionalUpdateValues = null;
			logErroneousData = false;
			createNewIndexIfNeeded = true;
			deactivateForeignKeyConstraints = false;
			deactivateTriggers = false;
			dataPath = null;
			schemaFilePath = null;
			zipPassword = null;
			kdbxPassword = null;
			databaseTimeZone = TimeZone.getDefault().getID();
			importDataTimeZone = TimeZone.getDefault().getID();
			dateFormat = null;
			dateTimeFormat = null;
			batchBlockSize = 1000;
			preventBatchFallbackToSingleLineOnErrors = false;
		} else if (otherDbConnectionDefinition instanceof DbImportDefinition) {
			final DbImportDefinition otherDbImportDefinition = (DbImportDefinition) otherDbConnectionDefinition;
			tableName = otherDbImportDefinition.getTableName();
			importFilePathOrData = otherDbImportDefinition.getImportFilePathOrData();
			isInlineData = otherDbImportDefinition.isInlineData();
			dataType = otherDbImportDefinition.getDataType();
			log = otherDbImportDefinition.isLog();
			verbose = otherDbImportDefinition.isVerbose();
			encoding = otherDbImportDefinition.getEncoding();
			separator = otherDbImportDefinition.getSeparator();
			stringQuote = otherDbImportDefinition.getStringQuote();
			escapeStringQuote = otherDbImportDefinition.getEscapeStringQuote();
			interpretEscapeSequences = otherDbImportDefinition.isInterpretEscapeSequences();
			noHeaders = otherDbImportDefinition.isNoHeaders();
			nullValueString = otherDbImportDefinition.getNullValueString();
			completeCommit = otherDbImportDefinition.isCompleteCommit();
			allowUnderfilledLines = otherDbImportDefinition.isAllowUnderfilledLines();
			removeSurplusEmptyTrailingColumns = otherDbImportDefinition.isRemoveSurplusEmptyTrailingColumns();
			importMode = otherDbImportDefinition.getImportMode();
			duplicateMode = otherDbImportDefinition.getDuplicateMode();
			updateNullData = otherDbImportDefinition.isUpdateNullData();
			keycolumns = otherDbImportDefinition.getKeycolumns();
			createTable = otherDbImportDefinition.isCreateTable();
			structureFilePath = otherDbImportDefinition.getStructureFilePath();
			trimData = otherDbImportDefinition.isTrimData();
			mapping = otherDbImportDefinition.getMapping();
			additionalInsertValues = otherDbImportDefinition.getAdditionalInsertValues();
			additionalUpdateValues = otherDbImportDefinition.getAdditionalUpdateValues();
			logErroneousData = otherDbImportDefinition.isLogErroneousData();
			createNewIndexIfNeeded = otherDbImportDefinition.isCreateNewIndexIfNeeded();
			deactivateForeignKeyConstraints = otherDbImportDefinition.isDeactivateForeignKeyConstraints();
			deactivateTriggers = otherDbImportDefinition.isDeactivateTriggers();
			dataPath = otherDbImportDefinition.getDataPath();
			schemaFilePath = otherDbImportDefinition.getSchemaFilePath();
			zipPassword = otherDbImportDefinition.getZipPassword();
			kdbxPassword = otherDbImportDefinition.getKdbxPassword();
			databaseTimeZone = otherDbImportDefinition.getDatabaseTimeZone();
			importDataTimeZone = otherDbImportDefinition.getImportDataTimeZone();
			dateFormat = otherDbImportDefinition.getDateFormat();
			dateTimeFormat = otherDbImportDefinition.getDateTimeFormat();
			batchBlockSize = otherDbImportDefinition.getBatchBlockSize();
			preventBatchFallbackToSingleLineOnErrors = otherDbImportDefinition.isPreventBatchFallbackToSingleLineOnErrors();
		}
	}
}
