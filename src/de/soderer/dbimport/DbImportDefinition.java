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
import de.soderer.utilities.DateUtilities;
import de.soderer.utilities.FileUtilities;
import de.soderer.utilities.Utilities;
import de.soderer.utilities.db.DbDefinition;
import de.soderer.utilities.db.DbUtilities.DbVendor;
import de.soderer.utilities.worker.WorkerParentSimple;

public class DbImportDefinition extends DbDefinition {
	/**
	 * The Enum DataType.
	 */
	public enum DataType {
		CSV,
		JSON,
		XML,
		SQL,
		EXCEL,
		ODS,
		VCF,
		KDBX;

		/**
		 * Gets the string representation of data type.
		 *
		 * @param dataType
		 *            the data type
		 * @return the from string
		 * @throws Exception
		 *             the exception
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
	 * The Enum ImportMode.
	 */
	public enum ImportMode {
		CLEARINSERT,
		INSERT,
		UPDATE,
		UPSERT;

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
	 * The Enum DuplicateMode.
	 */
	public enum DuplicateMode {
		NO_CHECK,
		CKECK_SOURCE_ONLY_DROP,
		CKECK_SOURCE_ONLY_JOIN,
		UPDATE_FIRST_DROP,
		UPDATE_FIRST_JOIN,
		UPDATE_ALL_DROP,
		UPDATE_ALL_JOIN,
		MAKE_UNIQUE_DROP,
		MAKE_UNIQUE_JOIN;

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

	public String getTableName() {
		return tableName;
	}

	public void setTableName(final String tableName) {
		this.tableName = tableName;
	}

	public String getImportFilePathOrData() {
		return importFilePathOrData;
	}

	public boolean isInlineData() {
		return isInlineData;
	}

	public void setInlineData(final boolean isInlineData) {
		this.isInlineData = isInlineData;
	}

	public void setImportFilePathOrData(final String importFilePathOrData, final boolean checkForExistingFiles) throws DbImportException {
		this.importFilePathOrData = importFilePathOrData;
		if (this.importFilePathOrData != null && !isInlineData) {
			// Check filepath syntax
			if (getImportFilePathOrData().contains("?") || getImportFilePathOrData().contains("*")) {
				final int lastSeparator = Math.max(getImportFilePathOrData().lastIndexOf("/"), getImportFilePathOrData().lastIndexOf("\\"));
				String directoryPath = getImportFilePathOrData().substring(0, lastSeparator);
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

	public DataType getDataType() {
		return dataType;
	}

	public void setDataType(final DataType dataType) {
		this.dataType = dataType;
	}

	public boolean isLog() {
		return log;
	}

	public void setLog(final boolean log) {
		this.log = log;
	}

	public boolean isVerbose() {
		return verbose;
	}

	public void setVerbose(final boolean verbose) {
		this.verbose = verbose;
	}

	public Charset getEncoding() {
		return encoding;
	}

	public void setEncoding(final Charset encoding) {
		this.encoding = encoding;
	}

	public char getSeparator() {
		return separator;
	}

	public void setSeparator(final char separator) {
		this.separator = separator;
	}

	public Character getStringQuote() {
		return stringQuote;
	}

	public void setStringQuote(final Character stringQuote) {
		this.stringQuote = stringQuote;
	}

	public char getEscapeStringQuote() {
		return escapeStringQuote;
	}

	public void setEscapeStringQuote(final char escapeStringQuote) {
		this.escapeStringQuote = escapeStringQuote;
	}

	public boolean isNoHeaders() {
		return noHeaders;
	}

	public void setNoHeaders(final boolean noHeaders) {
		this.noHeaders = noHeaders;
	}

	public String getNullValueString() {
		return nullValueString;
	}

	public void setNullValueString(final String nullValueString) {
		this.nullValueString = nullValueString;
	}

	public boolean isCompleteCommit() {
		return completeCommit;
	}

	public void setCompleteCommit(final boolean completeCommit) {
		this.completeCommit = completeCommit;
	}

	public boolean isAllowUnderfilledLines() {
		return allowUnderfilledLines;
	}

	public void setAllowUnderfilledLines(final boolean allowUnderfilledLines) {
		this.allowUnderfilledLines = allowUnderfilledLines;
	}

	public boolean isRemoveSurplusEmptyTrailingColumns() {
		return removeSurplusEmptyTrailingColumns;
	}

	public void setRemoveSurplusEmptyTrailingColumns(final boolean removeSurplusEmptyTrailingColumns) {
		this.removeSurplusEmptyTrailingColumns = removeSurplusEmptyTrailingColumns;
	}

	public ImportMode getImportMode() {
		return importMode;
	}

	public void setImportMode(final ImportMode importMode) {
		this.importMode = importMode;
	}

	public DuplicateMode getDuplicateMode() {
		return duplicateMode;
	}

	public void setDuplicateMode(final DuplicateMode duplicateMode) {
		this.duplicateMode = duplicateMode;
	}

	public boolean isUpdateNullData() {
		return updateNullData;
	}

	public void setUpdateNullData(final boolean updateNullData) {
		this.updateNullData = updateNullData;
	}

	public List<String> getKeycolumns() {
		return keycolumns;
	}

	public void setKeycolumns(final List<String> keycolumns) {
		this.keycolumns = keycolumns;
	}

	public boolean isCreateTable() {
		return createTable;
	}

	public void setCreateTable(final boolean createTable) {
		this.createTable = createTable;
	}

	public String getStructureFilePath() {
		return structureFilePath;
	}

	public void setStructureFilePath(final String structureFilePath) {
		this.structureFilePath = structureFilePath;
	}

	public String getMapping() {
		return mapping;
	}

	public void setMapping(final String mapping) {
		this.mapping = mapping;
	}

	public boolean isTrimData() {
		return trimData;
	}

	public void setTrimData(final boolean trimData) {
		this.trimData = trimData;
	}

	public void setAdditionalInsertValues(final String additionalInsertValues) {
		this.additionalInsertValues = additionalInsertValues;
	}

	public String getAdditionalInsertValues() {
		return additionalInsertValues;
	}

	public void setAdditionalUpdateValues(final String additionalUpdateValues) {
		this.additionalUpdateValues = additionalUpdateValues;
	}

	public String getAdditionalUpdateValues() {
		return additionalUpdateValues;
	}

	public boolean isLogErroneousData() {
		return logErroneousData;
	}

	public void setLogErroneousData(final boolean logErroneousData) {
		this.logErroneousData = logErroneousData;
	}

	public boolean isCreateNewIndexIfNeeded() {
		return createNewIndexIfNeeded;
	}

	public void setCreateNewIndexIfNeeded(final boolean createNewIndexIfNeeded) {
		this.createNewIndexIfNeeded = createNewIndexIfNeeded;
	}

	public boolean isDeactivateForeignKeyConstraints() {
		return deactivateForeignKeyConstraints;
	}

	public void setDeactivateForeignKeyConstraints(final boolean deactivateForeignKeyConstraints) {
		this.deactivateForeignKeyConstraints = deactivateForeignKeyConstraints;
	}

	public boolean isDeactivateTriggers() {
		return deactivateTriggers;
	}

	public void setDeactivateTriggers(final boolean deactivateTriggers) {
		this.deactivateTriggers = deactivateTriggers;
	}

	public String getDataPath() {
		return dataPath;
	}

	public void setDataPath(final String dataPath) {
		this.dataPath = dataPath;
	}

	public String getSchemaFilePath() {
		return schemaFilePath;
	}

	public void setSchemaFilePath(final String schemaFilePath) {
		this.schemaFilePath = schemaFilePath;
	}

	public void setZipPassword(final char[] zipPassword) {
		this.zipPassword = zipPassword;
	}

	public char[] getZipPassword() {
		return zipPassword;
	}

	public void setKdbxPassword(final char[] kdbxPassword) {
		this.kdbxPassword = kdbxPassword;
	}

	public char[] getKdbxPassword() {
		return kdbxPassword;
	}

	public void setDatabaseTimeZone(final String databaseTimeZone) {
		this.databaseTimeZone = databaseTimeZone;
		if (this.databaseTimeZone == null) {
			this.databaseTimeZone = TimeZone.getDefault().getID();
		}
	}

	public String getDatabaseTimeZone() {
		return databaseTimeZone;
	}

	public void setImportDataTimeZone(final String importDataTimeZone) {
		this.importDataTimeZone = importDataTimeZone;
		if (this.importDataTimeZone == null) {
			this.importDataTimeZone = TimeZone.getDefault().getID();
		}
	}

	public String getImportDataTimeZone() {
		return importDataTimeZone;
	}

	public String getDateFormat() {
		return dateFormat;
	}

	public void setDateFormat(final String dateFormat) {
		this.dateFormat = dateFormat;
	}

	public String getDateTimeFormat() {
		return dateTimeFormat;
	}

	public void setDateTimeFormat(final String dateTimeFormat) {
		this.dateTimeFormat = dateTimeFormat;
	}

	public int getBatchBlockSize() {
		return batchBlockSize;
	}

	public void setBatchBlockSize(final int batchBlockSize) {
		this.batchBlockSize = batchBlockSize;
	}

	public boolean isPreventBatchFallbackToSingleLineOnErrors() {
		return preventBatchFallbackToSingleLineOnErrors;
	}

	public void setPreventBatchFallbackToSingleLineOnErrors(final boolean preventBatchFallbackToSingleLineOnErrors) {
		this.preventBatchFallbackToSingleLineOnErrors = preventBatchFallbackToSingleLineOnErrors;
	}

	public void checkParameters() throws Exception {
		super.checkParameters(DbImport.APPLICATION_NAME, DbImport.CONFIGURATION_FILE);

		if (importFilePathOrData == null) {
			throw new DbImportException("ImportFilePath or data is missing");
		} else if (!isInlineData) {
			if (getDataType() == null) {
				if (Utilities.endsWithIgnoreCase(getImportFilePathOrData(), ".csv")
						|| Utilities.endsWithIgnoreCase(getImportFilePathOrData(), ".csv.zip")
						|| Utilities.endsWithIgnoreCase(getImportFilePathOrData(), ".csv.tar.gz")
						|| Utilities.endsWithIgnoreCase(getImportFilePathOrData(), ".csv.tgz")
						|| Utilities.endsWithIgnoreCase(getImportFilePathOrData(), ".csv.gz")) {
					setDataType(DataType.CSV);
				} else if (Utilities.endsWithIgnoreCase(getImportFilePathOrData(), ".json")
						|| Utilities.endsWithIgnoreCase(getImportFilePathOrData(), ".json.zip")
						|| Utilities.endsWithIgnoreCase(getImportFilePathOrData(), ".json.tar.gz")
						|| Utilities.endsWithIgnoreCase(getImportFilePathOrData(), ".json.tgz")
						|| Utilities.endsWithIgnoreCase(getImportFilePathOrData(), ".json.gz")) {
					setDataType(DataType.JSON);
				} else if (Utilities.endsWithIgnoreCase(getImportFilePathOrData(), ".xls")
						|| Utilities.endsWithIgnoreCase(getImportFilePathOrData(), ".xls.zip")
						|| Utilities.endsWithIgnoreCase(getImportFilePathOrData(), ".xls.tar.gz")
						|| Utilities.endsWithIgnoreCase(getImportFilePathOrData(), ".xls.tgz")
						|| Utilities.endsWithIgnoreCase(getImportFilePathOrData(), ".xls.gz")
						|| Utilities.endsWithIgnoreCase(getImportFilePathOrData(), ".xlsx")
						|| Utilities.endsWithIgnoreCase(getImportFilePathOrData(), ".xlsx.zip")
						|| Utilities.endsWithIgnoreCase(getImportFilePathOrData(), ".xlsx.tar.gz")
						|| Utilities.endsWithIgnoreCase(getImportFilePathOrData(), ".xlsx.tgz")
						|| Utilities.endsWithIgnoreCase(getImportFilePathOrData(), ".xlsx.gz")) {
					setDataType(DataType.EXCEL);
				} else if (Utilities.endsWithIgnoreCase(getImportFilePathOrData(), ".ods")
						|| Utilities.endsWithIgnoreCase(getImportFilePathOrData(), ".ods.zip")
						|| Utilities.endsWithIgnoreCase(getImportFilePathOrData(), ".ods.tar.gz")
						|| Utilities.endsWithIgnoreCase(getImportFilePathOrData(), ".ods.tgz")
						|| Utilities.endsWithIgnoreCase(getImportFilePathOrData(), ".ods.gz")) {
					setDataType(DataType.ODS);
				} else if (Utilities.endsWithIgnoreCase(getImportFilePathOrData(), ".xml")
						|| Utilities.endsWithIgnoreCase(getImportFilePathOrData(), ".xml.zip")
						|| Utilities.endsWithIgnoreCase(getImportFilePathOrData(), ".xml.tar.gz")
						|| Utilities.endsWithIgnoreCase(getImportFilePathOrData(), ".xml.tgz")
						|| Utilities.endsWithIgnoreCase(getImportFilePathOrData(), ".xml.gz")) {
					setDataType(DataType.XML);
				} else if (Utilities.endsWithIgnoreCase(getImportFilePathOrData(), ".sql")
						|| Utilities.endsWithIgnoreCase(getImportFilePathOrData(), ".sql.zip")
						|| Utilities.endsWithIgnoreCase(getImportFilePathOrData(), ".sql.tar.gz")
						|| Utilities.endsWithIgnoreCase(getImportFilePathOrData(), ".sql.tgz")
						|| Utilities.endsWithIgnoreCase(getImportFilePathOrData(), ".sql.gz")) {
					setDataType(DataType.SQL);
				} else if (Utilities.endsWithIgnoreCase(getImportFilePathOrData(), ".vcf")
						|| Utilities.endsWithIgnoreCase(getImportFilePathOrData(), ".vcf.zip")
						|| Utilities.endsWithIgnoreCase(getImportFilePathOrData(), ".vcf.tar.gz")
						|| Utilities.endsWithIgnoreCase(getImportFilePathOrData(), ".vcf.tgz")
						|| Utilities.endsWithIgnoreCase(getImportFilePathOrData(), ".vcf.gz")) {
					setDataType(DataType.VCF);
				} else if (Utilities.endsWithIgnoreCase(getImportFilePathOrData(), ".kdbx")
						|| Utilities.endsWithIgnoreCase(getImportFilePathOrData(), ".kdbx.zip")
						|| Utilities.endsWithIgnoreCase(getImportFilePathOrData(), ".kdbx.tar.gz")
						|| Utilities.endsWithIgnoreCase(getImportFilePathOrData(), ".kdbx.tgz")
						|| Utilities.endsWithIgnoreCase(getImportFilePathOrData(), ".kdbx.gz")) {
					setDataType(DataType.KDBX);
				} else {
					setDataType(DataType.CSV);
				}
			}

			if (getImportFilePathOrData().contains("?") || getImportFilePathOrData().contains("*")) {
				final int lastSeparator = Math.max(getImportFilePathOrData().lastIndexOf("/"), getImportFilePathOrData().lastIndexOf("\\"));
				String directoryPath = getImportFilePathOrData().substring(0, lastSeparator);
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

		if (Utilities.isNotEmpty(dataPath) && dataType != DataType.XML && dataType != DataType.JSON && dataType != DataType.EXCEL && dataType != DataType.ODS) {
			throw new DbImportException("DataPath is not supported for data format " + dataType);
		}

		if (Utilities.isNotEmpty(schemaFilePath) && dataType != DataType.XML && dataType != DataType.JSON) {
			throw new DbImportException("SchemaFilePath is not supported for data format " + dataType);
		}

		if (batchBlockSize < 1) {
			throw new DbImportException("Batch blocksize must be at least 1, but was " + batchBlockSize);
		}
	}

	/**
	 * Create and configure a worker according to the current configuration
	 *
	 * @param parent
	 * @return
	 * @throws Exception
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
				return new DbSqlWorker(parent,
						this,
						tableNameForImport,
						isInlineData(),
						importFileOrData,
						getZipPassword());
			case CSV:
				dataProvider = new CsvDataProvider(
						isInlineData(),
						importFileOrData,
						getZipPassword(),
						getSeparator(),
						getStringQuote(),
						getEscapeStringQuote(),
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
						getDataPath(),
						getSchemaFilePath());
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
						null);
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
						getSeparator(),
						getStringQuote(),
						getEscapeStringQuote(),
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
			final File logDir = new File(new File(importFileOrData).getParentFile(), "importlogs");
			if (!logDir.exists()) {
				logDir.mkdir();
			}
			worker.setLogFile(new File(logDir, new File(importFileOrData).getName() + "." + DateUtilities.formatDate(DateUtilities.YYYYMMDDHHMMSS, LocalDateTime.now()) + ".import.log"));
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
		worker.setLogErroneousData(isLogErroneousData());
		worker.setDatabaseTimeZone(databaseTimeZone);
		worker.setImportDataTimeZone(importDataTimeZone);
		worker.setBatchBlockSize(batchBlockSize);
		worker.setPreventBatchFallbackToSingleLineOnErrors(preventBatchFallbackToSingleLineOnErrors);

		return worker;
	}

	public String toParamsString() {
		String params = "";
		params += getDbVendor().name();
		if (getDbVendor() != DbVendor.SQLite && getDbVendor() != DbVendor.HSQL && getDbVendor() != DbVendor.Derby) {
			params += " " + getHostnameAndPort();
		}
		params += " " + getDbName();
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
		if (TimeZone.getDefault().getDisplayName().equalsIgnoreCase(getDatabaseTimeZone())) {
			params += " " + "-dbtz" + " " + getDatabaseTimeZone();
		}
		if (TimeZone.getDefault().getDisplayName().equalsIgnoreCase(getImportDataTimeZone())) {
			params += " " + "-idtz" + " " + getImportDataTimeZone();
		}
		if (getEncoding() != StandardCharsets.UTF_8) {
			params += " " + "-e" + " " + getEncoding().name();
		}
		if (getSeparator() != ';') {
			params += " " + "-s" + " '" + Character.toString(getSeparator()).replace("'", "\\'") + "'";
		}
		if (getStringQuote() != '"') {
			params += " " + "-q" + " '" + Character.toString(getStringQuote()).replace("'", "\\'") + "'";
		}
		if (getEscapeStringQuote() != '"') {
			params += " " + "-qe" + " '" + Character.toString(getEscapeStringQuote()).replace("'", "\\'") + "'";
		}
		if (isNoHeaders()) {
			params += " " + "-noheaders";
		}
		if (!"".equals(getNullValueString())) {
			params += " " + "-n" + " '" + getNullValueString() + "'";
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
			params += " " + "-i " + getDuplicateMode().name();
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
		if (getDatabaseTimeZone() != TimeZone.getDefault().getID()) {
			params += " " + "-dbtz " + getDatabaseTimeZone() + "";
		}
		if (getImportDataTimeZone() != TimeZone.getDefault().getID()) {
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

	@Override
	public void importParameters(final DbDefinition otherDbDefinition) {
		super.importParameters(otherDbDefinition);

		if (otherDbDefinition == null) {
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
		} else if (otherDbDefinition instanceof DbImportDefinition) {
			final DbImportDefinition otherDbImportDefinition = (DbImportDefinition) otherDbDefinition;
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
