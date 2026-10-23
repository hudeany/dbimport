package de.soderer.dbimport;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import de.soderer.json.JsonArray;
import de.soderer.json.JsonNode;
import de.soderer.json.JsonObject;
import de.soderer.json.JsonReader;
import de.soderer.utilities.LangResources;
import de.soderer.utilities.StringUtilities;
import de.soderer.utilities.Utilities;
import de.soderer.utilities.db.DbUtilities;
import de.soderer.utilities.db.data.DbConnectionDefinition;
import de.soderer.utilities.db.data.DbSimpleDataType;
import de.soderer.utilities.db.data.DbVendor;
import de.soderer.utilities.db.exception.DbNotExistsException;
import de.soderer.utilities.db.utilities.CaseInsensitiveSet;
import de.soderer.utilities.worker.WorkerParentSimple;
import de.soderer.utilities.worker.WorkerSimple;

/**
 * Worker that creates the missing tables and columns of a database structure, which is described in JSON format.
 */
public class DbStructureWorker extends WorkerSimple<Boolean> {
	// Mandatory parameters
	/**
	 * The connection parameters of the database.
	 */
	protected DbConnectionDefinition dbDefinition = null;

	/**
	 * The number of created tables.
	 */
	protected long createdTables = 0;
	/**
	 * The number of created columns (in existing tables).
	 */
	protected long createdColumns = 0;

	/**
	 * The JSON data describing the database structure.
	 */
	protected InputStream jsonStructureDataInputStream;

	/**
	 * Creates the worker.
	 *
	 * @param parent parent to signal the progress to
	 * @param dbDefinition the connection parameters of the database
	 * @param jsonStructureDataInputStream the JSON data describing the database structure (not closed by this worker)
	 * @throws Exception if the worker cannot be created
	 */
	public DbStructureWorker(final WorkerParentSimple parent, final DbConnectionDefinition dbDefinition, final InputStream jsonStructureDataInputStream) throws Exception {
		super(parent);

		this.dbDefinition = dbDefinition;
		this.jsonStructureDataInputStream = jsonStructureDataInputStream;
	}

	/**
	 * Creates the missing tables and columns and commits them.
	 *
	 * @return true if the worker was not canceled
	 * @throws Exception if the JSON data is invalid or a database statement fails
	 */
	@Override
	public Boolean work() throws Exception {
		signalUnlimitedProgress();

		Connection connection = null;
		boolean previousAutoCommit = false;
		try (JsonReader jsonReader = new JsonReader(jsonStructureDataInputStream)) {
			if (dbDefinition.getDbVendor() == DbVendor.Derby || (dbDefinition.getDbVendor() == DbVendor.HSQL && StringUtilities.isBlank(dbDefinition.getHostnameAndPort())) || dbDefinition.getDbVendor() == DbVendor.SQLite) {
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

			createdTables = 0;
			createdColumns = 0;

			parent.changeTitle(LangResources.get("creatingMissingTablesAndColumns"));

			final JsonNode dbStructureJsonNode = jsonReader.read();

			if (!dbStructureJsonNode.isJsonObject()) {
				throw new DbImportException("Invalid database structure file. Must contain JsonObject with table properties");
			}

			final JsonObject dbStructureJsonObject = (JsonObject) dbStructureJsonNode;

			itemsToDo = dbStructureJsonObject.size();
			itemsDone = 0;

			for (final Entry<String, Object> tableEntry : dbStructureJsonObject) {
				final String tableName = tableEntry.getKey();
				parent.changeTitle(LangResources.get("workingOnTable", tableName));
				final JsonObject tableJsonObject = (JsonObject) tableEntry.getValue();
				if (!DbUtilities.checkTableExist(connection, tableName)) {
					createTable(connection, tableName, tableJsonObject);
					createdTables++;
				} else {
					final CaseInsensitiveSet columnNames = DbUtilities.getColumnNames(connection, tableName);
					final JsonArray tableColumnsJsonArray = (JsonArray) tableJsonObject.get("columns");
					for (final Object columnObject : tableColumnsJsonArray) {
						final JsonObject columnJsonObject = (JsonObject) columnObject;
						final String columnName = (String) columnJsonObject.getSimpleValue("name");
						if (!columnNames.contains(columnName)) {
							createTableColumn(connection, tableName, columnJsonObject);
							createdColumns++;
						}
					}
				}

				itemsDone++;
				signalProgress(false);
			}

			// Without this commit the rollback in the finally block undoes the whole structure creation
			// on databases with transactional DDL (e.g. PostgreSQL)
			connection.commit();

			signalProgress(true);
			setEndTime(LocalDateTime.now());
		} finally {
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
		}

		return !cancel;
	}

	private void createTable(final Connection connection, final String tableName, final JsonObject tableJsonObject) throws Exception {
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

			// "keycolumns" is optional (same as in DbImportWorker.createTable())
			List<String> keyColumns = null;
			if (tableJsonObject.containsKey("keycolumns")) {
				keyColumns = ((JsonArray) tableJsonObject.get("keycolumns")).items().stream().map(String.class::cast).collect(Collectors.toList());
			}

			String primaryKeyPart = "";
			if (Utilities.isNotEmpty(keyColumns)) {
				primaryKeyPart = ", PRIMARY KEY (" + DbUtilities.joinColumnVendorEscaped(dbDefinition.getDbVendor(), keyColumns) + ")";
			}
			statement.execute("CREATE TABLE " + tableName + " (" + columnsPart + primaryKeyPart + ")");
			if (dbDefinition.getDbVendor() == DbVendor.Derby) {
				connection.commit();
			}
		}
	}

	private void createTableColumn(final Connection connection, final String tableName, final JsonObject columnJsonObject) throws Exception {
		if (columnJsonObject == null) {
			throw new DbImportException("Cannot create table column without column definition");
		}

		try (Statement statement = connection.createStatement()) {
			final String columnsPart = getColumnNameAndType(columnJsonObject);

			statement.execute("ALTER TABLE " + tableName + " ADD " + columnsPart);
			if (dbDefinition.getDbVendor() == DbVendor.Derby) {
				connection.commit();
			}
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

		String defaultvalue = null;
		if (columnJsonObject.containsKey("defaultvalue") && columnJsonObject.getSimpleValue("defaultvalue") != null) {
			// Numeric default values are no Strings (the cast failed with a ClassCastException)
			defaultvalue = columnJsonObject.getSimpleValue("defaultvalue").toString();
		}
		String defaultvaluePart = "";
		if (defaultvalue != null) {
			if (simpleDataType == DbSimpleDataType.String) {
				defaultvaluePart = defaultvalue;
				if (!defaultvaluePart.startsWith("'") || !defaultvaluePart.endsWith("'")) {
					defaultvaluePart = "'" + defaultvaluePart + "'";
				}
			} else {
				defaultvaluePart = defaultvalue;
			}

			defaultvaluePart = " DEFAULT " + defaultvaluePart;
		}

		// "databasevendorspecific_datatype"

		return name + " " + DbUtilities.getDataType(dbDefinition.getDbVendor(), simpleDataType) + (characterByteSize > -1 ? "(" + characterByteSize + ")" : "") + defaultvaluePart;
	}

	/**
	 * Returns the number of created tables.
	 *
	 * @return the number of created tables
	 */
	public long getCreatedTables() {
		return createdTables;
	}

	/**
	 * Returns the number of created columns (in existing tables).
	 *
	 * @return the number of created columns
	 */
	public long getCreatedColumns() {
		return createdColumns;
	}
}
