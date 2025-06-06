package de.soderer.dbimport;

import java.io.File;
import java.io.FileFilter;
import java.io.FileOutputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.Statement;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.w3c.dom.Element;

import de.soderer.dbimport.DbImportDefinition.DuplicateMode;
import de.soderer.utilities.BOM;
import de.soderer.utilities.FileUtilities;
import de.soderer.utilities.TextUtilities;
import de.soderer.utilities.Utilities;
import de.soderer.utilities.WildcardFilenameFilter;
import de.soderer.utilities.db.DbDefinition;
import de.soderer.utilities.db.DbUtilities;
import de.soderer.utilities.db.DbUtilities.DbVendor;
import de.soderer.utilities.json.JsonArray;
import de.soderer.utilities.json.JsonObject;
import de.soderer.utilities.json.JsonWriter;
import de.soderer.utilities.xml.XmlUtilities;

public class DbImportTest_Cassandra {
	public static final String HOSTNAME = System.getenv().get("HOSTNAME_CASSANDRA_TEST");
	public static final String DBNAME = System.getenv().get("DBNAME_CASSANDRA_TEST");
	public static final String USERNAME = System.getenv().get("USERNAME_CASSANDRA_TEST");
	public static final String PASSWORD = System.getenv().get("PASSWORD_CASSANDRA_TEST");

	public static final String[] DATA_TYPES = new String[] { "INT", "DOUBLE", "VARCHAR", "BLOB", "TEXT", "TIMESTAMP", "DATE" };

	public static File INPUTFILE_CSV = new File(Utilities.replaceUsersHome("~" + File.separator + "temp" + File.separator + "test_tbl.csv"));
	public static File INPUTFILE_JSON = new File(Utilities.replaceUsersHome("~" + File.separator + "temp" + File.separator + "test_tbl.json"));
	public static File INPUTFILE_XML = new File(Utilities.replaceUsersHome("~" + File.separator + "temp" + File.separator + "test_tbl.xml"));
	public static File BLOB_DATA_FILE = new File(Utilities.replaceUsersHome("~" + File.separator + "temp" + File.separator + "test.blob"));

	@Before
	public void setup() throws Exception {
		INPUTFILE_CSV.delete();
		INPUTFILE_JSON.delete();
		INPUTFILE_XML.delete();
		BLOB_DATA_FILE.delete();

		try (Connection connection = DbUtilities.createConnection(new DbDefinition(DbVendor.Cassandra, HOSTNAME, DBNAME, USERNAME, PASSWORD == null ? null : PASSWORD.toCharArray()), false)) {
			if (DbUtilities.checkTableExist(connection, "test_tbl")) {
				try (Statement statement = connection.createStatement()) {
					statement.execute("DROP TABLE test_tbl");
				}
			}
		} catch (final Exception e) {
			e.printStackTrace();
			throw e;
		}

		deleteLogFiles();
	}

	@After
	public void tearDown() throws Exception {
		INPUTFILE_CSV.delete();
		INPUTFILE_JSON.delete();
		INPUTFILE_XML.delete();
		BLOB_DATA_FILE.delete();

		try (Connection connection = DbUtilities.createConnection(new DbDefinition(DbVendor.Cassandra, HOSTNAME, DBNAME, USERNAME, PASSWORD == null ? null : PASSWORD.toCharArray()), false)) {
			if (DbUtilities.checkTableExist(connection, "test_tbl")) {
				try (Statement statement = connection.createStatement()) {
					statement.execute("DROP TABLE test_tbl");
				}
			}
		} catch (final Exception e) {
			e.printStackTrace();
			throw e;
		}
	}

	private void createEmptyTestTable() throws Exception {
		try (Connection connection = DbUtilities.createConnection(new DbDefinition(DbVendor.Cassandra, HOSTNAME, DBNAME, USERNAME, PASSWORD == null ? null : PASSWORD.toCharArray()), false);
				Statement statement = connection.createStatement()) {
			String dataColumnsPart = "";
			String dataColumnsPartForInsert = "";
			for (final String dataType : DATA_TYPES) {
				String columnName = "column_" + dataType.toLowerCase();
				if (columnName.contains("(")) {
					columnName = columnName.substring(0, columnName.indexOf("("));
				}

				if (dataColumnsPart.length() > 0) {
					dataColumnsPart += ", ";
				}
				dataColumnsPart += columnName + " " + dataType;
				if (dataColumnsPartForInsert.length() > 0) {
					dataColumnsPartForInsert += ", ";
				}
				dataColumnsPartForInsert += columnName;
			}
			statement.execute("CREATE TABLE test_tbl (id INT PRIMARY KEY, " + dataColumnsPart + ")");
		} catch (final Exception e) {
			e.printStackTrace();
			throw e;
		}
	}

	private void prefillTestTable() throws Exception {
		try (Connection connection = DbUtilities.createConnection(new DbDefinition(DbVendor.Cassandra, HOSTNAME, DBNAME, USERNAME, PASSWORD == null ? null : PASSWORD.toCharArray()), false);
				Statement statement = connection.createStatement()) {
			statement.executeUpdate("INSERT INTO test_tbl (id, column_int, column_varchar) VALUES (1, 1, '<test_text>_1')".replace("<test_text>", TextUtilities.GERMAN_TEST_STRING.replace("'", "''").replace("\\", "\\\\")));
			statement.executeUpdate("INSERT INTO test_tbl (id, column_int, column_varchar) VALUES (3, 3, '<test_text>_3')".replace("<test_text>", TextUtilities.GERMAN_TEST_STRING.replace("'", "''").replace("\\", "\\\\")));
			statement.executeUpdate("INSERT INTO test_tbl (id, column_int, column_varchar) VALUES (999, 999, '<test_text>_999')".replace("<test_text>", TextUtilities.GERMAN_TEST_STRING.replace("'", "''").replace("\\", "\\\\")));
		} catch (final Exception e) {
			e.printStackTrace();
			throw e;
		}
	}

	private String exportTestTable() throws Exception {
		try (Connection connection = DbUtilities.createConnection(new DbDefinition(DbVendor.Cassandra, HOSTNAME, DBNAME, USERNAME, PASSWORD == null ? null : PASSWORD.toCharArray()), false)) {
			return DbUtilities.readoutTable(connection, "test_tbl", ';', '\"').replace(TextUtilities.GERMAN_TEST_STRING.replace("\"", "\"\"").replace("\\", "\\\\"), "<test_text>");
		} catch (final Exception e) {
			e.printStackTrace();
			throw e;
		}
	}

	private void deleteLogFiles() {
		final File dir = new File(System.getProperty("user.home") + File.separator + "temp");
		final FileFilter fileFilter = new WildcardFilenameFilter("test_tbl*.log");
		final File[] files = dir.listFiles(fileFilter);
		for (final File logFile : files) {
			logFile.delete();
		}
	}

	private String getLogFileData() throws Exception {
		final File dir = new File(System.getProperty("user.home") + File.separator + "temp");
		final FileFilter fileFilter = new WildcardFilenameFilter("test_tbl*.log");
		final File[] files = dir.listFiles(fileFilter);
		return FileUtilities.readFileToString(files[0], StandardCharsets.UTF_8);
	}

	private void assertLogContains(final String logData, final String valueName, final String value) {
		boolean valueFound = false;
		for (final String line : logData.split("\r\n|\r|\n")) {
			if (line.startsWith(valueName + ":") && line.endsWith(" " + value)) {
				valueFound = true;
			}
		}
		Assert.assertTrue(logData, valueFound);
	}

	@Test
	public void testCsvImportNoMapping() {
		try {
			createEmptyTestTable();

			FileUtilities.write(INPUTFILE_CSV,
					("id;column_int; column_double; column_varchar; column_text\n"
							+ "123;123; 123.456; aBcDeF123; aBcDeF123\n").getBytes(StandardCharsets.UTF_8));
			Assert.assertEquals(0, DbImport._main(new String[] {
					"cassandra",
					HOSTNAME,
					DBNAME,
					USERNAME,
					"-table", "test_tbl",
					"-l",
					"-import", "~" + File.separator + "temp" + File.separator + "test_tbl.csv",
					"-i", "INSERT",
					PASSWORD }));
			Assert.assertEquals(
					"id;column_blob;column_date;column_double;column_int;column_text;column_timestamp;column_varchar\n"
							+ "123;;;123.456;123; aBcDeF123;; aBcDeF123\n",
							exportTestTable());

			final String logData = getLogFileData();
			assertLogContains(logData, "Found items", "1");
			assertLogContains(logData, "Valid items", "1");
			assertLogContains(logData, "Invalid items", "0");
			assertLogContains(logData, "Imported data amount", "97 Byte");
			assertLogContains(logData, "Inserted items", "1");
		} catch (final Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testConnection() {
		try {
			Assert.assertEquals(0, DbImport._main(new String[] {
					"connectiontest",
					"cassandra",
					HOSTNAME,
					DBNAME,
					"-iter", "5" }));
		} catch (final Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testCsvImportNoMappingNoHeaders() {
		try {
			FileUtilities.write(INPUTFILE_CSV,
					("123; 123.456; aBcDeF123; aBcDeF123; 1234567890; 12345678901234567890; 123456789012345678901\n").getBytes(StandardCharsets.UTF_8));
			Assert.assertEquals(1, DbImport._main(new String[] {
					"cassandra",
					HOSTNAME,
					DBNAME,
					USERNAME,
					"-table", "test_tbl",
					"-i", "INSERT",
					"-l",
					"-create",
					"-import", "~" + File.separator + "temp" + File.separator + "test_tbl.csv",
					"-noheaders",
					PASSWORD }));
			// Creation of new table needs key column definition in NoSql import
		} catch (final Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testCsvImport() {
		try {
			createEmptyTestTable();

			FileUtilities.write(INPUTFILE_CSV,
					("id;column int; column_double; column_varchar; column_text; column_timestamp; column_date\n"
							+ "123;123; 123.456; aBcDeF123; aBcDeF1234; 01.02.2003 11:12:13; 01.02.2003 21:22:23\n").getBytes(StandardCharsets.UTF_8));
			final String mapping = "id='id';column_int='column int'; column_double='column_double'; column_varchar='column_varchar'; column_text='column_text'; column_blob=; column_timestamp='column_timestamp'dd.MM.yyyy HH:mm:ss; column_date='column_date'dd.MM.yyyy HH:mm:ss";
			Assert.assertEquals(0, DbImport._main(new String[] {
					"cassandra",
					HOSTNAME,
					DBNAME,
					USERNAME,
					"-table", "test_tbl",
					"-i", "INSERT",
					"-l",
					"-import", "~" + File.separator + "temp" + File.separator + "test_tbl.csv",
					"-m", mapping,
					PASSWORD }));
			Assert.assertEquals(
					"id;column_blob;column_date;column_double;column_int;column_text;column_timestamp;column_varchar\n"
							+ "123;;2003-02-01;123.456;123; aBcDeF1234;2003-02-01 11:12:13.000000; aBcDeF123\n",
							exportTestTable());

			final String logData = getLogFileData();
			assertLogContains(logData, "Found items", "1");
			assertLogContains(logData, "Valid items", "1");
			assertLogContains(logData, "Invalid items", "0");
			assertLogContains(logData, "Imported data amount", "171 Byte");
			assertLogContains(logData, "Inserted items", "1");
		} catch (final Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testCsvImportBig() {
		try {
			createEmptyTestTable();

			final int numberOfLines = 1200;

			final StringBuilder dataPart = new StringBuilder();
			for (int i = 0; i < numberOfLines; i++) {
				dataPart.append(i + ";" + i + "; 123.456; aBcDeF123; aBcDeF1234; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			}
			FileUtilities.write(INPUTFILE_CSV, ("id;column int; column_double; column_varchar; column_text; column_timestamp; column_date\n" + dataPart.toString()).getBytes(StandardCharsets.UTF_8));

			final String mapping = "id='id';column_int='column int'; column_double='column_double'; column_varchar='column_varchar'; column_text='column_text'; column_blob=; column_timestamp='column_timestamp'dd.MM.yyyy HH:mm:ss; column_date='column_date'dd.MM.yyyy HH:mm:ss";

			Assert.assertEquals(0, DbImport._main(new String[] {
					"cassandra",
					HOSTNAME,
					DBNAME,
					USERNAME,
					"-table", "test_tbl",
					"-i", "INSERT",
					"-l",
					"-import", "~" + File.separator + "temp" + File.separator + "test_tbl.csv",
					"-m", mapping,
					PASSWORD }));

			final StringBuilder expectedDataPart = new StringBuilder();
			for (int i = 0; i < numberOfLines; i++) {
				expectedDataPart.append(i + ";;2003-03-01;123.456;" + i + "; aBcDeF1234;2003-02-01 11:12:13.000000; aBcDeF123\n");
			}
			Assert.assertEquals(
					"id;column_blob;column_date;column_double;column_int;column_text;column_timestamp;column_varchar\n"
							+ expectedDataPart,
							exportTestTable());

			final String logData = getLogFileData();
			assertLogContains(logData, "Found items", "1200");
			assertLogContains(logData, "Valid items", "1200");
			assertLogContains(logData, "Invalid items", "0");
			assertLogContains(logData, "Imported data amount", "96,356 KiByte");
			assertLogContains(logData, "Inserted items", "1200");
		} catch (final Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testCsvImportTrimmed() {
		try {
			createEmptyTestTable();

			FileUtilities.write(INPUTFILE_CSV,
					("id;column int; column_double; column_varchar; column_text; column_timestamp; column_date\n"
							+ "123;123; 123.456; aBcDeF123; aBcDeF1234; 01.02.2003 11:12:13; 01.02.2003 21:22:23\n").getBytes(StandardCharsets.UTF_8));
			final String mapping = "id='id';column_int='column int'; column_double='column_double'; column_varchar='column_varchar'; column_text='column_text'; column_timestamp='column_timestamp'dd.MM.yyyy HH:mm:ss; column_date='column_date'dd.MM.yyyy HH:mm:ss";
			Assert.assertEquals(0, DbImport._main(new String[] {
					"cassandra",
					HOSTNAME,
					DBNAME,
					USERNAME,
					"-table", "test_tbl",
					"-i", "INSERT",
					"-l",
					"-import", "~" + File.separator + "temp" + File.separator + "test_tbl.csv",
					"-m", mapping,
					"-t",
					PASSWORD }));
			Assert.assertEquals(
					"id;column_blob;column_date;column_double;column_int;column_text;column_timestamp;column_varchar\n"
							+ "123;;2003-02-01;123.456;123;aBcDeF1234;2003-02-01 11:12:13.000000;aBcDeF123\n",
							exportTestTable());

			final String logData = getLogFileData();
			assertLogContains(logData, "Found items", "1");
			assertLogContains(logData, "Valid items", "1");
			assertLogContains(logData, "Invalid items", "0");
			assertLogContains(logData, "Imported data amount", "171 Byte");
			assertLogContains(logData, "Inserted items", "1");
		} catch (final Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testCsvImportBase64Blob() {
		try {
			createEmptyTestTable();

			FileUtilities.write(INPUTFILE_CSV,
					("id;column int; column_double; column_varchar; column_blob; column_text; column_timestamp; column_date\n"
							+ "123;123; 123.456; aBcDeF123; " + Utilities.encodeBase64("abc".getBytes(StandardCharsets.UTF_8)) + "; aBcDeF1234; 01.02.2003 11:12:13; 01.02.2003 21:22:23\n").getBytes(StandardCharsets.UTF_8));
			final String mapping = "id='id';column_int='column int'; column_double='column_double'; column_varchar='column_varchar'; column_text='column_text'; column_blob='column_blob'; column_timestamp='column_timestamp'dd.MM.yyyy HH:mm:ss; column_date='column_date'dd.MM.yyyy HH:mm:ss";
			Assert.assertEquals(0, DbImport._main(new String[] {
					"cassandra",
					HOSTNAME,
					DBNAME,
					USERNAME,
					"-table", "test_tbl",
					"-i", "INSERT",
					"-l",
					"-import", "~" + File.separator + "temp" + File.separator + "test_tbl.csv",
					"-m", mapping,
					"-t",
					PASSWORD }));
			Assert.assertEquals(
					"id;column_blob;column_date;column_double;column_int;column_text;column_timestamp;column_varchar\n"
							+ "123;YWJj;2003-02-01;123.456;123;aBcDeF1234;2003-02-01 11:12:13.000000;aBcDeF123\n",
							exportTestTable());

			final String logData = getLogFileData();
			assertLogContains(logData, "Found items", "1");
			assertLogContains(logData, "Valid items", "1");
			assertLogContains(logData, "Invalid items", "0");
			assertLogContains(logData, "Imported data amount", "190 Byte");
			assertLogContains(logData, "Inserted items", "1");
		} catch (final Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testCsvImportErrorDataType() {
		try {
			createEmptyTestTable();

			FileUtilities.write(INPUTFILE_CSV,
					("id;column int; column_double; column_varchar; column_text; column_timestamp; column_date\n"
							+ "121;121; 123.456; aBcDeF123; aBcDeF1234; 01.02.2003 11:12:13; 01.02.2003 21:22:23\n"
							+ "122;122; 123x456; aBcDeF123; aBcDeF1234; 01.02.2003 11:12:13; 01.02.2003 21:22:23\n"
							+ "123;123; 123.456; aBcDeF123; aBcDeF1234; 01.02.2003 11:12:13; 01.02.2003 21:22:23\n").getBytes(StandardCharsets.UTF_8));
			final String mapping = "id='id';column_int='column int'; column_double='column_double'; column_varchar='column_varchar'; column_text='column_text'; column_blob=; column_timestamp='column_timestamp'dd.MM.yyyy HH:mm:ss; column_date='column_date'dd.MM.yyyy HH:mm:ss";
			Assert.assertEquals(0, DbImport._main(new String[] {
					"cassandra",
					HOSTNAME,
					DBNAME,
					USERNAME,
					"-table", "test_tbl",
					"-l",
					"-import", "~" + File.separator + "temp" + File.separator + "test_tbl.csv",
					"-m", mapping,
					"-i", "INSERT",
					PASSWORD }));
			Assert.assertEquals(
					"id;column_blob;column_date;column_double;column_int;column_text;column_timestamp;column_varchar\n"
							+ "121;;2003-02-01;123.456;121; aBcDeF1234;2003-02-01 11:12:13.000000; aBcDeF123\n"
							+ "123;;2003-02-01;123.456;123; aBcDeF1234;2003-02-01 11:12:13.000000; aBcDeF123\n",
							exportTestTable());

			final String logData = getLogFileData();
			assertLogContains(logData, "Found items", "3");
			assertLogContains(logData, "Valid items", "2");
			assertLogContains(logData, "Invalid items", "1");
			assertLogContains(logData, "Indices of invalid items", "2");
			assertLogContains(logData, "Imported data amount", "335 Byte");
			assertLogContains(logData, "Inserted items", "2");
			assertLogContains(logData, "Count items after import", "2");
		} catch (final Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testCsvImportErrorStructure() {
		try {
			createEmptyTestTable();

			FileUtilities.write(INPUTFILE_CSV,
					("id;column int; column_double; column_varchar; column_text; column_timestamp; column_date\n"
							+ "121;121; 123.456; aBcDeF123; aBcDeF1234; 01.02.2003 11:12:13; 01.02.2003 21:22:23\n"
							+ "122;122; 123.456; aBcDeF123; aBcDeF1234; 01.02.2003 11:12:13; 01.02.2003 21:22:23"
							+ "123;123; 123.456; aBcDeF123; aBcDeF1234; 01.02.2003 11:12:13; 01.02.2003 21:22:23\n").getBytes(StandardCharsets.UTF_8));
			final String mapping = "id='id';column_int='column int'; column_double='column_double'; column_varchar='column_varchar'; column_text='column_text'; column_blob=; column_timestamp='column_timestamp'dd.MM.yyyy HH:mm:ss; column_date='column_date'dd.MM.yyyy HH:mm:ss";
			Assert.assertEquals(1, DbImport._main(new String[] {
					"cassandra",
					HOSTNAME,
					DBNAME,
					USERNAME,
					"-table", "test_tbl",
					"-i", "INSERT",
					"-l",
					"-import", "~" + File.separator + "temp" + File.separator + "test_tbl.csv",
					"-m", mapping,
					PASSWORD }));
			Assert.assertEquals(
					"id;column_blob;column_date;column_double;column_int;column_text;column_timestamp;column_varchar\n",
					exportTestTable());

			final String logData = getLogFileData();
			Assert.assertTrue(logData.contains("Inconsistent number of values in line 3"));
		} catch (final Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testCsvImportErrorDataTypeRollback() {
		try {
			createEmptyTestTable();

			FileUtilities.write(INPUTFILE_CSV,
					("id;column int; column_double; column_varchar; column_text; column_timestamp; column_date\n"
							+ "121;121; 123.456; aBcDeF123; aBcDeF1234; 01.02.2003 11:12:13; 01.02.2003 21:22:23\n"
							+ "122;122; 123x456; aBcDeF123; aBcDeF1234; 01.02.2003 11:12:13; 01.02.2003 21:22:23\n"
							+ "123;123; 123.456; aBcDeF123; aBcDeF1234; 01.02.2003 11:12:13; 01.02.2003 21:22:23\n").getBytes(StandardCharsets.UTF_8));
			final String mapping = "id='id';column_int='column int'; column_double='column_double'; column_varchar='column_varchar'; column_text='column_text'; column_blob=; column_timestamp='column_timestamp'dd.MM.yyyy HH:mm:ss; column_date='column_date'dd.MM.yyyy HH:mm:ss";
			Assert.assertEquals(1, DbImport._main(new String[] {
					"cassandra",
					HOSTNAME,
					DBNAME,
					USERNAME,
					"-table", "test_tbl",
					"-i", "INSERT",
					"-l",
					"-import", "~" + File.separator + "temp" + File.separator + "test_tbl.csv",
					"-m", mapping,
					"-c",
					PASSWORD }));
			Assert.assertEquals(
					"id;column_blob;column_date;column_double;column_int;column_text;column_timestamp;column_varchar\n",
					exportTestTable());

			// Only commitOnFullSuccessOnly mode 'always commit' is allowed for this NoSql import
		} catch (final Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testCsvImportInsertWithKeycolumns() {
		try {
			createEmptyTestTable();
			prefillTestTable();

			final StringBuilder data = new StringBuilder();
			data.append("id;column int; column_double; column_varchar; column_text; column_timestamp; column_date\n");
			data.append("1;1; 123.456; aBcDeF123; aBcDeF1234; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("1;1; 123.456;; aBcDeF1235; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("2;2; 123.456; aBcDeF123; aBcDeF1234; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("2;2; 123.456;; aBcDeF1235; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("3;3; 123.456; aBcDeF123; aBcDeF1234; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("3;3; 123.456;; aBcDeF1235; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("4;4; 123.456;; aBcDeF1235; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			FileUtilities.write(INPUTFILE_CSV, data.toString().getBytes(StandardCharsets.UTF_8));

			final String mapping = "id='id';column_int='column int'; column_double='column_double'; column_varchar='column_varchar'; column_text='column_text'; column_blob=; column_timestamp='column_timestamp'dd.MM.yyyy HH:mm:ss; column_date='column_date'dd.MM.yyyy HH:mm:ss";
			Assert.assertEquals(0, DbImport._main(new String[] {
					"cassandra",
					HOSTNAME,
					DBNAME,
					USERNAME,
					"-table", "test_tbl",
					"-l",
					"-import", "~" + File.separator + "temp" + File.separator + "test_tbl.csv",
					"-m", mapping,
					"-i", "INSERT",
					"-k", "id",
					PASSWORD }));
			Assert.assertEquals(
					"id;column_blob;column_date;column_double;column_int;column_text;column_timestamp;column_varchar\n"
							+ "1;;;;1;;;\"<test_text>_1\"\n"
							+ "2;;2003-03-01;123.456;2; aBcDeF1234;2003-02-01 11:12:13.000000; aBcDeF123\n"
							+ "3;;;;3;;;\"<test_text>_3\"\n"
							+ "4;;2003-03-01;123.456;4; aBcDeF1235;2003-02-01 11:12:13.000000;\n"
							+ "999;;;;999;;;\"<test_text>_999\"\n",
							exportTestTable());

			final String logData = getLogFileData();
			assertLogContains(logData, "Found items", "7");
			assertLogContains(logData, "Valid items", "7");
			assertLogContains(logData, "Invalid items", "0");
			assertLogContains(logData, "Imported data amount", "595 Byte");
			assertLogContains(logData, "Inserted items", "2");
		} catch (final Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testCsvImportUpdateWithNull() {
		try {
			createEmptyTestTable();
			prefillTestTable();

			final StringBuilder data = new StringBuilder();
			data.append("id;column int; column_double; column_varchar; column_text; column_timestamp; column_date\n");
			data.append("1;1; 123.456; aBcDeF123_1; aBcDeF1234; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("1;1; 123.456;; aBcDeF1235_1; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("2;2; 123.456; aBcDeF123_2; aBcDeF1234; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("2;2; 123.456;; aBcDeF1235_2; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("3;3; 123.456; aBcDeF123_3; aBcDeF1234; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("3;3; 123.456;; aBcDeF1235_3; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("4;4; 123.456;; aBcDeF1235_4; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("5;5; 123.456; aBcDeF123_5; aBcDeF1234; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			FileUtilities.write(INPUTFILE_CSV, data.toString().getBytes(StandardCharsets.UTF_8));

			final String mapping = "id='id';column_int='column int'; column_double='column_double'; column_varchar='column_varchar'; column_text='column_text'; column_blob=; column_timestamp='column_timestamp'dd.MM.yyyy HH:mm:ss; column_date='column_date'dd.MM.yyyy HH:mm:ss";
			Assert.assertEquals(0, DbImport._main(new String[] {
					"cassandra",
					HOSTNAME,
					DBNAME,
					USERNAME,
					"-table", "test_tbl",
					"-l",
					"-import", "~" + File.separator + "temp" + File.separator + "test_tbl.csv",
					"-m", mapping,
					"-i", "UPDATE",
					"-k", "id",
					PASSWORD }));
			Assert.assertEquals(
					"id;column_blob;column_date;column_double;column_int;column_text;column_timestamp;column_varchar\n"
							+ "1;;2003-03-01;123.456;1; aBcDeF1235_1;2003-02-01 11:12:13.000000;\n"
							+ "3;;2003-03-01;123.456;3; aBcDeF1235_3;2003-02-01 11:12:13.000000;\n"
							+ "999;;;;999;;;\"<test_text>_999\"\n",
							exportTestTable());

			final String logData = getLogFileData();
			assertLogContains(logData, "Found items", "8");
			assertLogContains(logData, "Valid items", "8");
			assertLogContains(logData, "Invalid items", "0");
			assertLogContains(logData, "Imported data amount", "689 Byte");
			assertLogContains(logData, "Updated items", "4");
		} catch (final Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testCsvImportUpdateWithoutNull() {
		try {
			createEmptyTestTable();
			prefillTestTable();

			final StringBuilder data = new StringBuilder();
			data.append("id;column int; column_double; column_varchar; column_text; column_timestamp; column_date\n");
			data.append("1;1; 123.456; aBcDeF123_1; aBcDeF1234; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("1;1; 123.456;; aBcDeF1235_11; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("2;2; 123.456; aBcDeF123_2; aBcDeF1234; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("2;2; 123.456;; aBcDeF1235_21; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("3;3; 123.456; aBcDeF123_3; aBcDeF1234; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("3;3; 123.456;; aBcDeF1235_31; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("4;4; 123.456;; aBcDeF1235_41; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("5;5; 123.456; aBcDeF123_5; aBcDeF1234; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			FileUtilities.write(INPUTFILE_CSV, data.toString().getBytes(StandardCharsets.UTF_8));

			final String mapping = "id='id';column_int='column int'; column_double='column_double'; column_varchar='column_varchar'; column_text='column_text'; column_blob=; column_timestamp='column_timestamp'dd.MM.yyyy HH:mm:ss; column_date='column_date'dd.MM.yyyy HH:mm:ss";
			Assert.assertEquals(1, DbImport._main(new String[] {
					"cassandra",
					HOSTNAME,
					DBNAME,
					USERNAME,
					"-table", "test_tbl",
					"-l",
					"-import", "~" + File.separator + "temp" + File.separator + "test_tbl.csv",
					"-m", mapping,
					"-i", "UPDATE",
					"-k", "id",
					"-u",
					PASSWORD }));
			// Only UpdateNullData mode 'Update with null values' is allowed for this NoSql import
		} catch (final Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testCsvImportUpsertWithNull() {
		try {
			createEmptyTestTable();
			prefillTestTable();

			final StringBuilder data = new StringBuilder();
			data.append("id;column int; column_double; column_varchar; column_text; column_timestamp; column_date\n");
			data.append("1;1; 123.456; aBcDeF123_1; aBcDeF1234; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("1;1; 123.456;; aBcDeF1235_1; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("2;2; 123.456; aBcDeF123_2; aBcDeF1234; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("2;2; 123.456;; aBcDeF1235_2; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("3;3; 123.456; aBcDeF123_3; aBcDeF1234; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("3;3; 123.456;; aBcDeF1235_3; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("4;4; 123.456;; aBcDeF1235_4; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("5;5; 123.456; aBcDeF123_5; aBcDeF1234; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			FileUtilities.write(INPUTFILE_CSV, data.toString().getBytes(StandardCharsets.UTF_8));

			final String mapping = "id='id';column_int='column int'; column_double='column_double'; column_varchar='column_varchar'; column_text='column_text'; column_blob=; column_timestamp='column_timestamp'dd.MM.yyyy HH:mm:ss; column_date='column_date'dd.MM.yyyy HH:mm:ss";
			Assert.assertEquals(0, DbImport._main(new String[] {
					"cassandra",
					HOSTNAME,
					DBNAME,
					USERNAME,
					"-table", "test_tbl",
					"-l",
					"-import", "~" + File.separator + "temp" + File.separator + "test_tbl.csv",
					"-m", mapping,
					"-i", "UPSERT",
					"-k", "id",
					PASSWORD }));
			Assert.assertEquals(
					"id;column_blob;column_date;column_double;column_int;column_text;column_timestamp;column_varchar\n"
							+ "1;;2003-03-01;123.456;1; aBcDeF1235_1;2003-02-01 11:12:13.000000;\n"
							+ "2;;2003-03-01;123.456;2; aBcDeF1235_2;2003-02-01 11:12:13.000000;\n"
							+ "3;;2003-03-01;123.456;3; aBcDeF1235_3;2003-02-01 11:12:13.000000;\n"
							+ "4;;2003-03-01;123.456;4; aBcDeF1235_4;2003-02-01 11:12:13.000000;\n"
							+ "5;;2003-03-01;123.456;5; aBcDeF1234;2003-02-01 11:12:13.000000; aBcDeF123_5\n"
							+ "999;;;;999;;;\"<test_text>_999\"\n",
							exportTestTable());

			final String logData = getLogFileData();
			assertLogContains(logData, "Found items", "8");
			assertLogContains(logData, "Valid items", "8");
			assertLogContains(logData, "Invalid items", "0");
			assertLogContains(logData, "Imported data amount", "689 Byte");
			assertLogContains(logData, "Inserted items", "3");
			assertLogContains(logData, "Updated items", "5");
		} catch (final Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testCsvImportUpsertWithoutNull() {
		try {
			createEmptyTestTable();
			prefillTestTable();

			final StringBuilder data = new StringBuilder();
			data.append("id;column int; column_double; column_varchar; column_text; column_timestamp; column_date\n");
			data.append("1;1; 123.456; aBcDeF123_1; aBcDeF1234; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("1;1; 123.456;; aBcDeF1235_1; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("2;2; 123.456; aBcDeF123_2; aBcDeF1234; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("2;2; 123.456;; aBcDeF1235_2; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("3;3; 123.456; aBcDeF123_3; aBcDeF1234; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("3;3; 123.456;; aBcDeF1235_3; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("4;4; 123.456;; aBcDeF1235_4; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("5;5; 123.456; aBcDeF123_5; aBcDeF1234; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			FileUtilities.write(INPUTFILE_CSV, data.toString().getBytes(StandardCharsets.UTF_8));

			final String mapping = "id='id';column_int='column int'; column_double='column_double'; column_varchar='column_varchar'; column_text='column_text'; column_blob=; column_timestamp='column_timestamp'dd.MM.yyyy HH:mm:ss; column_date='column_date'dd.MM.yyyy HH:mm:ss";
			Assert.assertEquals(1, DbImport._main(new String[] {
					"cassandra",
					HOSTNAME,
					DBNAME,
					USERNAME,
					"-table", "test_tbl",
					"-l",
					"-import", "~" + File.separator + "temp" + File.separator + "test_tbl.csv",
					"-m", mapping,
					"-i", "UPSERT",
					"-k", "id",
					"-u",
					PASSWORD }));
			// Only UpdateNullData mode 'Update with null values' is allowed for this NoSql import
		} catch (final Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testCsvImportUpsertAdditionalValues() {
		try {
			createEmptyTestTable();
			prefillTestTable();

			final StringBuilder data = new StringBuilder();
			data.append("id;column int;column_text\n");
			data.append("1;1;Original1\n");
			data.append("1;1;Original2\n");
			data.append("2;2;Original1\n");
			data.append("2;2;Original2\n");
			data.append("3;3;Original1\n");
			data.append("3;3;Original2\n");
			data.append("4;4;Original\n");
			FileUtilities.write(INPUTFILE_CSV, data.toString().getBytes(StandardCharsets.UTF_8));

			final String mapping = "id='id';column_int='column int';column_text='column_text'";
			Assert.assertEquals(0, DbImport._main(new String[] {
					"cassandra",
					HOSTNAME,
					DBNAME,
					USERNAME,
					"-table", "test_tbl",
					"-l",
					"-import", "~" + File.separator + "temp" + File.separator + "test_tbl.csv",
					"-m", mapping,
					"-i", "UPSERT",
					"-k", "id",
					"-insvalues", "column_varchar='Insert'",
					"-updvalues", "column_varchar='Update'",
					PASSWORD }));
			Assert.assertEquals(
					"id;column_blob;column_date;column_double;column_int;column_text;column_timestamp;column_varchar\n"
							+ "1;;;;1;Original2;;Update\n"
							+ "2;;;;2;Original2;;Update\n"
							+ "3;;;;3;Original2;;Update\n"
							+ "4;;;;4;Original;;Insert\n"
							+ "999;;;;999;;;\"<test_text>_999\"\n",
							exportTestTable());

			final String logData = getLogFileData();
			assertLogContains(logData, "Found items", "7");
			assertLogContains(logData, "Valid items", "7");
			assertLogContains(logData, "Invalid items", "0");
			assertLogContains(logData, "Imported data amount", "123 Byte");
			assertLogContains(logData, "Inserted items", "2");
			assertLogContains(logData, "Updated items", "5");
		} catch (final Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testCsvImportInsertFile() {
		try {
			createEmptyTestTable();
			prefillTestTable();

			FileUtilities.write(BLOB_DATA_FILE, TextUtilities.GERMAN_TEST_STRING.getBytes(StandardCharsets.UTF_8));

			final StringBuilder data = new StringBuilder();
			data.append("id;column int;column_text\n");
			data.append("1;1;" + BLOB_DATA_FILE.getAbsolutePath() + "\n");
			FileUtilities.write(INPUTFILE_CSV, data.toString().getBytes(StandardCharsets.UTF_8));

			final String mapping = "id='id';column_int='column int';column_text='column_text' file";
			Assert.assertEquals(0, DbImport._main(new String[] {
					"cassandra",
					HOSTNAME,
					DBNAME,
					USERNAME,
					"-table", "test_tbl",
					"-l",
					"-import", "~" + File.separator + "temp" + File.separator + "test_tbl.csv",
					"-m", mapping,
					"-i", "UPSERT",
					"-k", "id",
					"-insvalues", "column_varchar='Insert'",
					"-updvalues", "column_varchar='Update'",
					PASSWORD }));
			Assert.assertEquals(
					"id;column_blob;column_date;column_double;column_int;column_text;column_timestamp;column_varchar\n"
							+ "1;;;;1;\"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 äöüßÄÖÜµ!?§@€$%&/\\<>(){}[]'\"\"´`^°¹²³*#.,;:=+-~_|½¼¬\";;Update\n"
							+ "3;;;;3;;;\"<test_text>_3\"\n"
							+ "999;;;;999;;;\"<test_text>_999\"\n",
							exportTestTable());

			final String logData = getLogFileData();
			assertLogContains(logData, "Found items", "1");
			assertLogContains(logData, "Valid items", "1");
			assertLogContains(logData, "Invalid items", "0");
			assertLogContains(logData, "Imported data amount", "194 Byte");
			assertLogContains(logData, "Inserted items", "0");
			assertLogContains(logData, "Updated items", "1");
		} catch (final Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testCsvImportInsertBlobFile() {
		try {
			createEmptyTestTable();
			prefillTestTable();

			FileUtilities.write(BLOB_DATA_FILE, TextUtilities.GERMAN_TEST_STRING.getBytes(StandardCharsets.UTF_8));

			final StringBuilder data = new StringBuilder();
			data.append("id;column int;column_blob\n");
			data.append("1;1;" + BLOB_DATA_FILE.getAbsolutePath() + "\n");
			FileUtilities.write(INPUTFILE_CSV, data.toString().getBytes(StandardCharsets.UTF_8));

			final String mapping = "id='id';column_int='column int';column_blob='column_blob' file";
			Assert.assertEquals(0, DbImport._main(new String[] {
					"cassandra",
					HOSTNAME,
					DBNAME,
					USERNAME,
					"-table", "test_tbl",
					"-l",
					"-import", "~" + File.separator + "temp" + File.separator + "test_tbl.csv",
					"-m", mapping,
					"-i", "UPSERT",
					"-k", "id",
					"-insvalues", "column_varchar='Insert'",
					"-updvalues", "column_varchar='Update'",
					PASSWORD }));
			Assert.assertEquals(
					"id;column_blob;column_date;column_double;column_int;column_text;column_timestamp;column_varchar\n"
							+ "1;YWJjZGVmZ2hpamtsbW5vcHFyc3R1dnd4eXpBQkNERUZHSElKS0xNTk9QUVJTVFVWV1hZWjAxMjM0NTY3ODkgw6TDtsO8w5/DhMOWw5zCtSE/wqdA4oKsJCUmL1w8Pigpe31bXSciwrRgXsKwwrnCssKzKiMuLDs6PSstfl98wr3CvMKs;;;1;;;Update\n"
							+ "3;;;;3;;;\"<test_text>_3\"\n"
							+ "999;;;;999;;;\"<test_text>_999\"\n",
							exportTestTable());

			final String logData = getLogFileData();
			assertLogContains(logData, "Found items", "1");
			assertLogContains(logData, "Valid items", "1");
			assertLogContains(logData, "Invalid items", "0");
			assertLogContains(logData, "Imported data amount", "194 Byte");
			assertLogContains(logData, "Inserted items", "0");
			assertLogContains(logData, "Updated items", "1");
		} catch (final Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testCsvImportCreateTable() {
		try {
			FileUtilities.write(BLOB_DATA_FILE, TextUtilities.GERMAN_TEST_STRING.getBytes(StandardCharsets.UTF_8));

			final StringBuilder data = new StringBuilder();
			data.append("id;column int;column_varchar;column_double;not%included\n");
			data.append("1;001;AbcÄ123;1.2300;not Included\n");
			FileUtilities.write(INPUTFILE_CSV, data.toString().getBytes(StandardCharsets.UTF_8));

			final String mapping = "id='id';column_int='column int';column_varchar='column_varchar';column_double='column_double'";
			Assert.assertEquals(0, DbImport._main(new String[] {
					"cassandra",
					HOSTNAME,
					DBNAME,
					USERNAME,
					"-create",
					"-table", "test_tbl",
					"-l",
					"-import", "~" + File.separator + "temp" + File.separator + "test_tbl.csv",
					"-m", mapping,
					"-i", "UPSERT",
					"-k", "id",
					PASSWORD }));
			Assert.assertEquals(
					"id;column_double;column_int;column_varchar\n"
							+ "1;1.23;1;AbcÄ123\n",
							exportTestTable());

			final String logData = getLogFileData();
			assertLogContains(logData, "Found items", "1");
			assertLogContains(logData, "Valid items", "1");
			assertLogContains(logData, "Invalid items", "0");
			assertLogContains(logData, "Imported data amount", "91 Byte");
			assertLogContains(logData, "Inserted items", "1");
			assertLogContains(logData, "Updated items", "0");
		} catch (final Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testCsvImportCreateTableLowerCased() {
		try {
			FileUtilities.write(BLOB_DATA_FILE, TextUtilities.GERMAN_TEST_STRING.getBytes(StandardCharsets.UTF_8));

			final StringBuilder data = new StringBuilder();
			data.append("id;column int;column_varchar;column_double;not%included\n");
			data.append("1;001;AbcÄ123;1.2300;not Included\n");
			FileUtilities.write(INPUTFILE_CSV, data.toString().getBytes(StandardCharsets.UTF_8));

			final String mapping = "id='id';column_int='column int';column_varchar='column_varchar' lc;column_double='column_double'";
			Assert.assertEquals(0, DbImport._main(new String[] {
					"cassandra",
					HOSTNAME,
					DBNAME,
					USERNAME,
					"-create",
					"-table", "test_tbl",
					"-l",
					"-import", "~" + File.separator + "temp" + File.separator + "test_tbl.csv",
					"-m", mapping,
					"-i", "UPSERT",
					"-k", "id",
					PASSWORD }));
			Assert.assertEquals(
					"id;column_double;column_int;column_varchar\n"
							+ "1;1.23;1;abcä123\n",
							exportTestTable());

			final String logData = getLogFileData();
			assertLogContains(logData, "Found items", "1");
			assertLogContains(logData, "Valid items", "1");
			assertLogContains(logData, "Invalid items", "0");
			assertLogContains(logData, "Imported data amount", "91 Byte");
			assertLogContains(logData, "Inserted items", "1");
			assertLogContains(logData, "Updated items", "0");
		} catch (final Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testCsvImportCreateTableCaseinsensitiveKey() {
		try {
			FileUtilities.write(BLOB_DATA_FILE, TextUtilities.GERMAN_TEST_STRING.getBytes(StandardCharsets.UTF_8));

			final StringBuilder data = new StringBuilder();
			data.append("id;column int;column_varchar;column_double;not%included\n");
			data.append("1;001;AbcÄ123;1.2300;not Included\n");
			data.append("2;002;ABCÄ123;1.2300;not Included\n");
			data.append("3;003;abcä123;1.2300;not Included\n");
			FileUtilities.write(INPUTFILE_CSV, data.toString().getBytes(StandardCharsets.UTF_8));

			final String mapping = "id='id';column_int='column int';column_varchar='column_varchar' lc;column_double='column_double'";
			Assert.assertEquals(0, DbImport._main(new String[] {
					"cassandra",
					HOSTNAME,
					DBNAME,
					USERNAME,
					"-create",
					"-table", "test_tbl",
					"-l",
					"-import", "~" + File.separator + "temp" + File.separator + "test_tbl.csv",
					"-m", mapping,
					"-i", "UPSERT",
					"-k", "lower(column_varchar)",
					PASSWORD }));
			Assert.assertEquals(
					"column_varchar;column_double;column_int;id\n"
							+ "abcä123;1.23;3;3\n",
							exportTestTable());

			final String logData = getLogFileData();
			assertLogContains(logData, "Found items", "3");
			assertLogContains(logData, "Valid items", "3");
			assertLogContains(logData, "Invalid items", "0");
			assertLogContains(logData, "Imported data amount", "161 Byte");
			assertLogContains(logData, "Inserted items", "1");
			assertLogContains(logData, "Updated items", "2");
		} catch (final Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testCsvImportCreateTableNoMapping() {
		try {
			FileUtilities.write(BLOB_DATA_FILE, TextUtilities.GERMAN_TEST_STRING.getBytes(StandardCharsets.UTF_8));

			final StringBuilder data = new StringBuilder();
			data.append("id;column_int;column_varchar;column_double\n");
			data.append("1;001;AbcÄ123;1.2300\n");
			FileUtilities.write(INPUTFILE_CSV, data.toString().getBytes(StandardCharsets.UTF_8));

			Assert.assertEquals(0, DbImport._main(new String[] {
					"cassandra",
					HOSTNAME,
					DBNAME,
					USERNAME,
					"-create",
					"-table", "test_tbl",
					"-l",
					"-import", "~" + File.separator + "temp" + File.separator + "test_tbl.csv",
					"-i", "UPSERT",
					"-k", "id",
					PASSWORD }));
			Assert.assertEquals(
					"id;column_double;column_int;column_varchar\n"
							+ "1;1.23;1;AbcÄ123\n",
							exportTestTable());

			final String logData = getLogFileData();
			assertLogContains(logData, "Found items", "1");
			assertLogContains(logData, "Valid items", "1");
			assertLogContains(logData, "Invalid items", "0");
			assertLogContains(logData, "Imported data amount", "65 Byte");
			assertLogContains(logData, "Inserted items", "1");
			assertLogContains(logData, "Updated items", "0");
		} catch (final Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testCsvImportCreateTableNoMappingNoKey() {
		try {
			FileUtilities.write(BLOB_DATA_FILE, TextUtilities.GERMAN_TEST_STRING.getBytes(StandardCharsets.UTF_8));

			final StringBuilder data = new StringBuilder();
			data.append(BOM.BOM_UTF_8_CHAR + "id;column_int;column_varchar;column_double\n");
			data.append("1;001;AbcÄ123;1.2300\n");
			FileUtilities.write(INPUTFILE_CSV, data.toString().getBytes(StandardCharsets.UTF_8));

			Assert.assertEquals(1, DbImport._main(new String[] {
					"cassandra",
					HOSTNAME,
					DBNAME,
					USERNAME,
					"-create",
					"-table", "test_tbl",
					"-l",
					"-import", "~" + File.separator + "temp" + File.separator + "test_tbl.csv",
					"-i", "INSERT",
					PASSWORD }));
			// Creation of new table needs key column definition in NoSql import
		} catch (final Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testJsonImportUpsert() {
		JsonWriter jsonWriter = null;
		try {
			createEmptyTestTable();
			prefillTestTable();

			final JsonArray jsonArray = new JsonArray();

			JsonObject jsonObject = new JsonObject();
			jsonObject.add("id", 1);
			jsonObject.add("column int", 1);
			jsonObject.add("column_double", 123.456);
			jsonObject.add("column_varchar", " aBcDeF123_1");
			jsonObject.add("column_text", " aBcDeF1234");
			jsonObject.add("column_timestamp", "01.02.2003 11:12:13");
			jsonObject.add("column_date", " 01.03.2003 21:22:23");
			jsonArray.add(jsonObject);

			jsonObject = new JsonObject();
			jsonObject.add("id", 1);
			jsonObject.add("column int", 1);
			jsonObject.add("column_double", 123.456);
			jsonObject.add("column_varchar", null);
			jsonObject.add("column_text", " aBcDeF1235_1");
			jsonObject.add("column_timestamp", "01.02.2003 11:12:13");
			jsonObject.add("column_date", " 01.03.2003 21:22:23");
			jsonArray.add(jsonObject);

			jsonObject = new JsonObject();
			jsonObject.add("id", 2);
			jsonObject.add("column int", 2);
			jsonObject.add("column_double", 123.456);
			jsonObject.add("column_varchar", " aBcDeF123_2");
			jsonObject.add("column_text", " aBcDeF1234");
			jsonObject.add("column_timestamp", "01.02.2003 11:12:13");
			jsonObject.add("column_date", " 01.03.2003 21:22:23");
			jsonArray.add(jsonObject);

			jsonObject = new JsonObject();
			jsonObject.add("id", 2);
			jsonObject.add("column int", 2);
			jsonObject.add("column_double", 123.456);
			jsonObject.add("column_varchar", null);
			jsonObject.add("column_text", " aBcDeF1235_2");
			jsonObject.add("column_timestamp", "01.02.2003 11:12:13");
			jsonObject.add("column_date", " 01.03.2003 21:22:23");
			jsonArray.add(jsonObject);

			jsonObject = new JsonObject();
			jsonObject.add("id", 3);
			jsonObject.add("column int", 3);
			jsonObject.add("column_double", 123.456);
			jsonObject.add("column_varchar", " aBcDeF123_3");
			jsonObject.add("column_text", " aBcDeF1234");
			jsonObject.add("column_timestamp", "01.02.2003 11:12:13");
			jsonObject.add("column_date", " 01.03.2003 21:22:23");
			jsonArray.add(jsonObject);

			jsonObject = new JsonObject();
			jsonObject.add("id", 3);
			jsonObject.add("column int", 3);
			jsonObject.add("column_double", 123.456);
			jsonObject.add("column_varchar", null);
			jsonObject.add("column_text", " aBcDeF1235_3");
			jsonObject.add("column_timestamp", "01.02.2003 11:12:13");
			jsonObject.add("column_date", " 01.03.2003 21:22:23");
			jsonArray.add(jsonObject);

			jsonObject = new JsonObject();
			jsonObject.add("id", 4);
			jsonObject.add("column int", 4);
			jsonObject.add("column_double", 123.456);
			jsonObject.add("column_varchar", null);
			jsonObject.add("column_text", " aBcDeF1235_4");
			jsonObject.add("column_timestamp", "01.02.2003 11:12:13");
			jsonObject.add("column_date", " 01.03.2003 21:22:23");
			jsonArray.add(jsonObject);

			jsonObject = new JsonObject();
			jsonObject.add("id", 5);
			jsonObject.add("column int", 5);
			jsonObject.add("column_double", 123.456);
			jsonObject.add("column_varchar", " aBcDeF123_5");
			jsonObject.add("column_text", " aBcDeF1234");
			jsonObject.add("column_timestamp", "01.02.2003 11:12:13");
			jsonObject.add("column_date", " 01.03.2003 21:22:23");
			jsonArray.add(jsonObject);

			jsonWriter = new JsonWriter(new FileOutputStream(INPUTFILE_JSON), StandardCharsets.UTF_8);
			jsonWriter.add(jsonArray);
			Utilities.closeQuietly(jsonWriter);
			jsonWriter = null;

			final String mapping = "id='id';column_int='column int'; column_double='column_double'; column_varchar='column_varchar'; column_text='column_text'; column_blob=; column_timestamp='column_timestamp'dd.MM.yyyy HH:mm:ss; column_date='column_date'dd.MM.yyyy HH:mm:ss";
			Assert.assertEquals(0, DbImport._main(new String[] {
					"cassandra",
					HOSTNAME,
					DBNAME,
					USERNAME,
					"-table", "test_tbl",
					"-l",
					"-x", "JSON",
					"-import", "~" + File.separator + "temp" + File.separator + "test_tbl.json",
					"-m", mapping,
					"-i", "UPSERT",
					"-k", "id",
					PASSWORD }));
			Assert.assertEquals(
					"id;column_blob;column_date;column_double;column_int;column_text;column_timestamp;column_varchar\n"
							+ "1;;2003-03-01;123.456;1; aBcDeF1235_1;2003-02-01 11:12:13.000000;\n"
							+ "2;;2003-03-01;123.456;2; aBcDeF1235_2;2003-02-01 11:12:13.000000;\n"
							+ "3;;2003-03-01;123.456;3; aBcDeF1235_3;2003-02-01 11:12:13.000000;\n"
							+ "4;;2003-03-01;123.456;4; aBcDeF1235_4;2003-02-01 11:12:13.000000;\n"
							+ "5;;2003-03-01;123.456;5; aBcDeF1234;2003-02-01 11:12:13.000000; aBcDeF123_5\n"
							+ "999;;;;999;;;\"<test_text>_999\"\n",
							exportTestTable());

			final String logData = getLogFileData();
			assertLogContains(logData, "Found items", "8");
			assertLogContains(logData, "Valid items", "8");
			assertLogContains(logData, "Invalid items", "0");
			assertLogContains(logData, "Imported data amount", "1,6738 KiByte");
			assertLogContains(logData, "Inserted items", "3");
			assertLogContains(logData, "Updated items", "5");
		} catch (final Exception e) {
			Assert.fail(e.getMessage());
		} finally {
			Utilities.closeQuietly(jsonWriter);
		}
	}

	@Test
	public void testXmlImportUpsert() {
		final JsonWriter jsonWriter = null;
		try {
			createEmptyTestTable();
			prefillTestTable();

			final Element xmlRootTag = XmlUtilities.createRootTagNode(XmlUtilities.getEmptyDocument(), "Entries");
			Element entryNode;

			entryNode = XmlUtilities.appendNode(xmlRootTag, "Entry");
			XmlUtilities.appendTextValueNode(entryNode, "id", 1);
			XmlUtilities.appendTextValueNode(entryNode, "column_int", 1);
			XmlUtilities.appendTextValueNode(entryNode, "column_double", 123.456);
			XmlUtilities.appendTextValueNode(entryNode, "column_varchar", " aBcDeF123_1");
			XmlUtilities.appendTextValueNode(entryNode, "column_text", " aBcDeF1234");
			XmlUtilities.appendTextValueNode(entryNode, "column_timestamp", "01.02.2003 11:12:13");
			XmlUtilities.appendTextValueNode(entryNode, "column_date", " 01.03.2003 21:22:23");

			entryNode = XmlUtilities.appendNode(xmlRootTag, "Entry");
			XmlUtilities.appendTextValueNode(entryNode, "id", 1);
			XmlUtilities.appendTextValueNode(entryNode, "column_int", 1);
			XmlUtilities.appendTextValueNode(entryNode, "column_double", 123.456);
			XmlUtilities.appendTextValueNode(entryNode, "column_varchar", "");
			XmlUtilities.appendTextValueNode(entryNode, "column_text", " aBcDeF1235_1");
			XmlUtilities.appendTextValueNode(entryNode, "column_timestamp", "01.02.2003 11:12:13");
			XmlUtilities.appendTextValueNode(entryNode, "column_date", " 01.03.2003 21:22:23");

			entryNode = XmlUtilities.appendNode(xmlRootTag, "Entry");
			XmlUtilities.appendTextValueNode(entryNode, "id", 2);
			XmlUtilities.appendTextValueNode(entryNode, "column_int", 2);
			XmlUtilities.appendTextValueNode(entryNode, "column_double", 123.456);
			XmlUtilities.appendTextValueNode(entryNode, "column_varchar", " aBcDeF123_2");
			XmlUtilities.appendTextValueNode(entryNode, "column_text", " aBcDeF1234");
			XmlUtilities.appendTextValueNode(entryNode, "column_timestamp", "01.02.2003 11:12:13");
			XmlUtilities.appendTextValueNode(entryNode, "column_date", " 01.03.2003 21:22:23");

			entryNode = XmlUtilities.appendNode(xmlRootTag, "Entry");
			XmlUtilities.appendTextValueNode(entryNode, "id", 2);
			XmlUtilities.appendTextValueNode(entryNode, "column_int", 2);
			XmlUtilities.appendTextValueNode(entryNode, "column_double", 123.456);
			XmlUtilities.appendTextValueNode(entryNode, "column_varchar", "");
			XmlUtilities.appendTextValueNode(entryNode, "column_text", " aBcDeF1235_2");
			XmlUtilities.appendTextValueNode(entryNode, "column_timestamp", "01.02.2003 11:12:13");
			XmlUtilities.appendTextValueNode(entryNode, "column_date", " 01.03.2003 21:22:23");

			entryNode = XmlUtilities.appendNode(xmlRootTag, "Entry");
			XmlUtilities.appendTextValueNode(entryNode, "id", 3);
			XmlUtilities.appendTextValueNode(entryNode, "column_int", 3);
			XmlUtilities.appendTextValueNode(entryNode, "column_double", 123.456);
			XmlUtilities.appendTextValueNode(entryNode, "column_varchar", " aBcDeF123_3");
			XmlUtilities.appendTextValueNode(entryNode, "column_text", " aBcDeF1234");
			XmlUtilities.appendTextValueNode(entryNode, "column_timestamp", "01.02.2003 11:12:13");
			XmlUtilities.appendTextValueNode(entryNode, "column_date", " 01.03.2003 21:22:23");

			entryNode = XmlUtilities.appendNode(xmlRootTag, "Entry");
			XmlUtilities.appendTextValueNode(entryNode, "id", 3);
			XmlUtilities.appendTextValueNode(entryNode, "column_int", 3);
			XmlUtilities.appendTextValueNode(entryNode, "column_double", 123.456);
			XmlUtilities.appendTextValueNode(entryNode, "column_varchar", "");
			XmlUtilities.appendTextValueNode(entryNode, "column_text", " aBcDeF1235_3");
			XmlUtilities.appendTextValueNode(entryNode, "column_timestamp", "01.02.2003 11:12:13");
			XmlUtilities.appendTextValueNode(entryNode, "column_date", " 01.03.2003 21:22:23");

			entryNode = XmlUtilities.appendNode(xmlRootTag, "Entry");
			XmlUtilities.appendTextValueNode(entryNode, "id", 4);
			XmlUtilities.appendTextValueNode(entryNode, "column_int", 4);
			XmlUtilities.appendTextValueNode(entryNode, "column_double", 123.456);
			XmlUtilities.appendTextValueNode(entryNode, "column_varchar", "");
			XmlUtilities.appendTextValueNode(entryNode, "column_text", " aBcDeF1235_4");
			XmlUtilities.appendTextValueNode(entryNode, "column_timestamp", "01.02.2003 11:12:13");
			XmlUtilities.appendTextValueNode(entryNode, "column_date", " 01.03.2003 21:22:23");

			entryNode = XmlUtilities.appendNode(xmlRootTag, "Entry");
			XmlUtilities.appendTextValueNode(entryNode, "id", 5);
			XmlUtilities.appendTextValueNode(entryNode, "column_int", 5);
			XmlUtilities.appendTextValueNode(entryNode, "column_double", 123.456);
			XmlUtilities.appendTextValueNode(entryNode, "column_varchar", " aBcDeF123_5");
			XmlUtilities.appendTextValueNode(entryNode, "column_text", " aBcDeF1234");
			XmlUtilities.appendTextValueNode(entryNode, "column_timestamp", "01.02.2003 11:12:13");
			XmlUtilities.appendTextValueNode(entryNode, "column_date", " 01.03.2003 21:22:23");

			FileUtilities.write(INPUTFILE_XML, XmlUtilities.convertXML2ByteArray(xmlRootTag, StandardCharsets.UTF_8));

			final String mapping = "id='id';column_int='column_int'; column_double='column_double'; column_varchar='column_varchar'; column_text='column_text'; column_blob=; column_timestamp='column_timestamp'dd.MM.yyyy HH:mm:ss; column_date='column_date'dd.MM.yyyy HH:mm:ss";
			Assert.assertEquals(0, DbImport._main(new String[] {
					"cassandra",
					HOSTNAME,
					DBNAME,
					USERNAME,
					"-table", "test_tbl",
					"-l",
					"-x", "XML",
					"-import", "~" + File.separator + "temp" + File.separator + "test_tbl.xml",
					"-m", mapping,
					"-i", "UPSERT",
					"-k", "id",
					PASSWORD }));
			Assert.assertEquals(
					"id;column_blob;column_date;column_double;column_int;column_text;column_timestamp;column_varchar\n"
							+ "1;;2003-03-01;123.456;1; aBcDeF1235_1;2003-02-01 11:12:13.000000;\n"
							+ "2;;2003-03-01;123.456;2; aBcDeF1235_2;2003-02-01 11:12:13.000000;\n"
							+ "3;;2003-03-01;123.456;3; aBcDeF1235_3;2003-02-01 11:12:13.000000;\n"
							+ "4;;2003-03-01;123.456;4; aBcDeF1235_4;2003-02-01 11:12:13.000000;\n"
							+ "5;;2003-03-01;123.456;5; aBcDeF1234;2003-02-01 11:12:13.000000; aBcDeF123_5\n"
							+ "999;;;;999;;;\"<test_text>_999\"\n",
							exportTestTable());

			final String logData = getLogFileData();
			assertLogContains(logData, "Found items", "8");
			assertLogContains(logData, "Valid items", "8");
			assertLogContains(logData, "Invalid items", "0");
			assertLogContains(logData, "Imported data amount", "2,1025 KiByte");
			assertLogContains(logData, "Inserted items", "3");
			assertLogContains(logData, "Updated items", "5");
		} catch (final Exception e) {
			Assert.fail(e.getMessage());
		} finally {
			Utilities.closeQuietly(jsonWriter);
		}
	}

	@Test
	public void testCsvImportInlineData() {
		try {
			createEmptyTestTable();
			prefillTestTable();

			Assert.assertEquals(0, DbImport._main(new String[] {
					"cassandra",
					HOSTNAME,
					DBNAME,
					USERNAME,
					"-x", "SQL",
					"-data",
					"-import", "INSERT INTO test_tbl (id, column_varchar) VALUES (1, 'Inline Insert');INSERT INTO test_tbl (id, column_varchar) VALUES (2, 'Inline Insert')",
					PASSWORD }));
			Assert.assertEquals(
					"id;column_blob;column_date;column_double;column_int;column_text;column_timestamp;column_varchar\n"
							+ "1;;;;1;;;Inline Insert\n"
							+ "2;;;;;;;Inline Insert\n"
							+ "3;;;;3;;;\"<test_text>_3\"\n"
							+ "999;;;;999;;;\"<test_text>_999\"\n",
							exportTestTable());
		} catch (final Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testCsvImportDuplicates() {
		try {
			createEmptyTestTable();

			FileUtilities.write(INPUTFILE_CSV, (
					"id;column int; column_double; column_varchar; column_text; column_timestamp; column_date\n"
							+ "123;123; 123.456; aBcDeF123; aBcDeF1234; 01.02.2003 11:12:13; 01.02.2003 21:22:23\n"
							+ "123;123; 123.456; aBcDeF123; aBcDeF1234; 01.02.2003 11:12:13; 01.02.2003 21:22:23\n"
							+ "123;123; 123.456; aBcDeF123; aBcDeF1234; 01.02.2003 11:12:13; 01.02.2003 21:22:23\n"
					).getBytes(StandardCharsets.UTF_8));
			final String mapping = "id='id';column_int='column int'; column_double='column_double'; column_varchar='column_varchar'; column_text='column_text'; column_blob=; column_timestamp='column_timestamp'dd.MM.yyyy HH:mm:ss; column_date='column_date'dd.MM.yyyy HH:mm:ss";
			Assert.assertEquals(0, DbImport._main(new String[] {
					"cassandra",
					HOSTNAME,
					DBNAME,
					USERNAME,
					"-table", "test_tbl",
					"-i", "UPSERT",
					"-l",
					"-import", "~" + File.separator + "temp" + File.separator + "test_tbl.csv",
					"-k", "id",
					"-m", mapping,
					PASSWORD }));
			Assert.assertEquals(
					"id;column_blob;column_date;column_double;column_int;column_text;column_timestamp;column_varchar\n"
							+ "123;;2003-02-01;123.456;123; aBcDeF1234;2003-02-01 11:12:13.000000; aBcDeF123\n",
							exportTestTable());

			final String logData = getLogFileData();
			assertLogContains(logData, "Found items", "3");
			assertLogContains(logData, "Valid items", "3");
			assertLogContains(logData, "Invalid items", "0");
			assertLogContains(logData, "Imported data amount", "335 Byte");
			assertLogContains(logData, "Inserted items", "1");
			assertLogContains(logData, "Updated items", "2");
		} catch (final Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testCsvImportExistingDuplicates() {
		try {
			createEmptyTestTable();
			prefillTestTable();
			prefillTestTable();

			FileUtilities.write(INPUTFILE_CSV, (
					"id;column int; column_double; column_varchar; column_text; column_timestamp; column_date\n"
							+ "1;1; 123.456; aBcDeF123; aBcDeF1234; 01.02.2003 11:12:13; 01.02.2003 21:22:23\n"
							+ "1;1; 123.456; aBcDeF123; aBcDeF1234; 01.02.2003 11:12:13; 01.02.2003 21:22:23\n"
							+ "3;3; 123.456; aBcDeF123; aBcDeF1234; 01.02.2003 11:12:13; 01.02.2003 21:22:23\n"
					).getBytes(StandardCharsets.UTF_8));
			final String mapping = "id='id';column_int='column int'; column_double='column_double'; column_varchar='column_varchar'; column_text='column_text'; column_blob=; column_timestamp='column_timestamp'dd.MM.yyyy HH:mm:ss; column_date='column_date'dd.MM.yyyy HH:mm:ss";
			Assert.assertEquals(0, DbImport._main(new String[] {
					"cassandra",
					HOSTNAME,
					DBNAME,
					USERNAME,
					"-table", "test_tbl",
					"-l",
					"-i", "UPSERT",
					"-import", "~" + File.separator + "temp" + File.separator + "test_tbl.csv",
					"-k", "id",
					"-m", mapping,
					PASSWORD }));
			Assert.assertEquals(
					"id;column_blob;column_date;column_double;column_int;column_text;column_timestamp;column_varchar\n"
							+ "1;;2003-02-01;123.456;1; aBcDeF1234;2003-02-01 11:12:13.000000; aBcDeF123\n"
							+ "3;;2003-02-01;123.456;3; aBcDeF1234;2003-02-01 11:12:13.000000; aBcDeF123\n"
							+ "999;;;;999;;;\"<test_text>_999\"\n",
							exportTestTable());

			final String logData = getLogFileData();
			assertLogContains(logData, "Found items", "3");
			assertLogContains(logData, "Valid items", "3");
			assertLogContains(logData, "Invalid items", "0");
			assertLogContains(logData, "Imported data amount", "323 Byte");
			assertLogContains(logData, "Inserted items", "0");
			assertLogContains(logData, "Updated items", "3");
		} catch (final Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testCsvImportUpsertWithNullMakeUnique() {
		try {
			createEmptyTestTable();
			prefillTestTable();
			prefillTestTable();

			final StringBuilder data = new StringBuilder();
			data.append("id;column int; column_double; column_varchar; column_text; column_timestamp; column_date\n");
			data.append("1;1; 123.456; aBcDeF123_1; aBcDeF1234; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("1;1; 123.456;; aBcDeF1235_1; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("2;2; 123.456; aBcDeF123_2; aBcDeF1234; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("2;2; 123.456;; aBcDeF1235_2; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("3;3; 123.456; aBcDeF123_3; aBcDeF1234; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("3;3; 123.456;; aBcDeF1235_3; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("4;4; 123.456;; aBcDeF1235_4; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("5;5; 123.456; aBcDeF123_5; aBcDeF1234; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			FileUtilities.write(INPUTFILE_CSV, data.toString().getBytes(StandardCharsets.UTF_8));

			final String mapping = "id='id';column_int='column int'; column_double='column_double'; column_varchar='column_varchar'; column_text='column_text'; column_blob=; column_timestamp='column_timestamp'dd.MM.yyyy HH:mm:ss; column_date='column_date'dd.MM.yyyy HH:mm:ss";
			Assert.assertEquals(1, DbImport._main(new String[] {
					"cassandra",
					HOSTNAME,
					DBNAME,
					USERNAME,
					"-table", "test_tbl",
					"-l",
					"-import", "~" + File.separator + "temp" + File.separator + "test_tbl.csv",
					"-m", mapping,
					"-i", "UPSERT",
					"-d", DuplicateMode.MAKE_UNIQUE_JOIN.toString(),
					"-k", "id",
					PASSWORD }));
			// Only duplicate mode 'UPDATE_ALL_JOIN' is allowed for this NoSql import
		} catch (final Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testCsvImportUpsertWithNullWithFirstOnly() {
		try {
			createEmptyTestTable();
			prefillTestTable();
			prefillTestTable();

			final StringBuilder data = new StringBuilder();
			data.append("id;column int; column_double; column_varchar; column_text; column_timestamp; column_date\n");
			data.append("1;1; 123.456; aBcDeF123_1; aBcDeF1234; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("1;1; 123.456;; aBcDeF1235_1; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("2;2; 123.456; aBcDeF123_2; aBcDeF1234; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("2;2; 123.456;; aBcDeF1235_2; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("3;3; 123.456; aBcDeF123_3; aBcDeF1234; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("3;3; 123.456;; aBcDeF1235_3; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("4;4; 123.456;; aBcDeF1235_4; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("5;5; 123.456; aBcDeF123_5; aBcDeF1234; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			FileUtilities.write(INPUTFILE_CSV, data.toString().getBytes(StandardCharsets.UTF_8));

			final String mapping = "id='id';column_int='column int'; column_double='column_double'; column_varchar='column_varchar'; column_text='column_text'; column_blob=; column_timestamp='column_timestamp'dd.MM.yyyy HH:mm:ss; column_date='column_date'dd.MM.yyyy HH:mm:ss";
			Assert.assertEquals(1, DbImport._main(new String[] {
					"cassandra",
					HOSTNAME,
					DBNAME,
					USERNAME,
					"-table", "test_tbl",
					"-l",
					"-import", "~" + File.separator + "temp" + File.separator + "test_tbl.csv",
					"-m", mapping,
					"-i", "UPSERT",
					"-k", "id",
					"-d", DuplicateMode.UPDATE_FIRST_JOIN.toString(),
					PASSWORD }));
			// Only duplicate mode 'UPDATE_ALL_JOIN' is allowed for this NoSql import
		} catch (final Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testCsvImportUpsertWithoutNullWithFirstOnly() {
		try {
			createEmptyTestTable();
			prefillTestTable();
			prefillTestTable();

			final StringBuilder data = new StringBuilder();
			data.append("id;column int; column_double; column_varchar; column_text; column_timestamp; column_date\n");
			data.append("1;1; 123.456; aBcDeF123_1; aBcDeF1234; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("2;2; 123.456;; aBcDeF1235_1; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("2;2; 123.456; aBcDeF123_2; aBcDeF1234; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("2;2; 123.456;; aBcDeF1235_2; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("3;3; 123.456; aBcDeF123_3; aBcDeF1234; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("3;3; 123.456;; aBcDeF1235_3; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("4;4; 123.456;; aBcDeF1235_4; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			data.append("5;5; 123.456; aBcDeF123_5; aBcDeF1234; 01.02.2003 11:12:13; 01.03.2003 21:22:23\n");
			FileUtilities.write(INPUTFILE_CSV, data.toString().getBytes(StandardCharsets.UTF_8));

			final String mapping = "id='id';column_int='column int'; column_double='column_double'; column_varchar='column_varchar'; column_text='column_text'; column_blob=; column_timestamp='column_timestamp'dd.MM.yyyy HH:mm:ss; column_date='column_date'dd.MM.yyyy HH:mm:ss";
			Assert.assertEquals(1, DbImport._main(new String[] {
					"cassandra",
					HOSTNAME,
					DBNAME,
					USERNAME,
					"-table", "test_tbl",
					"-l",
					"-import", "~" + File.separator + "temp" + File.separator + "test_tbl.csv",
					"-u",
					"-m", mapping,
					"-i", "UPSERT",
					"-k", "id",
					"-d", DuplicateMode.UPDATE_FIRST_JOIN.toString(),
					PASSWORD }));
			// // Only duplicate mode 'UPDATE_ALL_JOIN' is allowed for this NoSql import
		} catch (final Exception e) {
			Assert.fail(e.getMessage());
		}
	}
}
