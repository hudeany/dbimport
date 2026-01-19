package de.soderer.dbimport;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.Statement;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import de.soderer.json.JsonArray;
import de.soderer.json.JsonObject;
import de.soderer.json.JsonWriter;
import de.soderer.utilities.TextUtilities;
import de.soderer.utilities.Utilities;
import de.soderer.utilities.db.DbDefinition;
import de.soderer.utilities.db.DbUtilities;
import de.soderer.utilities.db.DbUtilities.DbVendor;

public class DbImportTest_Derby_BigInt {
	public static final String DERBY_DB_PATH = System.getProperty("user.home") + File.separator + "temp" + File.separator + "test.derby";

	public static final String[] DATA_TYPES = new String[] { "BIGINT", "DOUBLE", "VARCHAR(1024)", "BLOB", "CLOB", "TIMESTAMP", "DATE" };

	public static File INPUTFILE_CSV = new File(Utilities.replaceUsersHome("~" + File.separator + "temp" + File.separator + "test_tbl.csv"));
	public static File INPUTFILE_JSON = new File(Utilities.replaceUsersHome("~" + File.separator + "temp" + File.separator + "test_tbl.json"));
	public static File INPUTFILE_XML = new File(Utilities.replaceUsersHome("~" + File.separator + "temp" + File.separator + "test_tbl.xml"));
	public static File BLOB_DATA_FILE = new File(Utilities.replaceUsersHome("~" + File.separator + "temp" + File.separator + "test.blob"));

	@BeforeClass
	public static void setupTestClass() throws Exception {
		if (new File(DERBY_DB_PATH).exists()) {
			Utilities.delete(new File(DERBY_DB_PATH));
		}

		try (Connection connection = DbUtilities.createNewDatabase(DbVendor.Derby, DERBY_DB_PATH)) {
			// Just close the connection
		}
	}

	@Before
	public void setup() throws Exception {
		INPUTFILE_CSV.delete();
		INPUTFILE_JSON.delete();
		INPUTFILE_XML.delete();
		BLOB_DATA_FILE.delete();

		try (Connection connection = DbUtilities.createConnection(new DbDefinition(DbVendor.Derby, "", DERBY_DB_PATH, "", null), false);
				Statement statement = connection.createStatement()) {
			if (DbUtilities.checkTableExist(connection, "test_tbl")) {
				statement.execute("DROP TABLE test_tbl");
			}
		} catch (final Exception e) {
			e.printStackTrace();
			throw e;
		}
	}

	@After
	public void tearDown() throws Exception {
		INPUTFILE_CSV.delete();
		INPUTFILE_JSON.delete();
		INPUTFILE_XML.delete();
		BLOB_DATA_FILE.delete();

		try (Connection connection = DbUtilities.createConnection(new DbDefinition(DbVendor.Derby, "", DERBY_DB_PATH, "", null), false)) {
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

	@AfterClass
	public static void tearDownTestClass() throws Exception {
		try {
			if (!DbUtilities.deleteDatabase(DbVendor.Derby, DERBY_DB_PATH)) {
				System.out.println("Cannot clean up derby database after tests");
			}
		} catch (final Exception e) {
			System.out.println("Cannot clean up derby database after tests: " + e.getMessage());
			throw e;
		}
	}

	private void createEmptyTestTable() throws Exception {
		try (Connection connection = DbUtilities.createConnection(new DbDefinition(DbVendor.Derby, "", DERBY_DB_PATH, "", null), false);
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
			statement.execute("CREATE TABLE test_tbl (id INTEGER NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1), " + dataColumnsPart + ", PRIMARY KEY (id))");
		} catch (final Exception e) {
			e.printStackTrace();
			throw e;
		}
	}

	private void prefillTestTable() throws Exception {
		try (Connection connection = DbUtilities.createConnection(new DbDefinition(DbVendor.Derby, "", DERBY_DB_PATH, "", null), false);
				Statement statement = connection.createStatement()) {
			statement.executeUpdate("INSERT INTO test_tbl (column_bigint, column_varchar) VALUES (1, '<test_text>_1')".replace("<test_text>", TextUtilities.GERMAN_TEST_STRING.replace("'", "''")));
			statement.executeUpdate("INSERT INTO test_tbl (column_bigint, column_varchar) VALUES (3, '<test_text>_3')".replace("<test_text>", TextUtilities.GERMAN_TEST_STRING.replace("'", "''")));
			statement.executeUpdate("INSERT INTO test_tbl (column_bigint, column_varchar) VALUES (999, '<test_text>_999')".replace("<test_text>", TextUtilities.GERMAN_TEST_STRING.replace("'", "''")));
		} catch (final Exception e) {
			e.printStackTrace();
			throw e;
		}
	}

	private String exportTestTable(final String columns) throws Exception {
		try (Connection connection = DbUtilities.createConnection(new DbDefinition(DbVendor.Derby, "", DERBY_DB_PATH, "", null), false)) {
			return DbUtilities.readout(connection, "SELECT " + columns + " FROM test_tbl", ';', '\"').replace(TextUtilities.GERMAN_TEST_STRING.replace("\"", "\"\""), "<test_text>");
		} catch (final Exception e) {
			e.printStackTrace();
			throw e;
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
			jsonObject.add("column bigint", 1);
			jsonObject.add("column_double", 123.456);
			jsonObject.add("column_varchar", " aBcDeF123_1");
			jsonObject.add("column_clob", " aBcDeF1234");
			jsonObject.add("column_timestamp", "01.02.2003 11:12:13");
			jsonObject.add("column_date", " 01.03.2003 21:22:23");
			jsonArray.add(jsonObject);

			jsonObject = new JsonObject();
			jsonObject.add("column bigint", 1);
			jsonObject.add("column_double", 123.456);
			jsonObject.addNull("column_varchar");
			jsonObject.add("column_clob", " aBcDeF1235_1");
			jsonObject.add("column_timestamp", "01.02.2003 11:12:13");
			jsonObject.add("column_date", " 01.03.2003 21:22:23");
			jsonArray.add(jsonObject);

			jsonObject = new JsonObject();
			jsonObject.add("column bigint", 2);
			jsonObject.add("column_double", 123.456);
			jsonObject.add("column_varchar", " aBcDeF123_2");
			jsonObject.add("column_clob", " aBcDeF1234");
			jsonObject.add("column_timestamp", "01.02.2003 11:12:13");
			jsonObject.add("column_date", " 01.03.2003 21:22:23");
			jsonArray.add(jsonObject);

			jsonObject = new JsonObject();
			jsonObject.add("column bigint", 2);
			jsonObject.add("column_double", 123.456);
			jsonObject.addNull("column_varchar");
			jsonObject.add("column_clob", " aBcDeF1235_2");
			jsonObject.add("column_timestamp", "01.02.2003 11:12:13");
			jsonObject.add("column_date", " 01.03.2003 21:22:23");
			jsonArray.add(jsonObject);

			jsonObject = new JsonObject();
			jsonObject.add("column bigint", 3);
			jsonObject.add("column_double", 123.456);
			jsonObject.add("column_varchar", " aBcDeF123_3");
			jsonObject.add("column_clob", " aBcDeF1234");
			jsonObject.add("column_timestamp", "01.02.2003 11:12:13");
			jsonObject.add("column_date", " 01.03.2003 21:22:23");
			jsonArray.add(jsonObject);

			jsonObject = new JsonObject();
			jsonObject.add("column bigint", 3);
			jsonObject.add("column_double", 123.456);
			jsonObject.addNull("column_varchar");
			jsonObject.add("column_clob", " aBcDeF1235_3");
			jsonObject.add("column_timestamp", "01.02.2003 11:12:13");
			jsonObject.add("column_date", " 01.03.2003 21:22:23");
			jsonArray.add(jsonObject);

			jsonObject = new JsonObject();
			jsonObject.add("column bigint", 4);
			jsonObject.add("column_double", 123.456);
			jsonObject.addNull("column_varchar");
			jsonObject.add("column_clob", " aBcDeF1235_4");
			jsonObject.add("column_timestamp", "01.02.2003 11:12:13");
			jsonObject.add("column_date", " 01.03.2003 21:22:23");
			jsonArray.add(jsonObject);

			jsonObject = new JsonObject();
			jsonObject.add("column bigint", 5);
			jsonObject.add("column_double", 123.456);
			jsonObject.add("column_varchar", " aBcDeF123_5");
			jsonObject.add("column_clob", " aBcDeF1234");
			jsonObject.add("column_timestamp", "01.02.2003 11:12:13");
			jsonObject.add("column_date", " 01.03.2003 21:22:23");
			jsonArray.add(jsonObject);

			jsonWriter = new JsonWriter(new FileOutputStream(INPUTFILE_JSON), StandardCharsets.UTF_8);
			jsonWriter.add(jsonArray);
			Utilities.closeQuietly(jsonWriter);
			jsonWriter = null;

			final String mapping = "column_bigint='column bigint'; column_double='column_double'; column_varchar='column_varchar'; column_clob='column_clob'; column_blob=; column_timestamp='column_timestamp'dd.MM.yyyy HH:mm:ss; column_date='column_date'dd.MM.yyyy HH:mm:ss";
			Assert.assertEquals(0, DbImport._main(new String[] { "derby", DERBY_DB_PATH, "-table", "test_tbl", "-x", "JSON", "-import", "~" + File.separator + "temp" + File.separator + "test_tbl.json", "-m", mapping, "-i", "UPSERT", "-k", "column_bigint" }));
			Assert.assertEquals(
					"ID;COLUMN_BLOB;COLUMN_CLOB;COLUMN_DATE;COLUMN_DOUBLE;COLUMN_BIGINT;COLUMN_TIMESTAMP;COLUMN_VARCHAR\n"
							+ "1;; aBcDeF1235_1;2003-03-01;123.456;1;2003-02-01 11:12:13.0;\n"
							+ "2;; aBcDeF1235_3;2003-03-01;123.456;3;2003-02-01 11:12:13.0;\n"
							+ "3;;;;;999;;\"<test_text>_999\"\n"
							+ "4;; aBcDeF1235_2;2003-03-01;123.456;2;2003-02-01 11:12:13.0;\n"
							+ "5;; aBcDeF1235_4;2003-03-01;123.456;4;2003-02-01 11:12:13.0;\n"
							+ "6;; aBcDeF1234;2003-03-01;123.456;5;2003-02-01 11:12:13.0; aBcDeF123_5\n",
							exportTestTable("id,column_blob,column_clob,column_date,column_double,column_bigint,column_timestamp,column_varchar"));
		} catch (final Exception e) {
			Assert.fail(e.getMessage());
		} finally {
			Utilities.closeQuietly(jsonWriter);
		}
	}
}
