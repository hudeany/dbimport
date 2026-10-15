package de.soderer.dbimport;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DbImportTest {
	@BeforeEach
	public void setup() throws Exception {
	}

	@AfterEach
	public void tearDown() throws Exception {
	}

	@Test
	public void testFilename() {
		assertEquals("testtbl", DbImport.getImportTablenameFromFilename("testtbl"));
		assertEquals("testtbl", DbImport.getImportTablenameFromFilename("testtbl.csv"));
		assertEquals("test_tbl", DbImport.getImportTablenameFromFilename("test_tbl.csv"));
		assertEquals("testschema.test_tbl", DbImport.getImportTablenameFromFilename("testschema.test_tbl.csv"));
		assertEquals("test_tbl", DbImport.getImportTablenameFromFilename("some_additional text(test_tbl).csv"));
	}
}
