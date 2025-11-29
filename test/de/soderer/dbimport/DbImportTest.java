package de.soderer.dbimport;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class DbImportTest {
	@Before
	public void setup() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testFilename() {
		Assert.assertEquals("testtbl", DbImport.getImportTablenameFromFilename("testtbl"));
		Assert.assertEquals("testtbl", DbImport.getImportTablenameFromFilename("testtbl.csv"));
		Assert.assertEquals("test_tbl", DbImport.getImportTablenameFromFilename("test_tbl.csv"));
		Assert.assertEquals("testschema.test_tbl", DbImport.getImportTablenameFromFilename("testschema.test_tbl.csv"));
		Assert.assertEquals("test_tbl", DbImport.getImportTablenameFromFilename("some_additional text(test_tbl).csv"));
	}
}
