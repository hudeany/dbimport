package de.soderer.dbimport;

import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

@Suite
@SelectClasses({
	DbImportTest_Derby.class,
	DbImportTest_HSQL.class,
	DbImportTest_SQLite.class
})

public class NoInstallNeededImportTestSuite {

}
