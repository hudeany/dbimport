package de.soderer.dbimport;

import java.io.File;

import de.soderer.utilities.Utilities;
import de.soderer.utilities.db.DbDefinition;
import de.soderer.utilities.db.DbUtilities.DbVendor;

public class BlobImportDefinition extends DbDefinition {
	// Mandatory parameters

	/** The blobImportStatement. */
	private String blobImportStatement;

	/** The importFilePath. */
	private String importFilePath;

	// Default optional parameters

	public String getBlobImportStatement() {
		return blobImportStatement;
	}

	public void setBlobImportStatement(final String blobImportStatement) {
		this.blobImportStatement = blobImportStatement;
	}

	public String getImportFilePath() {
		return importFilePath;
	}

	public void setImportFilePath(final String importFilePath) {
		this.importFilePath = Utilities.replaceUsersHome(importFilePath);
	}

	public void checkParameters() throws Exception {
		super.checkParameters(DbImport.APPLICATION_NAME, DbImport.CONFIGURATION_FILE);

		if (Utilities.isBlank(blobImportStatement)) {
			throw new DbImportException("BlobImportStatement is missing");
		} else if (!blobImportStatement.contains("?")) {
			throw new DbImportException("BlobImportStatement does not contain mandatory '?' placeholder");
		}

		if (importFilePath == null) {
			throw new DbImportException("ImportFilePath is missing");
		} else if (!new File(importFilePath).exists()) {
			throw new DbImportException("ImportFilePath does not exist: " + importFilePath);
		} else if (!new File(importFilePath).isFile()) {
			throw new DbImportException("ImportFilePath is not a file: " + importFilePath);
		}
	}

	public String toParamsString() {
		String params = "importblob";
		params += " " + getDbVendor().name();
		if (getDbVendor() != DbVendor.SQLite && getDbVendor() != DbVendor.HSQL && getDbVendor() != DbVendor.Derby) {
			params += " " + getHostnameAndPort();
		}
		params += " " + getDbName();
		if (getDbVendor() != DbVendor.SQLite && getDbVendor() != DbVendor.Derby) {
			if (getUsername() != null) {
				params += " " + getUsername();
			}
		}
		params += " '" + getBlobImportStatement().replace("'", "\\'") + "'";
		params += " '" + getImportFilePath().replace("'", "\\'") + "'";
		if (getPassword() != null) {
			params += " '" + new String(getPassword()).replace("'", "\\'") + "'";
		}
		return params;
	}
}
