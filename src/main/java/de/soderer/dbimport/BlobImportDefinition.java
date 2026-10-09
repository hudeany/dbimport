package de.soderer.dbimport;

import java.io.File;

import de.soderer.utilities.Utilities;
import de.soderer.utilities.db.data.DbConnectionDefinition;
import de.soderer.utilities.db.data.DbVendor;
import de.soderer.utilities.db.exception.DbDefinitionException;

/**
 * Parameters of a blob import ("importblob"): the data of a single file is written to a database blob or clob
 * column via an update statement with one '?' placeholder.
 */
public class BlobImportDefinition extends DbConnectionDefinition {
	// Mandatory parameters

	/** The blobImportStatement. */
	private String blobImportStatement;

	/** The importFilePath. */
	private String importFilePath;

	// Default optional parameters

	/**
	 * Creates an empty definition, the parameters are set via the setter methods.
	 */
	public BlobImportDefinition() {
		// Nothing to initialize
	}

	/**
	 * Returns the SQL update statement, which contains a '?' placeholder for the file data.
	 *
	 * @return the SQL update statement
	 */
	public String getBlobImportStatement() {
		return blobImportStatement;
	}

	/**
	 * Sets the SQL update statement, which must contain a '?' placeholder for the file data.
	 *
	 * @param blobImportStatement the SQL update statement
	 */
	public void setBlobImportStatement(final String blobImportStatement) {
		this.blobImportStatement = blobImportStatement;
	}

	/**
	 * Returns the path of the file to import.
	 *
	 * @return the path of the file to import
	 */
	public String getImportFilePath() {
		return importFilePath;
	}

	/**
	 * Sets the path of the file to import. A leading "~" is replaced by the user's home directory.
	 *
	 * @param importFilePath the path of the file to import
	 */
	public void setImportFilePath(final String importFilePath) {
		this.importFilePath = Utilities.replaceUsersHome(importFilePath);
	}

	/**
	 * Checks the parameters and supplies the database driver (it may be asked for interactively, if it is missing).
	 *
	 * @throws Exception if the database driver is not available, or a parameter is missing or invalid
	 */
	@Override
	public void checkParameters() throws Exception {
		if (getDbVendor() != null) {
			try {
				if (!new DbDriverSupplier(null, getDbVendor()).supplyDriver(DbImport.APPLICATION_NAME, DbImport.CONFIGURATION_FILE)) {
					throw new DbDefinitionException("Cannot aquire database driver for database vendor: " + getDbVendor());
				}
			} catch (final Exception e) {
				throw new DbDefinitionException("Cannot aquire database driver for database vendor: " + getDbVendor(), e);
			}
		}

		super.checkParameters();

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

	/**
	 * Returns the command line parameters for this blob import, e.g. to show them in the console menu.
	 *
	 * @return the command line parameters, starting with "importblob"
	 */
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
