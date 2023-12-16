package de.soderer.dbimport;

/**
 * The Class DbImportException.
 */
public class DbImportException extends Exception {

	/** The Constant serialVersionUID. */
	private static final long serialVersionUID = 6039775378389122712L;

	/**
	 * Instantiates a new database csv import exception.
	 *
	 * @param errorMessage
	 *            the error message
	 */
	public DbImportException(final String errorMessage) {
		super(errorMessage);
	}

	public DbImportException(final String errorMessage, final Exception e) {
		super(errorMessage, e);
	}
}
