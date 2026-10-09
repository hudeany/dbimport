package de.soderer.dbimport;

/**
 * Exception for errors of the DbImport application, e.g. invalid parameters or import data.
 */
public class DbImportException extends Exception {

	/** The Constant serialVersionUID. */
	private static final long serialVersionUID = 6039775378389122712L;

	/**
	 * Creates an exception with an error message.
	 *
	 * @param errorMessage the error message
	 */
	public DbImportException(final String errorMessage) {
		super(errorMessage);
	}

	/**
	 * Creates an exception with an error message and its cause.
	 *
	 * @param errorMessage the error message
	 * @param e the cause
	 */
	public DbImportException(final String errorMessage, final Exception e) {
		super(errorMessage, e);
	}
}
