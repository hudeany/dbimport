package de.soderer.dbimport;

import java.awt.GraphicsEnvironment;
import java.io.File;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.NumberFormat;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import javax.swing.SwingUtilities;

import de.soderer.dbimport.DbImportDefinition.DataType;
import de.soderer.dbimport.DbImportDefinition.DuplicateMode;
import de.soderer.dbimport.DbImportDefinition.ImportMode;
import de.soderer.dbimport.console.BlobUpdateMenu;
import de.soderer.dbimport.console.ConnectionTestMenu;
import de.soderer.dbimport.console.CreateTrustStoreMenu;
import de.soderer.dbimport.console.HelpMenu;
import de.soderer.dbimport.console.ImportMenu;
import de.soderer.dbimport.console.PreferencesMenu;
import de.soderer.dbimport.console.UpdateMenu;
import de.soderer.network.trustmanager.TrustManagerUtilities;
import de.soderer.utilities.ConfigurationProperties;
import de.soderer.utilities.DateUtilities;
import de.soderer.utilities.FileUtilities;
import de.soderer.utilities.IoUtilities;
import de.soderer.utilities.LangResources;
import de.soderer.utilities.NumberUtilities;
import de.soderer.utilities.ParameterException;
import de.soderer.utilities.UpdateableConsoleApplication;
import de.soderer.utilities.Utilities;
import de.soderer.utilities.Version;
import de.soderer.utilities.appupdate.ApplicationUpdateUtilities;
import de.soderer.utilities.console.ConsoleMenu;
import de.soderer.utilities.console.ConsoleType;
import de.soderer.utilities.console.ConsoleUtilities;
import de.soderer.utilities.console.ConsoleUtilities.TextColor;
import de.soderer.utilities.console.PasswordConsoleInput;
import de.soderer.utilities.db.DbUtilities;
import de.soderer.utilities.db.data.DbVendor;
import de.soderer.utilities.db.exception.DbNotExistsException;
import de.soderer.utilities.worker.WorkerParentDual;

/**
 * The Main-Class of DbImport.
 */
// TODO: Missing mapping for not null column => error message
// TODO: Check for string too large
// TODO: Errorhandling on createConnection (dbconnection error detection and help messages in errors)
// TODO: Multiple file blob import export
// TODO: Get a list of tables
// TODO: Single Blob Upload GUI
public class DbImport extends UpdateableConsoleApplication implements WorkerParentDual {
	/** The Constant APPLICATION_NAME. */
	public static final String APPLICATION_NAME = "DbImport";
	/**
	 * Name of the startup class used for the application update.
	 */
	public static final String APPLICATION_STARTUPCLASS_NAME = "de-soderer-DbImport";

	/** The Constant VERSION_RESOURCE_FILE, which contains version number and versioninfo download url. */
	public static final String VERSION_RESOURCE_FILE = "/version.txt";

	/**
	 * Resource file containing the help text.
	 */
	public static final String HELP_RESOURCE_FILE = "/help.txt";

	/** The Constant CONFIGURATION_FILE. */
	public static final File CONFIGURATION_FILE = new File(System.getProperty("user.home") + File.separator + "." + APPLICATION_NAME + File.separator + "." + APPLICATION_NAME + ".config");
	/**
	 * Property name of the driver file path in the configuration file.
	 */
	public static final String CONFIGURATION_DRIVERLOCATIONPROPERTYNAME = "driver_location";

	/** The Constant SECURE_PREFERENCES_FILE. */
	public static final File SECURE_PREFERENCES_FILE = new File(System.getProperty("user.home") + File.separator + "." + APPLICATION_NAME + File.separator + "." + APPLICATION_NAME + ".secpref");

	/** The version is filled in at application start from the version.txt file. */
	public static Version VERSION = null;

	/** The version build time is filled in at application start from the version.txt file */
	public static LocalDateTime VERSION_BUILDTIME = null;

	/** The versioninfo download url is filled in at application start from the version.txt file. */
	public static String VERSIONINFO_DOWNLOAD_URL = null;

	/** Trusted CA certificate for updates **/
	public static String TRUSTED_UPDATE_CA_CERTIFICATES = null;

	/** The usage message. */
	private static String getUsageMessage() {
		try (InputStream helpInputStream = DbImport.class.getResourceAsStream(HELP_RESOURCE_FILE)) {
			return "DbImport (by Andreas Soderer, mail: dbimport@soderer.de)\n"
					+ "VERSION: " + VERSION.toString() + getBuildTimeText() + "\n\n"
					+ new String(IoUtilities.toByteArray(helpInputStream), StandardCharsets.UTF_8);
		} catch (@SuppressWarnings("unused") final Exception e) {
			return "Help info is missing";
		}
	}

	/**
	 * Returns the build time for the version output, e.g. " (2026-10-07 10:50:48)".
	 *
	 * @return the formatted build time in brackets, or an empty string, if the version file contains no build time
	 */
	public static String getBuildTimeText() {
		// The build time is optional in version.txt, so it must not break the help output
		return VERSION_BUILDTIME == null ? "" : " (" + DateUtilities.formatDate(DateUtilities.YYYY_MM_DD_HHMMSS, VERSION_BUILDTIME) + ")";
	}

	/** Lower case keywords for the help text, only recognized as single argument */
	private static final Set<String> HELP_KEYWORDS = Set.of("help", "-help", "--help", "-h", "--h", "-?", "--?");

	/** Lower case flags only known by the blob import parameters (see menu mode parsing in _main) */
	private static final Set<String> BLOB_IMPORT_FLAGS = Set.of("-updatesql", "-blobfile");

	/** Lower case flags only known by the connection test parameters (see menu mode parsing in _main) */
	private static final Set<String> CONNECTION_TEST_FLAGS = Set.of("-iter", "-sleep", "-check");

	/** The database import definition. */
	private DbImportDefinition dbImportDefinitionToExecute;

	private int previousTerminalWidth = 0;

	/**
	 * The main method.
	 *
	 * @param arguments the arguments
	 */
	public static void main(final String[] arguments) {
		final int returnCode = _main(arguments);
		if (returnCode >= 0) {
			System.exit(returnCode);
		}
	}

	/**
	 * Method used for main but with no System.exit call to make it junit testable.
	 *
	 * @param args the command line arguments
	 * @return the exit code (0 for success, 1 for errors), or a negative value if the application keeps running (GUI)
	 */
	protected static int _main(final String[] args) {
		ApplicationUpdateUtilities.removeUpdateLeftovers();

		try (InputStream resourceStream = DbImport.class.getResourceAsStream(VERSION_RESOURCE_FILE)) {
			// Try to fill the version and versioninfo download url
			final List<String> versionInfoLines = Utilities.readLines(resourceStream, StandardCharsets.UTF_8);
			VERSION = new Version(versionInfoLines.get(0));
			if (versionInfoLines.size() >= 2) {
				VERSION_BUILDTIME = DateUtilities.parseLocalDateTime(DateUtilities.YYYY_MM_DD_HHMMSS, versionInfoLines.get(1));
			}
			if (versionInfoLines.size() >= 3) {
				VERSIONINFO_DOWNLOAD_URL = versionInfoLines.get(2);
			}
			if (versionInfoLines.size() >= 4) {
				TRUSTED_UPDATE_CA_CERTIFICATES = versionInfoLines.get(3);
			}
		} catch (@SuppressWarnings("unused") final Exception e) {
			// Without the version.txt file we may not go on
			System.err.println("Invalid version.txt");
			return 1;
		}

		ConfigurationProperties applicationConfiguration;
		try {
			applicationConfiguration = new ConfigurationProperties(DbImport.APPLICATION_NAME, true);
			DbImportGui.setupDefaultConfig(applicationConfiguration);
		} catch (@SuppressWarnings("unused") final Exception e) {
			System.err.println("Invalid application configuration");
			return 1;
		}

		try {
			String[] arguments = args;

			boolean openGui = false;
			boolean openMenu = false;
			boolean connectionTest = false;
			boolean blobImport = false;
			boolean createTrustStore = false;

			if (arguments.length == 0) {
				// If started without any parameter we check for headless mode and show the console menu or the GUI
				if (GraphicsEnvironment.isHeadless()) {
					openMenu = true;
				} else {
					openGui = true;
				}
			} else if (arguments.length == 1 && HELP_KEYWORDS.contains(arguments[0].toLowerCase(Locale.ROOT))) {
				// Only as single argument, otherwise e.g. a table name, username or password "help" showed the help text
				System.out.println(getUsageMessage());
				return 1;
			} else if (arguments.length == 1 && "version".equalsIgnoreCase(arguments[0])) {
				System.out.println(VERSION);
				return 1;
			} else {
				// The mode keywords are only recognized as first argument. Before, they were recognized at any position,
				// so e.g. a password, username or table name "menu" or "gui" changed the mode and was removed from the arguments.
				final String modeKeyword = arguments[0].toLowerCase(Locale.ROOT);
				if ("update".equals(modeKeyword) && arguments.length <= 3) {
					final DbImport dbImport = new DbImport();
					final String username = arguments.length > 1 ? arguments[1] : null;
					final char[] password = arguments.length > 2 ? arguments[2].toCharArray() : null;
					ApplicationUpdateUtilities.executeUpdate(dbImport, DbImport.VERSIONINFO_DOWNLOAD_URL, applicationConfiguration.getProxyConfiguration(), DbImport.APPLICATION_NAME, DbImport.VERSION, DbImport.TRUSTED_UPDATE_CA_CERTIFICATES, username, password, null, false, false);
					return 1;
				} else if ("gui".equals(modeKeyword)) {
					if (GraphicsEnvironment.isHeadless()) {
						throw new DbImportException("GUI can only be shown on a non-headless environment");
					}
					openGui = true;
					arguments = Utilities.removeItemAtIndex(arguments, 0);
				} else if ("menu".equals(modeKeyword)) {
					openMenu = true;
					arguments = Utilities.removeItemAtIndex(arguments, 0);
				} else if ("connectiontest".equals(modeKeyword)) {
					connectionTest = true;
					arguments = Utilities.removeItemAtIndex(arguments, 0);
				} else if ("importblob".equals(modeKeyword)) {
					blobImport = true;
					arguments = Utilities.removeItemAtIndex(arguments, 0);
				} else if ("createtruststore".equals(modeKeyword)) {
					createTrustStore = true;
					arguments = Utilities.removeItemAtIndex(arguments, 0);
				}
			}

			final DbImportDefinition dbImportDefinition = new DbImportDefinition();
			final BlobImportDefinition blobImportDefinition = new BlobImportDefinition();
			final ConnectionTestDefinition connectionTestDefinition = new ConnectionTestDefinition();

			// Read the parameters
			for (int i = 0; i < arguments.length; i++) {
				boolean wasAllowedParam = createTrustStore;

				// In menu mode the import, blob import and connection test parameters are all read from the same
				// arguments. A flag handled by one parameter set (including its value, which advances "i") must not
				// be taken as a positional parameter (vendor, host, ...) of the next parameter set.
				final int startIndex = i;
				final String argumentLowerCase = arguments[i].toLowerCase(Locale.ROOT);
				final boolean isBlobImportFlag = BLOB_IMPORT_FLAGS.contains(argumentLowerCase);
				final boolean isConnectionTestFlag = CONNECTION_TEST_FLAGS.contains(argumentLowerCase);
				boolean importFlagHandled = false;
				boolean blobImportFlagHandled = false;

				if (openGui || openMenu || (!blobImport && !connectionTest && !createTrustStore)) {
					boolean positionalParameter = false;
					if ("-x".equalsIgnoreCase(arguments[i])) {
						i++;
						if (i >= arguments.length) {
							throw new ParameterException(arguments[i - 1], "Missing parameter for import format");
						} else if (Utilities.isBlank(arguments[i])) {
							throw new ParameterException(arguments[i - 1] + " " + arguments[i], "Invalid parameter for import format");
						} else {
							dbImportDefinition.setDataType(DataType.getFromString(arguments[i]));
						}
						wasAllowedParam = true;
					} else if ("-n".equalsIgnoreCase(arguments[i])) {
						i++;
						if (i >= arguments.length) {
							throw new ParameterException(arguments[i - 1], "Missing parameter null value string");
						} else {
							dbImportDefinition.setNullValueString(arguments[i]);
						}
						wasAllowedParam = true;
					} else if ("-m".equalsIgnoreCase(arguments[i])) {
						i++;
						if (i >= arguments.length) {
							throw new ParameterException(arguments[i - 1], "Missing parameter for mapping");
						} else if (Utilities.isBlank(arguments[i])) {
							throw new ParameterException(arguments[i - 1] + " " + arguments[i], "Invalid parameter for mapping");
						} else if (Utilities.isNotBlank(dbImportDefinition.getMapping())) {
							throw new ParameterException(arguments[i - 1] + " " + arguments[i], "Mapping is already defined");
						} else {
							dbImportDefinition.setMapping(arguments[i]);
						}
						wasAllowedParam = true;
					} else if ("-mf".equalsIgnoreCase(arguments[i])) {
						i++;
						if (i >= arguments.length) {
							throw new ParameterException(arguments[i - 1], "Missing parameter for mapping file");
						} else if (Utilities.isBlank(arguments[i])) {
							throw new ParameterException(arguments[i - 1] + " " + arguments[i], "Invalid parameter for mapping file");
						} else if (!new File(arguments[i]).exists()) {
							throw new ParameterException(arguments[i - 1] + " " + arguments[i], "Mapping file does not exist");
						} else if (Utilities.isNotBlank(dbImportDefinition.getMapping())) {
							throw new ParameterException(arguments[i - 1] + " " + arguments[i], "Mapping is already defined");
						} else {
							dbImportDefinition.setMapping(new String(Utilities.readFileToByteArray(new File(arguments[i])), StandardCharsets.UTF_8));
						}
						wasAllowedParam = true;
					} else if ("-l".equalsIgnoreCase(arguments[i])) {
						dbImportDefinition.setLog(true);
						wasAllowedParam = true;
					} else if ("-v".equalsIgnoreCase(arguments[i])) {
						dbImportDefinition.setVerbose(true);
						wasAllowedParam = true;
					} else if ("-e".equalsIgnoreCase(arguments[i])) {
						i++;
						if (i >= arguments.length) {
							throw new ParameterException(arguments[i - 1], "Missing parameter encoding");
						} else if (Utilities.isBlank(arguments[i])) {
							throw new ParameterException(arguments[i - 1] + " " + arguments[i], "Invalid parameter encoding");
						} else {
							dbImportDefinition.setEncoding(Charset.forName(arguments[i]));
						}
						wasAllowedParam = true;
					} else if ("-s".equalsIgnoreCase(arguments[i])) {
						i++;
						if (i >= arguments.length) {
							throw new ParameterException(arguments[i - 1], "Missing parameter separator character");
						} else if (Utilities.isBlank(arguments[i]) || arguments[i].length() != 1) {
							throw new ParameterException(arguments[i - 1] + " " + arguments[i], "Invalid parameter separator character");
						} else {
							dbImportDefinition.setSeparator(arguments[i].charAt(0));
						}
						wasAllowedParam = true;
					} else if ("-q".equalsIgnoreCase(arguments[i])) {
						i++;
						if (i >= arguments.length) {
							throw new ParameterException(arguments[i - 1], "Missing parameter stringquote character");
						} else if ("null".equalsIgnoreCase(arguments[i]) || "none".equalsIgnoreCase(arguments[i])) {
							dbImportDefinition.setStringQuote(null);
						} else if (Utilities.isBlank(arguments[i]) || arguments[i].length() != 1) {
							throw new ParameterException(arguments[i - 1] + " " + arguments[i], "Invalid parameter stringquote character (use 'null' or 'none' for empty)");
						} else {
							dbImportDefinition.setStringQuote(arguments[i].charAt(0));
						}
						wasAllowedParam = true;
					} else if ("-qe".equalsIgnoreCase(arguments[i])) {
						i++;
						if (i >= arguments.length) {
							throw new ParameterException(arguments[i - 1], "Missing parameter stringquote escape character");
						} else if (Utilities.isBlank(arguments[i]) || arguments[i].length() != 1) {
							throw new ParameterException(arguments[i - 1] + " " + arguments[i], "Invalid parameter stringquote escape character");
						} else {
							dbImportDefinition.setEscapeStringQuote(arguments[i].charAt(0));
						}
						wasAllowedParam = true;
					} else if ("-noescapesequences".equalsIgnoreCase(arguments[i])) {
						dbImportDefinition.setInterpretEscapeSequences(false);
						wasAllowedParam = true;
					} else if ("-a".equalsIgnoreCase(arguments[i])) {
						dbImportDefinition.setAllowUnderfilledLines(true);
						wasAllowedParam = true;
					} else if ("-r".equalsIgnoreCase(arguments[i])) {
						dbImportDefinition.setRemoveSurplusEmptyTrailingColumns(true);
						wasAllowedParam = true;
					} else if ("-t".equalsIgnoreCase(arguments[i])) {
						dbImportDefinition.setTrimData(true);
						wasAllowedParam = true;
					} else if ("-i".equalsIgnoreCase(arguments[i])) {
						i++;
						if (i >= arguments.length) {
							throw new ParameterException(arguments[i - 1], "Missing parameter importmode");
						} else if (Utilities.isBlank(arguments[i])) {
							throw new ParameterException(arguments[i - 1] + " " + arguments[i], "Invalid parameter importmode");
						} else {
							dbImportDefinition.setImportMode(ImportMode.getFromString(arguments[i]));
						}
						wasAllowedParam = true;
					} else if ("-d".equalsIgnoreCase(arguments[i])) {
						i++;
						if (i >= arguments.length) {
							throw new ParameterException(arguments[i - 1], "Missing parameter duplicatemode");
						} else if (Utilities.isBlank(arguments[i])) {
							throw new ParameterException(arguments[i - 1] + " " + arguments[i], "Invalid parameter duplicatemode");
						} else {
							dbImportDefinition.setDuplicateMode(DuplicateMode.getFromString(arguments[i]));
						}
						wasAllowedParam = true;
					} else if ("-u".equalsIgnoreCase(arguments[i])) {
						dbImportDefinition.setUpdateNullData(false);
						wasAllowedParam = true;
					} else if ("-k".equalsIgnoreCase(arguments[i])) {
						i++;
						if (i >= arguments.length) {
							throw new ParameterException(arguments[i - 1], "Missing parameter keycolumnslist");
						} else if (Utilities.isBlank(arguments[i])) {
							throw new ParameterException(arguments[i - 1] + " " + arguments[i], "Invalid parameter keycolumnslist");
						} else {
							dbImportDefinition.setKeycolumns(Utilities.splitAndTrimList(arguments[i]));
						}
						wasAllowedParam = true;
					} else if ("-insvalues".equalsIgnoreCase(arguments[i])) {
						i++;
						if (i >= arguments.length) {
							throw new ParameterException(arguments[i - 1], "Missing parameter valueslist");
						} else if (Utilities.isBlank(arguments[i])) {
							throw new ParameterException(arguments[i - 1] + " " + arguments[i], "Invalid parameter valueslist");
						} else {
							dbImportDefinition.setAdditionalInsertValues(arguments[i]);
						}
						wasAllowedParam = true;
					} else if ("-updvalues".equalsIgnoreCase(arguments[i])) {
						i++;
						if (i >= arguments.length) {
							throw new ParameterException(arguments[i - 1], "Missing parameter valueslist");
						} else if (Utilities.isBlank(arguments[i])) {
							throw new ParameterException(arguments[i - 1] + " " + arguments[i], "Invalid parameter valueslist");
						} else {
							dbImportDefinition.setAdditionalUpdateValues(arguments[i]);
						}
						wasAllowedParam = true;
					} else if ("-create".equalsIgnoreCase(arguments[i])) {
						dbImportDefinition.setCreateTable(true);
						wasAllowedParam = true;
					} else if ("-structure".equalsIgnoreCase(arguments[i])) {
						i++;
						if (i >= arguments.length) {
							throw new ParameterException(arguments[i - 1], "Missing parameter for structure file path");
						} else if (Utilities.isBlank(arguments[i])) {
							throw new ParameterException(arguments[i - 1] + " " + arguments[i], "Invalid parameter for structure file path");
						} else if (!new File(arguments[i]).exists()) {
							throw new ParameterException(arguments[i - 1] + " " + arguments[i], "Structure file file does not exist");
						} else if (Utilities.isNotBlank(dbImportDefinition.getStructureFilePath())) {
							throw new ParameterException(arguments[i - 1] + " " + arguments[i], "Structure file path is already defined");
						} else {
							dbImportDefinition.setStructureFilePath(arguments[i]);
						}
						wasAllowedParam = true;
					} else if ("-logerrors".equalsIgnoreCase(arguments[i])) {
						dbImportDefinition.setLogErroneousData(true);
						wasAllowedParam = true;
					} else if ("-noheaders".equalsIgnoreCase(arguments[i])) {
						dbImportDefinition.setNoHeaders(true);
						wasAllowedParam = true;
					} else if ("-c".equalsIgnoreCase(arguments[i])) {
						dbImportDefinition.setCompleteCommit(true);
						wasAllowedParam = true;
					} else if ("-nonewindex".equalsIgnoreCase(arguments[i])) {
						dbImportDefinition.setCreateNewIndexIfNeeded(false);
						wasAllowedParam = true;
					} else if ("-deactivatefk".equalsIgnoreCase(arguments[i])) {
						dbImportDefinition.setDeactivateForeignKeyConstraints(true);
						wasAllowedParam = true;
					} else if ("-deactivatetriggers".equalsIgnoreCase(arguments[i])) {
						dbImportDefinition.setDeactivateTriggers(true);
						wasAllowedParam = true;
					} else if ("-data".equalsIgnoreCase(arguments[i])) {
						dbImportDefinition.setInlineData(true);
						wasAllowedParam = true;
					} else if ("-dp".equalsIgnoreCase(arguments[i])) {
						i++;
						if (i >= arguments.length) {
							throw new ParameterException(arguments[i - 1], "Missing parameter for data path");
						} else if (Utilities.isBlank(arguments[i])) {
							throw new ParameterException(arguments[i - 1] + " " + arguments[i], "Invalid parameter for data path");
						} else if (Utilities.isNotBlank(dbImportDefinition.getDataPath())) {
							throw new ParameterException(arguments[i - 1] + " " + arguments[i], "Data path is already defined");
						} else {
							dbImportDefinition.setDataPath(arguments[i]);
						}
						wasAllowedParam = true;
					} else if ("-sp".equalsIgnoreCase(arguments[i])) {
						i++;
						if (i >= arguments.length) {
							throw new ParameterException(arguments[i - 1], "Missing parameter for schema file path");
						} else if (Utilities.isBlank(arguments[i])) {
							throw new ParameterException(arguments[i - 1] + " " + arguments[i], "Invalid parameter for schema file path");
						} else if (!new File(arguments[i]).exists()) {
							throw new ParameterException(arguments[i - 1] + " " + arguments[i], "Schema file file does not exist");
						} else if (Utilities.isNotBlank(dbImportDefinition.getSchemaFilePath())) {
							throw new ParameterException(arguments[i - 1] + " " + arguments[i], "Schema file path is already defined");
						} else {
							dbImportDefinition.setSchemaFilePath(arguments[i]);
						}
						wasAllowedParam = true;
					} else if ("-zippassword".equalsIgnoreCase(arguments[i])) {
						i++;
						if (i >= arguments.length) {
							throw new ParameterException(arguments[i - 1], "Missing value for parameter zippassword");
						} else {
							String zipPassword = arguments[i];
							if ((zipPassword.startsWith("\"") && zipPassword.endsWith("\"")) || (zipPassword.startsWith("'") && zipPassword.endsWith("'"))) {
								zipPassword = zipPassword.substring(1, zipPassword.length() - 1);
							}
							dbImportDefinition.setZipPassword(zipPassword.toCharArray());
						}
						wasAllowedParam = true;
					} else if ("-kdbxpassword".equalsIgnoreCase(arguments[i])) {
						// Documented in help.txt, but was missing here
						i++;
						if (i >= arguments.length) {
							throw new ParameterException(arguments[i - 1], "Missing value for parameter kdbxpassword");
						} else {
							String kdbxPassword = arguments[i];
							if ((kdbxPassword.startsWith("\"") && kdbxPassword.endsWith("\"")) || (kdbxPassword.startsWith("'") && kdbxPassword.endsWith("'"))) {
								kdbxPassword = kdbxPassword.substring(1, kdbxPassword.length() - 1);
							}
							dbImportDefinition.setKdbxPassword(kdbxPassword.toCharArray());
						}
						wasAllowedParam = true;
					} else if ("-dbtz".equalsIgnoreCase(arguments[i])) {
						i++;
						if (i >= arguments.length) {
							throw new ParameterException(arguments[i - 1], "Missing value for parameter database timezone");
						} else {
							dbImportDefinition.setDatabaseTimeZone(arguments[i]);
						}
						wasAllowedParam = true;
					} else if ("-idtz".equalsIgnoreCase(arguments[i])) {
						i++;
						if (i >= arguments.length) {
							throw new ParameterException(arguments[i - 1], "Missing value for parameter import data timezone");
						} else {
							dbImportDefinition.setImportDataTimeZone(arguments[i]);
						}
						wasAllowedParam = true;
					} else if ("-dateFormat".equalsIgnoreCase(arguments[i])) {
						i++;
						if (i >= arguments.length) {
							throw new ParameterException(arguments[i - 1], "Missing parameter dateFormat");
						} else if (Utilities.isBlank(arguments[i])) {
							throw new ParameterException(arguments[i - 1] + " " + arguments[i], "Invalid parameter dateFormat");
						} else {
							dbImportDefinition.setDateFormat(arguments[i]);
						}
						wasAllowedParam = true;
					} else if ("-dateTimeFormat".equalsIgnoreCase(arguments[i])) {
						i++;
						if (i >= arguments.length) {
							throw new ParameterException(arguments[i - 1], "Missing parameter dateTimeFormat");
						} else if (Utilities.isBlank(arguments[i])) {
							throw new ParameterException(arguments[i - 1] + " " + arguments[i], "Invalid parameter dateTimeFormat");
						} else {
							dbImportDefinition.setDateTimeFormat(arguments[i]);
						}
						wasAllowedParam = true;
					} else if ("-table".equalsIgnoreCase(arguments[i])) {
						i++;
						if (i >= arguments.length) {
							throw new ParameterException(arguments[i - 1], "Missing value for parameter import table name");
						} else {
							dbImportDefinition.setTableName(arguments[i]);
						}
						wasAllowedParam = true;
					} else if ("-import".equalsIgnoreCase(arguments[i])) {
						i++;
						if (i >= arguments.length) {
							throw new ParameterException(arguments[i - 1], "Missing value for parameter importfilepathOrData");
						} else {
							dbImportDefinition.setImportFilePathOrData(arguments[i], true);
						}
						wasAllowedParam = true;
					} else if ("-secure".equalsIgnoreCase(arguments[i])) {
						dbImportDefinition.setSecureConnection(true);
						wasAllowedParam = true;
					} else if ("-truststore".equalsIgnoreCase(arguments[i])) {
						i++;
						if (i >= arguments.length) {
							throw new ParameterException(arguments[i - 1], "Missing value for parameter truststore");
						} else {
							dbImportDefinition.setTrustStoreFile(new File(arguments[i]));
						}
						wasAllowedParam = true;
					} else if ("-truststorepassword".equalsIgnoreCase(arguments[i])) {
						i++;
						if (i >= arguments.length) {
							throw new ParameterException(arguments[i - 1], "Missing value for parameter truststorepassword");
						} else {
							dbImportDefinition.setTrustStorePassword(Utilities.isNotEmpty(arguments[i]) ? arguments[i].toCharArray() : null);
						}
						wasAllowedParam = true;
					} else if ("-batchblocksize".equalsIgnoreCase(arguments[i])) {
						i++;
						if (i >= arguments.length) {
							throw new ParameterException(arguments[i - 1], "Missing value for parameter batchblocksize");
						} else {
							int value;
							try {
								value = Integer.parseInt(arguments[i]);
							} catch (@SuppressWarnings("unused") final NumberFormatException e) {
								throw new ParameterException(arguments[i - 1], "Invalid value for parameter batchblocksize, must be integer");
							}
							dbImportDefinition.setBatchBlockSize(value);
						}
						wasAllowedParam = true;
					} else if ("-noSingleMode".equalsIgnoreCase(arguments[i])) {
						dbImportDefinition.setPreventBatchFallbackToSingleLineOnErrors(true);
						wasAllowedParam = true;
					} else if (!(openMenu && (isBlobImportFlag || isConnectionTestFlag))) {
						positionalParameter = true;
						if (dbImportDefinition.getDbVendor() == null) {
							dbImportDefinition.setDbVendor(DbVendor.getDbVendorByName(arguments[i]));
							wasAllowedParam = true;
						} else if (dbImportDefinition.getHostnameAndPort() == null && dbImportDefinition.getDbVendor() != DbVendor.SQLite && dbImportDefinition.getDbVendor() != DbVendor.Derby) {
							dbImportDefinition.setHostnameAndPort(arguments[i]);
							wasAllowedParam = true;
						} else if (dbImportDefinition.getDbName() == null) {
							dbImportDefinition.setDbName(arguments[i]);
							wasAllowedParam = true;
						} else if (dbImportDefinition.getUsername() == null && dbImportDefinition.getDbVendor() != DbVendor.SQLite && dbImportDefinition.getDbVendor() != DbVendor.Derby) {
							dbImportDefinition.setUsername(arguments[i]);
							wasAllowedParam = true;
						} else if (dbImportDefinition.getPassword() == null && dbImportDefinition.getDbVendor() != DbVendor.SQLite && dbImportDefinition.getDbVendor() != DbVendor.Derby) {
							dbImportDefinition.setPassword(arguments[i] == null ? null : arguments[i].toCharArray());
							wasAllowedParam = true;
						}
					}
					importFlagHandled = wasAllowedParam && !positionalParameter;
				}

				if ((openMenu || blobImport) && i == startIndex && !importFlagHandled) {
					boolean positionalParameter = false;
					if ("-updatesql".equalsIgnoreCase(arguments[i])) {
						i++;
						if (i >= arguments.length) {
							throw new ParameterException(arguments[i - 1], "Missing parameter value for updatesql");
						} else if (!arguments[i].contains("?")) {
							throw new ParameterException(arguments[i], "Updatesql does not contain mandatory '?' placeholder");
						} else {
							blobImportDefinition.setBlobImportStatement(arguments[i]);
						}
						wasAllowedParam = true;
					} else if ("-blobfile".equalsIgnoreCase(arguments[i])) {
						i++;
						if (i >= arguments.length) {
							throw new ParameterException(arguments[i - 1], "Missing parameter value for blobfile");
						} else if (!new File(arguments[i]).exists()) {
							throw new ParameterException(arguments[i], "Blob import file does not exist");
						} else {
							blobImportDefinition.setImportFilePath(arguments[i]);
						}
						wasAllowedParam = true;
					} else if ("-secure".equalsIgnoreCase(arguments[i])) {
						blobImportDefinition.setSecureConnection(true);
						wasAllowedParam = true;
					} else if ("-truststore".equalsIgnoreCase(arguments[i])) {
						i++;
						if (i >= arguments.length) {
							throw new ParameterException(arguments[i - 1], "Missing value for parameter truststore");
						} else {
							blobImportDefinition.setTrustStoreFile(new File(arguments[i]));
						}
						wasAllowedParam = true;
					} else if ("-truststorepassword".equalsIgnoreCase(arguments[i])) {
						i++;
						if (i >= arguments.length) {
							throw new ParameterException(arguments[i - 1], "Missing value for parameter truststorepassword");
						} else {
							blobImportDefinition.setTrustStorePassword(Utilities.isNotEmpty(arguments[i]) ? arguments[i].toCharArray() : null);
						}
						wasAllowedParam = true;
					} else if (!(openMenu && isConnectionTestFlag)) {
						positionalParameter = true;
						if (blobImportDefinition.getDbVendor() == null) {
							blobImportDefinition.setDbVendor(DbVendor.getDbVendorByName(arguments[i]));
							wasAllowedParam = true;
						} else if (blobImportDefinition.getHostnameAndPort() == null && blobImportDefinition.getDbVendor() != DbVendor.SQLite && blobImportDefinition.getDbVendor() != DbVendor.Derby) {
							blobImportDefinition.setHostnameAndPort(arguments[i]);
							wasAllowedParam = true;
						} else if (blobImportDefinition.getDbName() == null) {
							blobImportDefinition.setDbName(arguments[i]);
							wasAllowedParam = true;
						} else if (blobImportDefinition.getUsername() == null && blobImportDefinition.getDbVendor() != DbVendor.SQLite && blobImportDefinition.getDbVendor() != DbVendor.Derby) {
							blobImportDefinition.setUsername(arguments[i]);
							wasAllowedParam = true;
						} else if (blobImportDefinition.getPassword() == null && blobImportDefinition.getDbVendor() != DbVendor.SQLite && blobImportDefinition.getDbVendor() != DbVendor.Derby) {
							blobImportDefinition.setPassword(arguments[i] == null ? null : arguments[i].toCharArray());
							wasAllowedParam = true;
						}
					}
					blobImportFlagHandled = wasAllowedParam && !positionalParameter;
				}

				if ((openMenu || connectionTest) && i == startIndex && !importFlagHandled && !blobImportFlagHandled) {
					if ("-iter".equalsIgnoreCase(arguments[i])) {
						i++;
						if (i >= arguments.length) {
							throw new ParameterException(arguments[i - 1], "Missing parameter for connectiontest iterations");
						} else if (!NumberUtilities.isInteger(arguments[i])) {
							throw new ParameterException(arguments[i - 1] + " " + arguments[i], "Invalid parameter for connectiontest iterations");
						} else {
							connectionTestDefinition.setIterations(Integer.parseInt(arguments[i]));
						}
						wasAllowedParam = true;
					} else if ("-sleep".equalsIgnoreCase(arguments[i])) {
						i++;
						if (i >= arguments.length) {
							throw new ParameterException(arguments[i - 1], "Missing parameter for connectiontest sleep time");
						} else if (!NumberUtilities.isInteger(arguments[i])) {
							throw new ParameterException(arguments[i - 1] + " " + arguments[i], "Invalid parameter for connectiontest sleep time");
						} else {
							connectionTestDefinition.setSleepTime(Integer.parseInt(arguments[i]));
						}
						wasAllowedParam = true;
					} else if ("-check".equalsIgnoreCase(arguments[i])) {
						i++;
						if (i >= arguments.length) {
							throw new ParameterException(arguments[i - 1], "Missing parameter value for check");
						} else {
							connectionTestDefinition.setCheckStatement(arguments[i]);
						}
						wasAllowedParam = true;
					} else if ("-secure".equalsIgnoreCase(arguments[i])) {
						connectionTestDefinition.setSecureConnection(true);
						wasAllowedParam = true;
					} else if ("-truststore".equalsIgnoreCase(arguments[i])) {
						i++;
						if (i >= arguments.length) {
							throw new ParameterException(arguments[i - 1], "Missing value for parameter truststore");
						} else {
							connectionTestDefinition.setTrustStoreFile(new File(arguments[i]));
						}
						wasAllowedParam = true;
					} else if ("-truststorepassword".equalsIgnoreCase(arguments[i])) {
						i++;
						if (i >= arguments.length) {
							throw new ParameterException(arguments[i - 1], "Missing value for parameter truststorepassword");
						} else {
							connectionTestDefinition.setTrustStorePassword(Utilities.isNotEmpty(arguments[i]) ? arguments[i].toCharArray() : null);
						}
						wasAllowedParam = true;
					} else {
						if (connectionTestDefinition.getDbVendor() == null) {
							connectionTestDefinition.setDbVendor(DbVendor.getDbVendorByName(arguments[i]));
							wasAllowedParam = true;
						} else if (connectionTestDefinition.getHostnameAndPort() == null && connectionTestDefinition.getDbVendor() != DbVendor.SQLite && connectionTestDefinition.getDbVendor() != DbVendor.Derby) {
							connectionTestDefinition.setHostnameAndPort(arguments[i]);
							wasAllowedParam = true;
						} else if (connectionTestDefinition.getDbName() == null) {
							connectionTestDefinition.setDbName(arguments[i]);
							wasAllowedParam = true;
						} else if (connectionTestDefinition.getUsername() == null && connectionTestDefinition.getDbVendor() != DbVendor.SQLite && connectionTestDefinition.getDbVendor() != DbVendor.Derby) {
							connectionTestDefinition.setUsername(arguments[i]);
							wasAllowedParam = true;
						} else if (connectionTestDefinition.getPassword() == null && connectionTestDefinition.getDbVendor() != DbVendor.SQLite && connectionTestDefinition.getDbVendor() != DbVendor.Derby) {
							connectionTestDefinition.setPassword(arguments[i] == null ? null : arguments[i].toCharArray());
							wasAllowedParam = true;
						}
					}
				}

				if (!wasAllowedParam) {
					throw new ParameterException(arguments[i], "Invalid parameter");
				}
			}

			if (createTrustStore) {
				// "createtruststore hostname[:port] truststorefilePath [truststorepassword]"
				if (arguments.length < 2 || arguments.length > 3) {
					throw new ParameterException("createtruststore", "Expected parameters: hostname[:port] truststorefilePath [truststorepassword]");
				}
				final char[] trustStorePassword = arguments.length > 2 && Utilities.isNotEmpty(arguments[2]) ? arguments[2].toCharArray() : null;
				TrustManagerUtilities.createTrustStoreFile(arguments[0], 443, new File(arguments[1]), trustStorePassword, null);
				System.out.println();
				System.out.println("Created TrustStore in file '" + arguments[1] + "'");
				return 0;
			}

			if (openMenu) {
				LangResources.enforceDefaultLocale();

				if (System.console() == null) {
					System.err.println("Couldn't get Console instance for menu");
					return 1;
				}

				// Always the same definition objects, which are then used below for the chosen action. Before, the blob
				// definition was only passed for "importblob", which can never be combined with "menu", so the blob update
				// menu edited its own definition and the action below used the (empty) one parsed from the command line.
				final int consoleMenuExecutionCode = openConsoleMenu(dbImportDefinition, blobImportDefinition, connectionTestDefinition);

				if (consoleMenuExecutionCode == -1) {
					// Validate all given parameters
					dbImportDefinition.checkParameters();

					// Start the import worker for terminal output
					try {
						new DbImport().importData(dbImportDefinition);
						return 0;
					} catch (final DbImportException e) {
						System.err.println(e.getMessage());
						return 1;
					} catch (final Exception e) {
						e.printStackTrace();
						return 1;
					}
				} else if (consoleMenuExecutionCode == -2) {
					// Validate all given parameters
					blobImportDefinition.checkParameters();

					DbUtilities.updateBlob(blobImportDefinition, blobImportDefinition.getBlobImportStatement(), blobImportDefinition.getImportFilePath());
					return 0;
				} else if (consoleMenuExecutionCode == -3) {
					// Validate all given parameters
					connectionTestDefinition.checkParameters();

					return connectionTest(connectionTestDefinition);
				} else if (consoleMenuExecutionCode == -4) {
					// Update application
					final DbImport dbImport = new DbImport();
					ApplicationUpdateUtilities.executeUpdate(dbImport, DbImport.VERSIONINFO_DOWNLOAD_URL, applicationConfiguration.getProxyConfiguration(), DbImport.APPLICATION_NAME, DbImport.VERSION, DbImport.TRUSTED_UPDATE_CA_CERTIFICATES, null, null, null, false, false);
					return 0;
				} else if (consoleMenuExecutionCode == -5) {
					// Create TrustStore
					TrustManagerUtilities.createTrustStoreFile(connectionTestDefinition.getHostnameAndPort(), 443, connectionTestDefinition.getTrustStoreFile(), connectionTestDefinition.getTrustStorePassword(), null);
					System.out.println();
					System.out.println("Created TrustStore in file '" + connectionTestDefinition.getTrustStoreFile().getAbsolutePath() + "'");
					return 0;
				} else {
					System.out.println();
					System.out.println("Bye");
					System.out.println();
					return 0;
				}
			} else if (openGui) {
				// open the preconfigured GUI
				de.soderer.utilities.swing.SwingUtilities.setSystemLookAndFeel();

				try {
					final DbImportGui dbImportGui = new DbImportGui(dbImportDefinition);
					SwingUtilities.invokeLater(new Runnable() {
						@Override
						public void run() {
							dbImportGui.setVisible(true);
						}
					});
					return -1;
				} catch (final Exception e) {
					e.printStackTrace();
					return 1;
				}
			} else if (blobImport) {
				LangResources.enforceDefaultLocale();

				// If started without GUI we may enter the missing password via the terminal
				if (Utilities.isNotBlank(blobImportDefinition.getUsername()) && blobImportDefinition.getPassword() == null
						&& blobImportDefinition.getDbVendor() != DbVendor.SQLite
						&& blobImportDefinition.getDbVendor() != DbVendor.Derby
						&& blobImportDefinition.getDbVendor() != DbVendor.Cassandra) {
					final char[] passwordArray = new PasswordConsoleInput().withPrompt(LangResources.get("enterDbPassword") + ": ").readInput();
					blobImportDefinition.setPassword(passwordArray);
				}

				DbUtilities.updateBlob(blobImportDefinition, blobImportDefinition.getBlobImportStatement(), blobImportDefinition.getImportFilePath());
				return 0;
			} else if (connectionTest) {
				LangResources.enforceDefaultLocale();

				// If started without GUI we may enter the missing password via the terminal
				if (Utilities.isNotBlank(connectionTestDefinition.getUsername()) && connectionTestDefinition.getPassword() == null
						&& connectionTestDefinition.getDbVendor() != DbVendor.SQLite
						&& connectionTestDefinition.getDbVendor() != DbVendor.Derby
						&& connectionTestDefinition.getDbVendor() != DbVendor.Cassandra) {
					final char[] passwordArray = new PasswordConsoleInput().withPrompt(LangResources.get("enterDbPassword") + ": ").readInput();
					connectionTestDefinition.setPassword(passwordArray);
				}

				return connectionTest(connectionTestDefinition);
			} else {
				LangResources.enforceDefaultLocale();

				// If started without GUI we may enter the missing password via the terminal
				if (Utilities.isNotBlank(dbImportDefinition.getUsername()) && dbImportDefinition.getPassword() == null
						&& dbImportDefinition.getDbVendor() != DbVendor.SQLite
						&& dbImportDefinition.getDbVendor() != DbVendor.Derby
						&& dbImportDefinition.getDbVendor() != DbVendor.Cassandra) {
					final char[] passwordArray = new PasswordConsoleInput().withPrompt(LangResources.get("enterDbPassword") + ": ").readInput();
					dbImportDefinition.setPassword(passwordArray);
				}

				// Validate all given parameters
				dbImportDefinition.checkParameters();

				// Start the import worker for terminal output
				try {
					new DbImport().importData(dbImportDefinition);
					return 0;
				} catch (final DbImportException e) {
					System.err.println(e.getMessage());
					return 1;
				} catch (final Exception e) {
					e.printStackTrace();
					return 1;
				}
			}
		} catch (final ParameterException e) {
			System.err.println(e.getMessage());
			System.err.println();
			System.err.println(getUsageMessage());
			return 1;
		} catch (final Exception e) {
			// Some exceptions (e.g. NullPointerException) carry no message, which would print just "null"
			System.err.println(e.getMessage() != null ? e.getMessage() : e.toString());
			return 1;
		}
	}

	/**
	 * Instantiates a new database csv import.
	 *
	 * @throws Exception the exception
	 */
	public DbImport() throws Exception {
		super(APPLICATION_NAME, VERSION);
	}

	/**
	 * Import.
	 *
	 * @param dbImportDefinitionToExecute the database csv import definition
	 * @throws Exception the exception
	 */
	private void importData(final DbImportDefinition dbImportDefinition) throws Exception {
		dbImportDefinitionToExecute = dbImportDefinition;

		List<File> filesToImport;
		if (!dbImportDefinitionToExecute.isInlineData()) {
			if (dbImportDefinitionToExecute.getImportFilePathOrData().contains("?") || dbImportDefinitionToExecute.getImportFilePathOrData().contains("*")) {
				final int lastSeparator = Math.max(dbImportDefinitionToExecute.getImportFilePathOrData().lastIndexOf("/"), dbImportDefinitionToExecute.getImportFilePathOrData().lastIndexOf("\\"));
				// A pattern without directory (e.g. "*.csv") refers to the current directory
				String directoryPath = lastSeparator < 0 ? "." : dbImportDefinitionToExecute.getImportFilePathOrData().substring(0, lastSeparator);
				directoryPath = Utilities.replaceUsersHome(directoryPath);
				final String filePattern = dbImportDefinitionToExecute.getImportFilePathOrData().substring(lastSeparator + 1);
				if (directoryPath.contains("?") || directoryPath.contains("*")) {
					throw new DbImportException("Import directory path contains wildcards, but wildcards only allowed for filenames: " + (directoryPath));
				} else if (!new File(directoryPath).exists()) {
					throw new DbImportException("Import path does not exist: " + (directoryPath));
				} else if (!new File((directoryPath)).isDirectory()) {
					throw new DbImportException("Import path is not a directory: " + (directoryPath));
				} else {
					filesToImport = FileUtilities.getFilesByPattern(new File(directoryPath), filePattern.replace(".", "\\.").replace("?", ".").replace("*", ".*"), false);
					if (filesToImport.size() == 0) {
						throw new DbImportException("Import file pattern has no matching files: " + (directoryPath));
					}
				}
			} else {
				if (!new File(dbImportDefinitionToExecute.getImportFilePathOrData()).exists()) {
					throw new DbImportException("Import file does not exist: " + (dbImportDefinitionToExecute.getImportFilePathOrData()));
				} else if (new File(dbImportDefinitionToExecute.getImportFilePathOrData()).isDirectory()) {
					throw new DbImportException("Import path is a directory: " + (dbImportDefinitionToExecute.getImportFilePathOrData()));
				} else {
					filesToImport = new ArrayList<>();
					filesToImport.add(new File(dbImportDefinitionToExecute.getImportFilePathOrData()));
				}
			}

			if (filesToImport.size() == 0) {
				throw new DbImportException("No files for import were found");
			} else if (filesToImport.size() == 1) {
				final File fileToImport = filesToImport.get(0);
				String tableName = dbImportDefinitionToExecute.getTableName();
				if ("*".equals(tableName)) {
					tableName = DbImport.getImportTablenameFromFilename(fileToImport.getName());
					dbImportDefinitionToExecute.setTableName(tableName);
				}
				System.out.println("Importing file '" + fileToImport.getAbsolutePath() + "' into table '"+ tableName + "'");
				importFileOrData(dbImportDefinitionToExecute, tableName, fileToImport.getAbsolutePath());
			} else {
				Collections.sort(filesToImport);
				multiImportFiles(dbImportDefinitionToExecute, filesToImport, dbImportDefinitionToExecute.getTableName());
			}
		} else {
			importFileOrData(dbImportDefinitionToExecute, dbImportDefinitionToExecute.getTableName(), dbImportDefinitionToExecute.getImportFilePathOrData());
		}
	}

	private void importFileOrData(final DbImportDefinition dbImportDefinition, final String tableNameForImport, final String filePathOrImportData) throws Exception, ExecutionException {
		dbImportDefinitionToExecute = dbImportDefinition;

		try {
			final DbImportWorker worker = dbImportDefinitionToExecute.getConfiguredWorker(this, false, tableNameForImport, filePathOrImportData);
			if (dbImportDefinitionToExecute.isVerbose()) {
				System.out.println(worker.getConfigurationLogString());
				System.out.println();
			}

			worker.setProgressDisplayDelayMilliseconds(2000);
			worker.run();

			// Get result to trigger possible Exception
			worker.get();

			// Only show errors. Other statistics are kept in log file if verbose was set
			if (worker.getNotImportedItems().size() > 0) {
				String errorText = "Amount of not imported items: " + worker.getNotImportedItems().size() + "\n";
				if (worker.getNotImportedItems().size() > 0) {
					final List<String> errorList = new ArrayList<>();
					for (int i = 0; i < Math.min(10, worker.getNotImportedItems().size()); i++) {
						errorList.add(Integer.toString(worker.getNotImportedItems().get(i)) + ": " + worker.getNotImportedItemsReasons().get(i));
					}
					if (worker.getNotImportedItems().size() > 10) {
						errorList.add("...");
					}
					errorText += "Not imported items indices: \n" + Utilities.join(errorList, " \n") + "\n";
				}
				System.err.println(errorText);
			}

			if (worker.getCreatedNewIndexName() != null) {
				System.out.println("Created new index name: " + worker.getCreatedNewIndexName());
			}

			System.out.println("Imported items: " + worker.getImportedItems());

			if (dbImportDefinitionToExecute.isVerbose()) {
				System.out.println(LangResources.get("importeddataamount") + ": " + Utilities.getHumanReadableNumber(worker.getImportedDataAmount(), "Byte", false, 5, false, Locale.getDefault()));
			}

			System.out.println();
		} catch (final ExecutionException e) {
			if (e.getCause() instanceof Exception) {
				throw (Exception) e.getCause();
			} else {
				throw e;
			}
		} catch (final Exception e) {
			throw e;
		}
	}

	private void multiImportFiles(final DbImportDefinition dbImportDefinition, final List<File> filesToImport, final String tableName) throws Exception {
		try {
			final DbImportMultiWorker worker = new DbImportMultiWorker(dbImportDefinition, filesToImport, tableName);

			worker.setParent(this);

			worker.setProgressDisplayDelayMilliseconds(2000);
			worker.run();

			// Get result to trigger possible Exception
			worker.get();

			System.out.println();
			System.out.println("End of multiple file import.\nImported files: " + worker.getItemsDone());

			if (worker.wasErroneous()) {
				System.out.println(ConsoleUtilities.getAnsiColoredText(worker.getResult(), TextColor.Light_red));
			} else if (dbImportDefinitionToExecute.isVerbose()) {
				System.out.println(worker.getResult());
			}

			System.out.println();
		} catch (final ExecutionException e) {
			if (e.getCause() instanceof Exception) {
				throw (Exception) e.getCause();
			} else {
				throw e;
			}
		} catch (final Exception e) {
			throw e;
		}
	}

	private static int connectionTest(final ConnectionTestDefinition connectionTestDefinition) {
		int returnCode = 0;
		int connectionCheckCount = 0;
		int successfulConnectionCount = 0;
		for (int i = 1; i <= connectionTestDefinition.getIterations() || connectionTestDefinition.getIterations() == 0; i++) {
			connectionCheckCount++;
			System.out.println("Connection test " + i + (connectionTestDefinition.getIterations() > 0 ? " / " + connectionTestDefinition.getIterations() : ""));
			Connection testConnection = null;
			try {
				System.out.println(DateUtilities.formatDate(DateUtilities.YYYY_MM_DD_HHMMSS, LocalDateTime.now()) + ": Creating database connection");
				if (connectionTestDefinition.getDbVendor() == DbVendor.Derby || (connectionTestDefinition.getDbVendor() == DbVendor.HSQL && Utilities.isBlank(connectionTestDefinition.getHostnameAndPort())) || connectionTestDefinition.getDbVendor() == DbVendor.SQLite) {
					try {
						testConnection = DbUtilities.createConnection(connectionTestDefinition, false);
					} catch (@SuppressWarnings("unused") final DbNotExistsException e) {
						testConnection = DbUtilities.createNewDatabase(connectionTestDefinition.getDbVendor(), connectionTestDefinition.getDbName());
					}
				} else {
					testConnection = DbUtilities.createConnection(connectionTestDefinition, false);
				}

				System.out.println(DateUtilities.formatDate(DateUtilities.YYYY_MM_DD_HHMMSS, LocalDateTime.now()) + ": Successfully created database connection");

				if (Utilities.isNotBlank(connectionTestDefinition.getCheckStatement())) {
					try (Statement statement = testConnection.createStatement()) {
						String statementString = connectionTestDefinition.getCheckStatement();
						// help.txt and the console menu document the keyword "vendor", "check" is kept for compatibility
						if ("vendor".equalsIgnoreCase(statementString) || "check".equalsIgnoreCase(statementString)) {
							statementString = connectionTestDefinition.getDbVendor().getTestStatement();
						}

						System.out.println(DateUtilities.formatDate(DateUtilities.YYYY_MM_DD_HHMMSS, LocalDateTime.now()) + ": Executing \"" + statementString + "\"");
						try (ResultSet resultSet = statement.executeQuery(statementString)) {
							// do nothing
						}
					}
				}

				successfulConnectionCount++;
			} catch (final SQLException sqle) {
				System.out.println(DateUtilities.formatDate(DateUtilities.YYYY_MM_DD_HHMMSS, LocalDateTime.now()) + ": SQL-Error creating database connection: " + sqle.getMessage() + " (" + sqle.getErrorCode() + " / " + sqle.getSQLState() + ")");
				returnCode = 1;
			} catch (final Exception e) {
				System.out.println(DateUtilities.formatDate(DateUtilities.YYYY_MM_DD_HHMMSS, LocalDateTime.now()) + ": Error creating database connection: " + e.getClass().getSimpleName() + ":" + e.getMessage());
				e.printStackTrace();
				returnCode = 1;
			} finally {
				if (testConnection != null) {
					try {
						System.out.println(DateUtilities.formatDate(DateUtilities.YYYY_MM_DD_HHMMSS, LocalDateTime.now()) + ": Closing database connection");
						if (!testConnection.isClosed()) {
							testConnection.close();
						}
					} catch (final SQLException e) {
						System.out.println(DateUtilities.formatDate(DateUtilities.YYYY_MM_DD_HHMMSS, LocalDateTime.now()) + ": Error closing database connection: " + e.getMessage());
						returnCode = 1;
					}
				}
				if (connectionTestDefinition.getDbVendor() == DbVendor.Derby) {
					try {
						DbUtilities.shutDownDerbyDb(connectionTestDefinition.getDbName());
					} catch (final Exception e) {
						System.err.println(e.getMessage());
					}
				}
			}

			if (connectionTestDefinition.getIterations() == 0) {
				final int successPercentage = successfulConnectionCount * 100 / connectionCheckCount;
				System.out.println("Successful connection checks: " + successfulConnectionCount + " / " + connectionCheckCount + " (" + successPercentage + "%)");
			}

			if ((connectionCheckCount < connectionTestDefinition.getIterations() || connectionTestDefinition.getIterations() == 0) && connectionTestDefinition.getSleepTime() > 0) {
				try {
					System.out.println(DateUtilities.formatDate(DateUtilities.YYYY_MM_DD_HHMMSS, LocalDateTime.now()) + ": Sleeping for " + connectionTestDefinition.getSleepTime() + " seconds");
					Thread.sleep(connectionTestDefinition.getSleepTime() * 1000);
				} catch (@SuppressWarnings("unused") final InterruptedException e) {
					// do nothing
				}
			}
		}

		final int successPercentage = successfulConnectionCount * 100 / connectionCheckCount;
		System.out.println("Successful connection checks: " + successfulConnectionCount + " / " + connectionCheckCount + " (" + successPercentage + "%)");

		return returnCode;
	}

	/**
	 * Signals progress with an unknown total amount. Nothing is shown on the console.
	 */
	@Override
	public void receiveUnlimitedProgressSignal() {
		// Do nothing
	}

	/**
	 * Shows the progress bar of the import on the console (verbose mode only).
	 *
	 * @param start start time of the import
	 * @param itemsToDo amount of items to import
	 * @param itemsDone amount of items imported so far
	 * @param itemsUnitSign unit sign of the amounts (e.g. "B" for bytes), or null for data items
	 */
	@Override
	public void receiveProgressSignal(final LocalDateTime start, final long itemsToDo, final long itemsDone, final String itemsUnitSign) {
		if (isVerbose() && !"*".equals(dbImportDefinitionToExecute.getTableName())) {
			printProgressBar(start, itemsToDo, itemsDone, itemsUnitSign);
		}
	}

	/**
	 * Shows the summary and the result text of the import on the console (verbose mode only).
	 *
	 * @param start start time of the import
	 * @param end end time of the import
	 * @param itemsDone amount of imported items
	 * @param itemsUnitSign unit sign of the amount (e.g. "B" for bytes), or null for data items
	 * @param resultText result text of the import
	 */
	@Override
	public void receiveDoneSignal(final LocalDateTime start, final LocalDateTime end, final long itemsDone, final String itemsUnitSign, final String resultText) {
		if (isVerbose()) {
			int currentTerminalWidth;
			try {
				currentTerminalWidth = ConsoleUtilities.getTerminalSize().getWidth();
			} catch (@SuppressWarnings("unused") final Exception e) {
				currentTerminalWidth = 80;
			}
			if ("*".equals(dbImportDefinitionToExecute.getTableName())) {
				System.out.println(Utilities.rightPad("Imported " + NumberFormat.getNumberInstance(Locale.ENGLISH).format(itemsDone) + " files in " + DateUtilities.getShortHumanReadableTimespan(Duration.between(start, end), false, false), currentTerminalWidth));
			} else {
				if (itemsUnitSign != null) {
					System.out.println(Utilities.rightPad("Imported " + Utilities.getHumanReadableNumber(itemsDone, itemsUnitSign, true, 5, true, Locale.ENGLISH) + " in " + DateUtilities.getShortHumanReadableTimespan(Duration.between(start, end), false, false), currentTerminalWidth));
				} else {
					System.out.println(Utilities.rightPad("Imported " + NumberFormat.getNumberInstance(Locale.ENGLISH).format(itemsDone) + " lines in " + DateUtilities.getShortHumanReadableTimespan(Duration.between(start, end), false, false), currentTerminalWidth));
				}
			}

			if (Utilities.isNotBlank(resultText)) {
				System.out.println("Result: \n" + resultText);
			}

			System.out.println();
			System.out.println();
		}
	}

	/**
	 * Signals the cancellation of the import on the console.
	 *
	 * @return always true
	 */
	@Override
	public boolean cancel() {
		System.out.println("Canceled");
		return true;
	}

	/**
	 * Shows a title change on the console (verbose mode only).
	 *
	 * @param text the new title
	 */
	@Override
	public void changeTitle(final String text) {
		if (isVerbose()) {
			System.out.println(text);
		}
	}

	private static int openConsoleMenu(final DbImportDefinition dbImportDefinition, final BlobImportDefinition blobImportDefinition, final ConnectionTestDefinition connectionTestDefinition) {
		try {
			final ConsoleMenu mainMenu = new ConsoleMenu(APPLICATION_NAME + " (v" + VERSION.toString() + ")");
			final ImportMenu importMenu = new ImportMenu(mainMenu);
			importMenu.setDbImportDefinition(dbImportDefinition);
			final BlobUpdateMenu blobUpdateMenu = new BlobUpdateMenu(mainMenu, importMenu.getDbImportDefinition());
			if (blobImportDefinition != null) {
				blobUpdateMenu.setBlobImportDefinition(blobImportDefinition);
			}
			final ConnectionTestMenu connectionTestMenu = new ConnectionTestMenu(mainMenu, importMenu.getDbImportDefinition());
			connectionTestMenu.setConnectionTestDefinition(connectionTestDefinition);

			@SuppressWarnings("unused")
			final PreferencesMenu preferencesMenu = new PreferencesMenu(mainMenu, importMenu.getDbImportDefinition());

			final CreateTrustStoreMenu createTrustStoreMenu = new CreateTrustStoreMenu(mainMenu, importMenu.getDbImportDefinition());
			createTrustStoreMenu.setConnectionTestDefinition(connectionTestDefinition);

			@SuppressWarnings("unused")
			final ConsoleMenu updateMenu = new UpdateMenu(mainMenu);
			@SuppressWarnings("unused")
			final HelpMenu helpMenu = new HelpMenu(mainMenu);

			final int returnValue = mainMenu.show();

			if (returnValue == -1) {
				// Execute the defined import
				return -1;
			} else if (returnValue == -2) {
				// Connection test
				return -2;
			} else if (returnValue == -3) {
				// Blob import
				return -3;
			} else if (returnValue == -4) {
				// Update application
				return -4;
			} else if (returnValue == -5) {
				// create TrustStore
				return -5;
			} else {
				System.out.println();
				System.out.println("Bye");
				System.out.println();
				System.exit(returnValue);
				return 0;
			}
		} catch (final Exception e) {
			e.printStackTrace();
			System.exit(1);
			return 1;
		}
	}

	/**
	 * Signals sub progress with an unknown total amount. Nothing is shown on the console.
	 */
	@Override
	public void receiveUnlimitedSubProgressSignal() {
		// Do nothing
	}

	/**
	 * Shows the start of the import of a single file on the console (the description in verbose mode only).
	 *
	 * @param itemName name of the imported file
	 * @param description description of the import
	 */
	@Override
	public void receiveItemStartSignal(final String itemName, final String description) {
		System.out.println("Importing " + itemName);
		if (isVerbose()) {
			System.out.println(description);
		}
	}

	/**
	 * Shows the progress bar of the import of a single file on the console (verbose mode only).
	 *
	 * @param itemStart start time of the import of the file
	 * @param subItemToDo amount of items to import from the file
	 * @param subItemDone amount of items imported so far
	 * @param itemsUnitSign unit sign of the amounts (e.g. "B" for bytes), or null for data items
	 */
	@Override
	public void receiveItemProgressSignal(final LocalDateTime itemStart, final long subItemToDo, final long subItemDone, final String itemsUnitSign) {
		if (isVerbose() && subItemToDo > 0) {
			printProgressBar(itemStart, subItemToDo, subItemDone, itemsUnitSign);
		}
	}

	/**
	 * The worker signals are also received while no import runs (e.g. as parent of the application update),
	 * so there may be no import definition at all.
	 */
	private boolean isVerbose() {
		return dbImportDefinitionToExecute != null && dbImportDefinitionToExecute.isVerbose();
	}

	private void printProgressBar(final LocalDateTime itemStart, final long subItemToDo, final long subItemDone, final String itemsUnitSign) {
		if (ConsoleUtilities.getConsoleType() == ConsoleType.ANSI) {
			int currentTerminalWidth;
			try {
				currentTerminalWidth = ConsoleUtilities.getTerminalSize().getWidth();
			} catch (@SuppressWarnings("unused") final Exception e) {
				currentTerminalWidth = 80;
			}

			ConsoleUtilities.saveCurrentCursorPosition();

			if (currentTerminalWidth < previousTerminalWidth) {
				System.out.print("\r" + Utilities.repeat(" ", currentTerminalWidth));
			}
			previousTerminalWidth = currentTerminalWidth;

			ConsoleUtilities.moveCursorToSavedPosition();

			System.out.print(ConsoleUtilities.getConsoleProgressString(currentTerminalWidth - 1, itemStart, subItemToDo, subItemDone, itemsUnitSign));

			ConsoleUtilities.moveCursorToSavedPosition();
		} else if (ConsoleUtilities.getConsoleType() == ConsoleType.TEST) {
			System.out.print(ConsoleUtilities.getConsoleProgressString(80 - 1, itemStart, subItemToDo, subItemDone, itemsUnitSign) + "\n");
		} else {
			System.out.print("\r" + ConsoleUtilities.getConsoleProgressString(80 - 1, itemStart, subItemToDo, subItemDone, itemsUnitSign) + "\r");
		}
	}

	/**
	 * Shows the summary and the result text of the import of a single file on the console (verbose mode only).
	 *
	 * @param itemStart start time of the import of the file
	 * @param itemEnd end time of the import of the file
	 * @param subItemsDone amount of imported items
	 * @param itemsUnitSign unit sign of the amount (e.g. "B" for bytes), or null for data items
	 * @param resultText result text of the import of the file
	 */
	@Override
	public void receiveItemDoneSignal(final LocalDateTime itemStart, final LocalDateTime itemEnd, final long subItemsDone, final String itemsUnitSign, final String resultText) {
		if (isVerbose()) {
			if (subItemsDone > 0) {
				printProgressBar(itemStart, subItemsDone, subItemsDone, itemsUnitSign);
			}
			System.out.println();
			if (itemsUnitSign != null) {
				System.out.println("End (" + Utilities.getHumanReadableNumber(subItemsDone, itemsUnitSign, true, 5, true, Locale.ENGLISH) + " done in " + DateUtilities.getHumanReadableTimespanEnglish(Duration.between(itemStart, itemEnd), true) + ")");
			} else {
				System.out.println("End (" + NumberFormat.getNumberInstance(Locale.ENGLISH).format(subItemsDone) + " data items done in " + DateUtilities.getHumanReadableTimespanEnglish(Duration.between(itemStart, itemEnd), true) + ")");
			}

			if (Utilities.isNotBlank(resultText)) {
				System.out.println("Result: \n" + resultText);
			}

			System.out.println();
		}
	}

	/**
	 * Derives the table name for a multiple file import ("*" as table name) from a file name, by removing the
	 * compression and data format extensions, e.g. "customer.csv.zip" results in "customer".
	 *
	 * @param filename the file name
	 * @return the table name in lower case
	 */
	public static String getImportTablenameFromFilename(final String filename) {
		String tableNameToImport = filename.toLowerCase(Locale.ROOT);
		if (Utilities.endsWithIgnoreCase(tableNameToImport, ".zip")) {
			tableNameToImport = tableNameToImport.substring(0, tableNameToImport.length() - 4);
		} else if (Utilities.endsWithIgnoreCase(tableNameToImport, ".tar.gz")) {
			tableNameToImport = tableNameToImport.substring(0, tableNameToImport.length() - 7);
		} else if (Utilities.endsWithIgnoreCase(tableNameToImport, ".tgz")) {
			tableNameToImport = tableNameToImport.substring(0, tableNameToImport.length() - 4);
		} else if (Utilities.endsWithIgnoreCase(tableNameToImport, ".gz")) {
			tableNameToImport = tableNameToImport.substring(0, tableNameToImport.length() - 3);
		}

		if (Utilities.endsWithIgnoreCase(tableNameToImport, ".csv")) {
			tableNameToImport = tableNameToImport.substring(0, tableNameToImport.length() - 4);
		} else if (Utilities.endsWithIgnoreCase(tableNameToImport, ".del")) {
			tableNameToImport = tableNameToImport.substring(0, tableNameToImport.length() - 4);
		} else if (Utilities.endsWithIgnoreCase(tableNameToImport, ".xls")) {
			tableNameToImport = tableNameToImport.substring(0, tableNameToImport.length() - 4);
		} else if (Utilities.endsWithIgnoreCase(tableNameToImport, ".xlsx")) {
			tableNameToImport = tableNameToImport.substring(0, tableNameToImport.length() - 5);
		} else if (Utilities.endsWithIgnoreCase(tableNameToImport, ".json")) {
			tableNameToImport = tableNameToImport.substring(0, tableNameToImport.length() - 5);
		} else if (Utilities.endsWithIgnoreCase(tableNameToImport, ".jsn")) {
			tableNameToImport = tableNameToImport.substring(0, tableNameToImport.length() - 4);
		} else if (Utilities.endsWithIgnoreCase(tableNameToImport, ".kdbx")) {
			tableNameToImport = tableNameToImport.substring(0, tableNameToImport.length() - 5);
		} else if (Utilities.endsWithIgnoreCase(tableNameToImport, ".ods")) {
			tableNameToImport = tableNameToImport.substring(0, tableNameToImport.length() - 4);
		} else if (Utilities.endsWithIgnoreCase(tableNameToImport, ".sql")) {
			tableNameToImport = tableNameToImport.substring(0, tableNameToImport.length() - 4);
		} else if (Utilities.endsWithIgnoreCase(tableNameToImport, ".vcf")) {
			tableNameToImport = tableNameToImport.substring(0, tableNameToImport.length() - 4);
		} else if (Utilities.endsWithIgnoreCase(tableNameToImport, ".xml")) {
			tableNameToImport = tableNameToImport.substring(0, tableNameToImport.length() - 4);
		} else if (Utilities.endsWithIgnoreCase(tableNameToImport, ".yaml")) {
			tableNameToImport = tableNameToImport.substring(0, tableNameToImport.length() - 5);
		} else if (Utilities.endsWithIgnoreCase(tableNameToImport, ".yml")) {
			tableNameToImport = tableNameToImport.substring(0, tableNameToImport.length() - 4);
		}

		if (tableNameToImport.contains("(") && tableNameToImport.indexOf(")") > tableNameToImport.indexOf("(")) {
			tableNameToImport = tableNameToImport.substring(tableNameToImport.indexOf("(") + 1, tableNameToImport.indexOf(")"));
		}

		return tableNameToImport;
	}
}
