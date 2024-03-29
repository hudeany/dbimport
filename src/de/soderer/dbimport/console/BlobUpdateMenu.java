package de.soderer.dbimport.console;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import de.soderer.dbimport.BlobImportDefinition;
import de.soderer.dbimport.DbImport;
import de.soderer.utilities.FileUtilities;
import de.soderer.utilities.Utilities;
import de.soderer.utilities.console.ConsoleMenu;
import de.soderer.utilities.console.ConsoleUtilities;
import de.soderer.utilities.console.ConsoleUtilities.TextColor;
import de.soderer.utilities.console.PasswordConsoleInput;
import de.soderer.utilities.console.SimpleConsoleInput;
import de.soderer.utilities.db.DbDefinition;
import de.soderer.utilities.db.DbUtilities.DbVendor;

public class BlobUpdateMenu extends ConsoleMenu {
	private BlobImportDefinition blobImportDefinition = new BlobImportDefinition();
	private DbDefinition dbDefinitionCache = null;

	public BlobImportDefinition getBlobImportDefinition() {
		return blobImportDefinition;
	}

	public void setBlobImportDefinition(final BlobImportDefinition blobImportDefinition) {
		this.blobImportDefinition = blobImportDefinition;
	}

	public BlobUpdateMenu(final ConsoleMenu parentMenu, final DbDefinition dbDefinitionCache) throws Exception {
		super(parentMenu, "Blob update");

		this.dbDefinitionCache = dbDefinitionCache;
	}

	@Override
	public int show() throws Exception {
		try {
			blobImportDefinition.importParameters(dbDefinitionCache);

			ConsoleUtilities.clearScreen();
			ConsoleUtilities.printBoxed(DbImport.APPLICATION_NAME + " (v" + DbImport.VERSION.toString() + ")");
			System.out.println();
			System.out.println("Blob update");
			System.out.println();
			printMessages();

			while (true) {
				while (blobImportDefinition.getDbVendor() == null) {
					try {
						final String dbVendorString = askForSelection("Please choose database vendor", Stream.of(DbVendor.values()).map(Enum::name).collect(Collectors.toList()));
						if (dbVendorString == null) {
							getParentMenu().getMessages().add("Canceled by user");
							return 0;
						} else {
							blobImportDefinition.setDbVendor(DbVendor.getDbVendorByName(dbVendorString));
						}
					} catch (final Exception e) {
						System.out.println(ConsoleUtilities.getAnsiColoredText(e.getMessage(), TextColor.Light_red));
					}
				}

				if (Utilities.isBlank(blobImportDefinition.getHostnameAndPort()) && blobImportDefinition.getDbVendor() != DbVendor.SQLite && blobImportDefinition.getDbVendor() != DbVendor.HSQL && blobImportDefinition.getDbVendor() != DbVendor.Derby) {
					System.out.println();
					System.out.println("Please enter database hostname and optional port separated by ':' (No port uses database vendors default port, Blank => Cancel)");
					String choice = new SimpleConsoleInput().setPrompt(" > ").readInput();
					choice = choice == null ? "" : choice.trim();
					if (Utilities.isBlank(choice)) {
						getParentMenu().getMessages().add("Canceled by user");
						return 0;
					} else {
						blobImportDefinition.setHostnameAndPort(choice);
					}
				}

				if (blobImportDefinition.getDbVendor() == DbVendor.SQLite || blobImportDefinition.getDbVendor() == DbVendor.Derby) {
					while (Utilities.isBlank(blobImportDefinition.getDbName())) {
						System.out.println();
						System.out.println("Please enter database filepath (Blank => Cancel)");
						String choice = new SimpleConsoleInput().setPrompt(" > ").readInput();
						choice = choice == null ? "" : choice.trim();
						if (Utilities.isBlank(choice)) {
							getParentMenu().getMessages().add("Canceled by user");
							return 0;
						} else if (!FileUtilities.isValidFilePath(choice)) {
							System.out.println(ConsoleUtilities.getAnsiColoredText("Not a valid filepath", TextColor.Light_red));
						} else if (!new File(choice).exists()) {
							System.out.println(ConsoleUtilities.getAnsiColoredText("Filepath does not exist", TextColor.Light_red));
						} else {
							blobImportDefinition.setDbName(choice);
						}
					}
				} else {
					while (Utilities.isBlank(blobImportDefinition.getDbName())) {
						System.out.println();
						System.out.println("Please enter database name (Blank => Cancel)");
						String choice = new SimpleConsoleInput().setPrompt(" > ").readInput();
						choice = choice == null ? "" : choice.trim();
						if (Utilities.isBlank(choice)) {
							getParentMenu().getMessages().add("Canceled by user");
							return 0;
						} else {
							blobImportDefinition.setDbName(choice);
						}
					}
				}

				if (Utilities.isBlank(blobImportDefinition.getUsername()) && blobImportDefinition.getDbVendor() != DbVendor.SQLite && blobImportDefinition.getDbVendor() != DbVendor.Derby) {
					System.out.println();
					System.out.println("Please enter database username (Blank => Cancel)");
					String choice = new SimpleConsoleInput().setPrompt(" > ").readInput();
					choice = choice == null ? "" : choice.trim();
					if (Utilities.isBlank(choice)) {
						if (blobImportDefinition.getDbVendor() == DbVendor.Cassandra) {
							blobImportDefinition.setUsername(null);
						} else {
							getParentMenu().getMessages().add("Canceled by user");
							return 0;
						}
					} else {
						blobImportDefinition.setUsername(choice);
					}
				}

				if (Utilities.isBlank(blobImportDefinition.getPassword()) && blobImportDefinition.getDbVendor() != DbVendor.SQLite && blobImportDefinition.getDbVendor() != DbVendor.HSQL && blobImportDefinition.getDbVendor() != DbVendor.Derby && (blobImportDefinition.getDbVendor() != DbVendor.Cassandra || blobImportDefinition.getUsername() != null)) {
					System.out.println();
					System.out.println("Please enter database password (Blank => Cancel)");
					final char[] passwordArray = new PasswordConsoleInput().setPrompt(" > ").readInput();
					if (Utilities.isBlank(passwordArray)) {
						getParentMenu().getMessages().add("Canceled by user");
						return 0;
					}
					blobImportDefinition.setPassword(passwordArray);
				}

				while (Utilities.isBlank(blobImportDefinition.getBlobImportStatement())) {
					System.out.println();
					System.out.println("Please enter blob import statement (Placeholder for filedata is '?' like in prepared statements, Blank => Cancel)");
					String choice = new SimpleConsoleInput().setPrompt(" > ").readInput();
					choice = choice == null ? "" : choice.trim();
					if (Utilities.isBlank(choice)) {
						getParentMenu().getMessages().add("Canceled by user");
						return 0;
					} else {
						blobImportDefinition.setBlobImportStatement(choice);
					}
				}

				while (Utilities.isBlank(blobImportDefinition.getImportFilePath())) {
					System.out.println();
					System.out.println("Please enter blob data file path (Blank => Cancel)");
					String choice = new SimpleConsoleInput().setPrompt(" > ").readInput();
					choice = choice == null ? "" : choice.trim();
					if (Utilities.isBlank(choice)) {
						getParentMenu().getMessages().add("Canceled by user");
						return 0;
					} else {
						blobImportDefinition.setImportFilePath(choice);
					}
				}

				ConsoleUtilities.clearScreen();

				ConsoleUtilities.printBoxed(DbImport.APPLICATION_NAME + " (v" + DbImport.VERSION.toString() + ")");

				System.out.println();

				System.out.println("Change parameters or start blob update");
				System.out.println();
				printMessages();

				final int bulletSize = 19;
				final int nameSize = 41;

				final List<String> autoCompletionStrings = new ArrayList<>();
				autoCompletionStrings.add("");

				System.out.println("  " + Utilities.rightPad("DB vendor:", bulletSize) + " " + blobImportDefinition.getDbVendor().name());
				if (blobImportDefinition.getDbVendor() != DbVendor.SQLite && blobImportDefinition.getDbVendor() != DbVendor.HSQL && blobImportDefinition.getDbVendor() != DbVendor.Derby) {
					System.out.println("  " + Utilities.rightPad("DB hostname:", bulletSize) + " " + blobImportDefinition.getHostnameAndPort());
				}
				if (blobImportDefinition.getDbVendor() == DbVendor.SQLite || blobImportDefinition.getDbVendor() == DbVendor.Derby) {
					System.out.println("  " + Utilities.rightPad("DB filepath:", bulletSize) + " " + blobImportDefinition.getDbName());
				} else {
					System.out.println("  " + Utilities.rightPad("DB name:", bulletSize) + " " + blobImportDefinition.getDbName());
				}
				if (blobImportDefinition.getDbVendor() != DbVendor.SQLite && blobImportDefinition.getDbVendor() != DbVendor.Derby) {
					System.out.println("  " + Utilities.rightPad("DB username:", bulletSize) + " " + (blobImportDefinition.getUsername() == null ? "<empty>" : blobImportDefinition.getUsername()));
				}
				System.out.println("  " + Utilities.rightPad("DB password:", bulletSize) + " " + (blobImportDefinition.getPassword() == null ? "<empty>" : "***"));

				if (blobImportDefinition.getDbVendor() == DbVendor.Oracle || blobImportDefinition.getDbVendor() == DbVendor.MySQL || blobImportDefinition.getDbVendor() == DbVendor.MariaDB) {
					System.out.println("  " + Utilities.rightPad("Secure connection:", bulletSize) + " " + (blobImportDefinition.isSecureConnection() ? "yes" : "no"));
					System.out.println("  " + Utilities.rightPad("TrustStore filepath:", bulletSize) + " " + (blobImportDefinition.getTrustStoreFile() == null ? "<none>" : blobImportDefinition.getTrustStoreFile().getAbsolutePath()));
					System.out.println("  " + Utilities.rightPad("TrustStore password:", bulletSize) + " " + (blobImportDefinition.getTrustStorePassword() == null ? "<empty>" : "***"));
				}

				System.out.println();
				System.out.println("  " + Utilities.rightPad("reset)", bulletSize) + " " + "Reset basic database parameters");
				autoCompletionStrings.add("reset");
				System.out.println();

				if (blobImportDefinition.getDbVendor() == DbVendor.Oracle || blobImportDefinition.getDbVendor() == DbVendor.MySQL || blobImportDefinition.getDbVendor() == DbVendor.MariaDB) {
					System.out.println("  " + Utilities.rightPad("secure)", bulletSize) + " " + "Change setting for secure connection via SSL/TLS");
					autoCompletionStrings.add("secure");
					System.out.println("  " + Utilities.rightPad("truststore)", bulletSize) + " " + "Define TrustStore filepath (Optional)");
					autoCompletionStrings.add("truststore");
					System.out.println("  " + Utilities.rightPad("truststorepassword)", bulletSize) + " " + "Define TrustStore password");
					autoCompletionStrings.add("truststorepassword");
					System.out.println();
				}

				System.out.println();
				System.out.println("  " + Utilities.rightPad("updatesql)", bulletSize) + " " + Utilities.rightPad("Blob import statement:", nameSize) + blobImportDefinition.getBlobImportStatement());
				autoCompletionStrings.add("updatesql");
				System.out.println("  " + Utilities.rightPad("blobfile)", bulletSize) + " " + Utilities.rightPad("Import blob file path:", nameSize) + blobImportDefinition.getImportFilePath());
				autoCompletionStrings.add("blobfile");
				System.out.println();

				System.out.println();
				System.out.println("  " + Utilities.rightPad("params)", bulletSize) + " " + "Print parameters for later use (Includes passwords)");
				autoCompletionStrings.add("params");
				System.out.println("  " + Utilities.rightPad("start)", bulletSize) + " " + "Start blob update");
				autoCompletionStrings.add("start");
				System.out.println("  " + Utilities.rightPad("cancel)", bulletSize) + " " + "Cancel");
				autoCompletionStrings.add("cancel");

				String choice = new SimpleConsoleInput().setAutoCompletionStrings(autoCompletionStrings).setPrompt(" > ").readInput();
				choice = choice == null ? "" : choice.trim();
				if (Utilities.isBlank(choice)) {
					if (dbDefinitionCache != null) {
						dbDefinitionCache.importParameters(blobImportDefinition);
					}

					getParentMenu().getMessages().add("Canceled by user");
					return 0;
				} else if ("reset".equalsIgnoreCase(choice)) {
					blobImportDefinition.setDbVendor((DbVendor) null);
					blobImportDefinition.setHostnameAndPort(null);
					blobImportDefinition.setUsername(null);
					blobImportDefinition.setDbName(null);
					blobImportDefinition.setDbName(null);
					blobImportDefinition.setBlobImportStatement(null);
					blobImportDefinition.setPassword(null);
					blobImportDefinition.setSecureConnection(false);
					blobImportDefinition.setTrustStoreFile(null);
					blobImportDefinition.setTrustStorePassword(null);
				} else if ("secure".equalsIgnoreCase(choice)) {
					blobImportDefinition.setSecureConnection(blobImportDefinition.isSecureConnection());
				} else if ("truststore".equalsIgnoreCase(choice)) {
					System.out.println();
					System.out.println("Please enter database TrustStore filepath (Blank => None)");
					String choiceTruststore = new SimpleConsoleInput().setPrompt(" > ").readInput();
					choiceTruststore = choiceTruststore == null ? "" : choiceTruststore.trim();
					if (Utilities.isBlank(choiceTruststore)) {
						blobImportDefinition.setTrustStoreFile(null);
					} else if (!FileUtilities.isValidFilePath(choiceTruststore)) {
						getErrors().add("Not a valid filepath");
					} else if (!new File(choiceTruststore).exists()) {
						getErrors().add("Filepath does not exist");
					} else {
						blobImportDefinition.setTrustStoreFile(new File(choiceTruststore));
					}
				} else if ("truststorepassword".equalsIgnoreCase(choice)) {
					System.out.println();
					System.out.println("Please enter TrustStore password (Blank => Empty)");
					final char[] passwordArray = new PasswordConsoleInput().setPrompt(" > ").readInput();
					blobImportDefinition.setTrustStorePassword(Utilities.isNotEmpty(passwordArray) ? passwordArray : null);
				} else if ("params".equalsIgnoreCase(choice)) {
					getParentMenu().getMessages().add("Parameters: " + blobImportDefinition.toParamsString());
					return 0;
				} else if ("start".equalsIgnoreCase(choice)) {
					return -2;
				} else {
					System.out.println(ConsoleUtilities.getAnsiColoredText("Invalid selection: " + choice, TextColor.Light_red));
				}
			}
		} catch (final Exception e) {
			e.printStackTrace();
			System.exit(1);
			return 0;
		}
	}
}
