package de.soderer.dbimport.console;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import de.soderer.dbimport.ConnectionTestDefinition;
import de.soderer.dbimport.DbImport;
import de.soderer.dbimport.DbImportDefinition;
import de.soderer.utilities.SecureDataStore;
import de.soderer.utilities.Utilities;
import de.soderer.utilities.WrongPasswordException;
import de.soderer.utilities.console.ConsoleMenu;
import de.soderer.utilities.console.ConsoleUtilities;
import de.soderer.utilities.console.PasswordConsoleInput;
import de.soderer.utilities.console.SimpleConsoleInput;

public class PreferencesMenu extends ConsoleMenu {
	private final ConnectionTestDefinition connectionTestDefinition = new ConnectionTestDefinition();
	private DbImportDefinition dbImportDefinitionCache = null;
	private SecureDataStore secureDataStore = null;
	private char[] latestPassword = null;

	public PreferencesMenu(final ConsoleMenu parentMenu, final DbImportDefinition dbImportDefinitionCache) throws Exception {
		super(parentMenu, "Preferences");

		this.dbImportDefinitionCache = dbImportDefinitionCache;
	}

	@Override
	public int show() throws Exception {
		try {
			while (true) {
				connectionTestDefinition.importParameters(dbImportDefinitionCache);

				ConsoleUtilities.clearScreen();
				ConsoleUtilities.printBoxed(DbImport.APPLICATION_NAME + " (v" + DbImport.VERSION.toString() + ")");
				System.out.println();
				System.out.println("Database connection preferences");
				System.out.println();
				printMessages();

				System.out.println("Available preferences:");

				final List<String> availablePreferences = new ArrayList<>();

				if (DbImport.SECURE_PREFERENCES_FILE != null && DbImport.SECURE_PREFERENCES_FILE.exists()) {
					final boolean retry = true;
					while (secureDataStore == null && retry) {
						try {
							secureDataStore = new SecureDataStore();
							secureDataStore.load(DbImport.SECURE_PREFERENCES_FILE, getPassword());
						} catch (@SuppressWarnings("unused") final WrongPasswordException e) {
							secureDataStore = null;

							System.out.println();
							System.out.println("Please enter preferences password (Blank => Cancel)");
							final char[] passwordArray = new PasswordConsoleInput().setPrompt(" > ").readInput();
							if (Utilities.isBlank(passwordArray)) {
								setPassword(null);
								getParentMenu().getMessages().add("Canceled by user");
							} else {
								setPassword(passwordArray);
							}
						}
					}

					if (secureDataStore != null) {
						for (final String entryName : secureDataStore.getEntryNames(DbImportDefinition.class)) {
							availablePreferences.add(entryName);
						}
					}
				}

				if (availablePreferences.size() > 0) {
					for (final String entryName : secureDataStore.getEntryNames(DbImportDefinition.class)) {
						System.out.println("  " + entryName);
					}

					final List<String> autoCompletionStrings = new ArrayList<>(availablePreferences);
					autoCompletionStrings.add("save");
					autoCompletionStrings.add("delete");

					System.out.println();
					System.out.println("Please select existing preference\n or 'save' to store new preference\n or 'delete' to delete a preference ('save' => new preference, 'delete' => delete preference, Blank => Cancel)");
					String choice = new SimpleConsoleInput().setAutoCompletionStrings(autoCompletionStrings).setPrompt(" > ").readInput();
					choice = choice == null ? "" : choice.trim();
					if (Utilities.isBlank(choice)) {
						return 0;
					} else if ("save".equalsIgnoreCase(choice)) {
						System.out.println();
						System.out.println("Please enter new preference name (Blank => Cancel)");
						final String newPreferenceName = new SimpleConsoleInput().setPrompt(" > ").readInput();
						if (Utilities.isBlank(newPreferenceName)) {
							return 0;
						} else {
							storeNewPreference(newPreferenceName, dbImportDefinitionCache);
						}
					} else if ("delete".equalsIgnoreCase(choice)) {
						System.out.println();
						System.out.println("Please enter new preference name to delete (Blank => Cancel)");
						final String deletePreferenceName = new SimpleConsoleInput().setAutoCompletionStrings(new ArrayList<>(availablePreferences)).setPrompt(" > ").readInput();
						if (Utilities.isNotBlank(deletePreferenceName)) {
							secureDataStore.removeEntriesByEntryName(deletePreferenceName);
							secureDataStore.save(DbImport.SECURE_PREFERENCES_FILE, getPassword());
							getMessages().add("Deleted preference: " + deletePreferenceName);
						}
					} else {
						if (availablePreferences.contains(choice)) {
							final DbImportDefinition dbImportDafinitionPreference = (DbImportDefinition) secureDataStore.getEntry(choice);
							dbImportDefinitionCache.importParameters(dbImportDafinitionPreference);
							return 0;
						} else {
							System.out.println();
							getErrors().add("Unknown preference name: " + choice);
						}
					}
				} else {
					System.out.println();
					System.out.println("Please enter new preference name (Blank => Cancel)");
					final String newPreferenceName = new SimpleConsoleInput().setPrompt(" > ").readInput();
					if (Utilities.isBlank(newPreferenceName)) {
						return 0;
					} else {
						storeNewPreference(newPreferenceName, dbImportDefinitionCache);
					}
				}
			}
		} catch (final Exception e) {
			e.printStackTrace();
			System.exit(1);
			return 0;
		}
	}

	private void storeNewPreference(final String newPreferenceName, final DbImportDefinition dbImportDefinitionCache) throws Exception {
		if (secureDataStore == null) {
			secureDataStore = new SecureDataStore();
		}

		if (Utilities.isNotBlank(newPreferenceName)) {
			secureDataStore.addEntry(newPreferenceName, dbImportDefinitionCache);
			if (getPassword() == null) {
				System.out.println();
				System.out.println("Please enter new preferences password (Blank => Cancel)");
				final char[] passwordArray = new PasswordConsoleInput().setPrompt(" > ").readInput();
				setPassword(passwordArray);
			}
			if (getPassword() != null) {
				try {
					secureDataStore.save(DbImport.SECURE_PREFERENCES_FILE, getPassword());
					getMessages().add("Stored preferences");
				} catch (final Exception e) {
					e.printStackTrace();
				}
			}
		}
	}

	public char[] getPassword() {
		if (latestPassword == null) {
			return null;
		} else {
			return Arrays.copyOf(latestPassword, latestPassword.length);
		}
	}

	public void setPassword(final char[] password) {
		latestPassword = password;
	}
}
