package de.soderer.dbimport;

import java.io.File;
import java.sql.Connection;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import de.soderer.utilities.DateUtilities;
import de.soderer.utilities.LangResources;
import de.soderer.utilities.StringUtilities;
import de.soderer.utilities.Utilities;
import de.soderer.utilities.db.DbUtilities;
import de.soderer.utilities.db.data.DbVendor;
import de.soderer.utilities.db.exception.DbNotExistsException;
import de.soderer.utilities.worker.WorkerDual;
import de.soderer.utilities.worker.WorkerParentSimple;

/**
 * Worker that imports several files one after another, each by its own {@link DbImportWorker}.
 *
 * <p>
 * The table name "*" means, that each file is imported into the table named like the file (see
 * {@link DbImport#getImportTablenameFromFilename(String)}). The progress of the single imports is signaled as
 * sub item progress.
 * </p>
 */
public class DbImportMultiWorker extends WorkerDual<Boolean> implements WorkerParentSimple {
	private final DbImportDefinition dbImportDefinition;
	private final List<File> filesToImport;
	private final String tableName;

	private DbImportWorker subWorker;
	private StringBuilder multiImportResult;
	private boolean multiImportHadError;

	/**
	 * Creates the worker.
	 *
	 * @param dbImportDefinition the import definition used for all files
	 * @param filesToImport the files to import
	 * @param tableName the table to import into, or "*" for a table named like each file
	 */
	public DbImportMultiWorker(final DbImportDefinition dbImportDefinition, final List<File> filesToImport, final String tableName) {
		super(null);

		this.dbImportDefinition = dbImportDefinition;
		this.filesToImport = filesToImport;
		this.tableName = tableName;
	}

	/**
	 * Imports all files.
	 *
	 * @return true if the import was not canceled
	 * @throws Exception if the database connection fails or a table is missing
	 */
	@Override
	public Boolean work() throws Exception {
		boolean constraintsWereDeactivated = false;
		boolean triggersWereDeactivated = false;

		try (Connection connection = getDatabaseConnection(dbImportDefinition)) {
			signalUnlimitedProgress();
			itemsToDo = filesToImport.size();

			multiImportResult = new StringBuilder();
			multiImportHadError = false;
			final LocalDateTime startDate = LocalDateTime.now();
			long importedDataSize = 0l;

			multiImportResult.append(LangResources.get("start") + ": " + DateUtilities.formatDate(DateUtilities.YYYY_MM_DD_HHMMSS, startDate) + "\n");
			itemsDone = 0;
			signalProgress();

			if ("*".equals(tableName) && !dbImportDefinition.isCreateTable()) {
				final List<String> notFoundTables = new ArrayList<>();
				final List<String> availableTables = DbUtilities.getAvailableTables(connection, "*").stream().map(x -> x.toLowerCase()).collect(Collectors.toList());
				for (int fileIndex = 0; fileIndex < filesToImport.size(); fileIndex++) {
					final String tableNameToImport = DbImport.getImportTablenameFromFilename(filesToImport.get(fileIndex).getName());
					if (!availableTables.contains(tableNameToImport)) {
						notFoundTables.add(tableNameToImport);
					}
				}

				if (notFoundTables.size() > 0) {
					throw new DbImportException("Import tables not available in database: " + Utilities.join(notFoundTables, ", "));
				}
			}

			if (dbImportDefinition.isDeactivateForeignKeyConstraints()) {
				if (dbImportDefinition.isVerbose()) {
					parent.changeTitle(LangResources.get("deactivateForeignKeyConstraints"));
				}
				constraintsWereDeactivated = true;
				try {
					DbUtilities.setForeignKeyConstraintStatus(dbImportDefinition.getDbVendor(), connection, false);
					if (!connection.getAutoCommit()) {
						connection.commit();
					}
				} catch (final Exception e) {
					throw new DbImportException("Cannot deactivate foreign key constraints: " + e.getMessage());
				}
				if (!connection.getAutoCommit()) {
					connection.commit();
				}
			}

			if (dbImportDefinition.isDeactivateTriggers()) {
				if (dbImportDefinition.isVerbose()) {
					parent.changeTitle(LangResources.get("deactivateTriggers"));
				}
				triggersWereDeactivated = true;
				try {
					DbUtilities.setTriggerStatus(dbImportDefinition.getDbVendor(), connection, false);
					if (!connection.getAutoCommit()) {
						connection.commit();
					}
				} catch (final Exception e) {
					throw new DbImportException("Cannot deactivate triggers: " + e.getMessage());
				}
				if (!connection.getAutoCommit()) {
					connection.commit();
				}
			}

			for (int fileIndex = 0; fileIndex < filesToImport.size(); fileIndex++) {
				if (cancel || (multiImportHadError && dbImportDefinition.isCompleteCommit())) {
					break;
				}

				String tableToImport = tableName;
				final File fileToImport = filesToImport.get(fileIndex);
				if ("*".equals(tableToImport)) {
					tableToImport = DbImport.getImportTablenameFromFilename(fileToImport.getName());
				}

				signalUnlimitedSubProgress();

				subWorker = dbImportDefinition.getConfiguredWorker(this, false, tableToImport, fileToImport.getAbsolutePath());

				// prevent multiple constraint deactivation
				subWorker.setDeactivateForeignKeyConstraints(false);
				subWorker.setDeactivateTriggers(false);

				signalItemStart(tableToImport + " (" + fileToImport.getName() + ") " + (itemsDone + 1) + "/" + itemsToDo, subWorker.getConfigurationLogString());

				subWorker.run();

				importedDataSize += subWorker.getImportedDataAmount();

				final String tableImportShortResult = tableToImport + " (" + fileToImport.getName() + ", " + subWorker.getImportedDataItems() + " data items, " + Utilities.getHumanReadableNumber(subWorker.getImportedDataAmount(), "Byte", false, 5, false, Locale.getDefault()) + ")";
				if (subWorker.getError() != null) {
					multiImportHadError = true;
					multiImportResult.append(tableImportShortResult + ": ERROR (" + subWorker.getError().getMessage().replace("\n", "") + ")\n");
				} else if (subWorker.getNotImportedItems().size() > 0) {
					multiImportHadError = true;
					multiImportResult.append(tableImportShortResult + ": ERROR (Not imported errorneous data items: " + subWorker.getNotImportedItems().size() + ")\n");
				} else {
					multiImportResult.append(tableImportShortResult + ": OK\n");
				}

				subWorker = null;

				itemsDone = fileIndex + 1;
				signalProgress(true);
			}

			itemsDone = itemsToDo;
			signalProgress(true);

			final LocalDateTime endDate = LocalDateTime.now();

			multiImportResult.append("End: " + DateUtilities.formatDate(DateUtilities.YYYY_MM_DD_HHMMSS, endDate) + "\n");
			multiImportResult.append("Time elapsed: " + DateUtilities.getHumanReadableTimespan(Duration.between(startDate, endDate), true) + "\n");
			if ("*".equals(tableName)) {
				multiImportResult.append("Number of imported files: " + Utilities.getHumanReadableNumber(filesToImport.size(), null, false, 5, false, Locale.getDefault()) + "\n");
			}
			multiImportResult.append("Imported dataamount: " + Utilities.getHumanReadableNumber(importedDataSize, "Byte", false, 5, false, Locale.getDefault()) + "\n");

			if (multiImportHadError) {
				return false;
			} else if (cancel) {
				return false;
			} else {
				return true;
			}
		} catch (final Exception e) {
			throw e;
		} finally {
			if (dbImportDefinition.isDeactivateForeignKeyConstraints() && constraintsWereDeactivated) {
				parent.changeTitle(LangResources.get("reactivateForeignKeyConstraints"));
				try (Connection connection = getDatabaseConnection(dbImportDefinition)) {
					DbUtilities.setForeignKeyConstraintStatus(dbImportDefinition.getDbVendor(), connection, true);
					if (!connection.getAutoCommit()) {
						connection.commit();
					}
				} catch (final Exception e) {
					multiImportHadError = true;
					multiImportResult.append("ERROR: Cannot reactivate foreign key constraints: " + e.getMessage());
				}
			}
			if (dbImportDefinition.isDeactivateTriggers() && triggersWereDeactivated) {
				parent.changeTitle(LangResources.get("reactivateTriggers"));
				try (Connection connection = getDatabaseConnection(dbImportDefinition)) {
					DbUtilities.setTriggerStatus(dbImportDefinition.getDbVendor(), connection, true);
					if (!connection.getAutoCommit()) {
						connection.commit();
					}
				} catch (final Exception e) {
					multiImportHadError = true;
					multiImportResult.append("ERROR: Cannot reactivate triggers: " + e.getMessage());
				}
			}
		}
	}

	private static Connection getDatabaseConnection(final DbImportDefinition dbImportDefinitionParam) throws Exception {
		if (dbImportDefinitionParam.getDbVendor() == DbVendor.Derby || (dbImportDefinitionParam.getDbVendor() == DbVendor.HSQL && StringUtilities.isBlank(dbImportDefinitionParam.getHostnameAndPort())) || dbImportDefinitionParam.getDbVendor() == DbVendor.SQLite) {
			try {
				return DbUtilities.createConnection(dbImportDefinitionParam, true);
			} catch (@SuppressWarnings("unused") final DbNotExistsException e) {
				return DbUtilities.createNewDatabase(dbImportDefinitionParam.getDbVendor(), dbImportDefinitionParam.getDbName());
			}
		} else {
			return DbUtilities.createConnection(dbImportDefinitionParam, false);
		}
	}

	/**
	 * Cancels this worker and the currently running single import.
	 *
	 * @return true if the worker was canceled
	 */
	@Override
	public boolean cancel() {
		if (subWorker != null) {
			subWorker.cancel();
		}
		return super.cancel();
	}

	/**
	 * Returns the result text of all imports.
	 *
	 * @return the result text
	 */
	public String getResult() {
		return multiImportResult.toString();
	}

	/**
	 * Returns whether at least one of the imports failed.
	 *
	 * @return true if at least one of the imports failed
	 */
	public boolean wasErroneous() {
		return multiImportHadError;
	}

	/**
	 * Signals the unlimited progress of the current single import as sub item progress.
	 */
	@Override
	public void receiveUnlimitedProgressSignal() {
		signalUnlimitedSubProgress();
	}

	/**
	 * Signals the progress of the current single import as sub item progress.
	 *
	 * @param start start time of the single import
	 * @param itemsToDoParameter amount of items to import
	 * @param itemsDoneParameter amount of items imported so far
	 * @param itemsUnitSignParameter unit sign of the amounts (e.g. "B" for bytes), or null for data items
	 */
	@Override
	public void receiveProgressSignal(final LocalDateTime start, final long itemsToDoParameter, final long itemsDoneParameter, final String itemsUnitSignParameter) {
		// Progress of subWorker must be display of subItem progress
		subItemsToDo = itemsToDoParameter;
		subItemsUnitSign = itemsUnitSignParameter;
		subItemsDone = itemsDoneParameter;
		signalItemProgress();
	}

	/**
	 * Signals the end of the current single import as end of the current item.
	 *
	 * @param start start time of the single import
	 * @param end end time of the single import
	 * @param itemsDoneParameter amount of imported items
	 * @param itemsUnitSignParameter unit sign of the amount (e.g. "B" for bytes), or null for data items
	 * @param resultText result text of the single import
	 */
	@Override
	public void receiveDoneSignal(final LocalDateTime start, final LocalDateTime end, final long itemsDoneParameter, final String itemsUnitSignParameter, final String resultText) {
		// Progress of subWorker must be display of subItem progress
		subItemsDone = itemsDoneParameter;
		signalItemProgress();
		signalItemDone(resultText);
	}

	/**
	 * Passes a title change on to the parent.
	 *
	 * @param text the new title
	 */
	@Override
	public void changeTitle(final String text) {
		if (parent != null) {
			parent.changeTitle(text);
		}
	}
}
