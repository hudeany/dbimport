package de.soderer.dbimport.console;

import de.soderer.dbimport.DbImport;
import de.soderer.utilities.console.ConsoleMenu;
import de.soderer.utilities.console.ConsoleUtilities;

/**
 * Console menu entry, which starts the application update.
 */
public class UpdateMenu extends ConsoleMenu {
	/**
	 * Creates the menu.
	 *
	 * @param parentMenu the parent menu
	 * @throws Exception if the menu cannot be created
	 */
	public UpdateMenu(final ConsoleMenu parentMenu) throws Exception {
		super(parentMenu, "Update application");
	}

	/**
	 * Requests the application update.
	 *
	 * @return -4 to start the application update
	 * @throws Exception if the console input or output fails
	 */
	@Override
	public int show() throws Exception {
		ConsoleUtilities.clearScreen();
		ConsoleUtilities.printBoxed(DbImport.APPLICATION_NAME + " (v" + DbImport.VERSION.toString() + ")");
		System.out.println();
		return -4;
	}
}
