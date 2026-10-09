package de.soderer.dbimport.console;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import de.soderer.dbimport.DbImport;
import de.soderer.utilities.IoUtilities;
import de.soderer.utilities.console.ConsoleMenu;
import de.soderer.utilities.console.ConsoleUtilities;

/**
 * Console menu entry, which prints the help text.
 */
public class HelpMenu extends ConsoleMenu {
	/**
	 * Creates the menu.
	 *
	 * @param parentMenu the parent menu
	 * @throws Exception if the menu cannot be created
	 */
	public HelpMenu(final ConsoleMenu parentMenu) throws Exception {
		super(parentMenu, "Print help text");
	}

	/**
	 * Prints the help text and waits for the user to continue.
	 *
	 * @return 0 to return to the parent menu
	 * @throws Exception if the console input or output fails
	 */
	@Override
	public int show() throws Exception {
		ConsoleUtilities.clearScreen();
		try (InputStream helpInputStream = DbImport.class.getResourceAsStream(DbImport.HELP_RESOURCE_FILE)) {
			System.out.println("DbImport (by Andreas Soderer, mail: dbimport@soderer.de)\n"
					+ "VERSION: " + DbImport.VERSION.toString() + DbImport.getBuildTimeText() + "\n\n"
					+ new String(IoUtilities.toByteArray(helpInputStream), StandardCharsets.UTF_8));
		} catch (@SuppressWarnings("unused") final Exception e) {
			System.out.println("Help info is missing");
		}
		System.exit(0);
		return 0;
	}
}
