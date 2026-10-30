package de.soderer.dbimport;

import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.Frame;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.file.InvalidPathException;
import java.nio.file.Paths;

import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JFileChooser;
import javax.swing.JPanel;
import javax.swing.JTextField;
import javax.swing.event.DocumentEvent;
import javax.swing.event.DocumentListener;
import javax.swing.filechooser.FileNameExtensionFilter;

import de.soderer.swing.modern.ButtonStyle;
import de.soderer.swing.modern.DialogStyle;
import de.soderer.swing.modern.IconKind;
import de.soderer.swing.modern.ModalDialog;
import de.soderer.swing.modern.ModernUi;
import de.soderer.swing.modern.QuestionDialog;
import de.soderer.utilities.LangResources;
import de.soderer.utilities.StringUtilities;
import de.soderer.utilities.db.SqlDdlMergeGenerator;
import de.soderer.utilities.db.SqlDdlMigrationGenerator;

/**
 * Dialog to generate a SQL migration or merge script from two SQL DDL files, which describe the source
 * and the destination database structure.
 *
 * @serial exclude
 */
public class SqlDdlMigrationDialog extends ModalDialog<Boolean> {
	private static final long serialVersionUID = -5639244545181271366L;

	/** Width of the path fields */
	private static final int FIELD_WIDTH = 360;

	private final JTextField sourceFileField;
	private final JTextField destinationFileField;
	private final JTextField outputFileField;

	private final JCheckBox sortBySchemaCheckBox;
	private final JCheckBox sortByTableCheckBox;
	private final JCheckBox sortByColumnCheckBox;

	private final JButton generateMigrationButton;
	private final JButton generateMergeButton;

	/**
	 * Creates the dialog.
	 *
	 * @param parent the parent window
	 */
	public SqlDdlMigrationDialog(final Frame parent) {
		super(parent, DbImport.APPLICATION_NAME + " - SQL DDL Migration / Merge", IconKind.FILE);

		setResizable(false);

		final JPanel formPanel = new JPanel(new GridBagLayout());
		formPanel.setOpaque(false);

		sourceFileField = createFileField(LangResources.get("sourceFile_help"));
		addFileRow(formPanel, 0, LangResources.get("sourceFile"), sourceFileField, event -> {
			final File selected = chooseSqlFile(sourceFileField.getText(), LangResources.get("sourceFile"));
			if (selected != null) {
				sourceFileField.setText(selected.getAbsolutePath());
				checkButtonStatus();
			}
		});

		destinationFileField = createFileField(LangResources.get("destinationFile_help"));
		addFileRow(formPanel, 1, LangResources.get("destinationFile"), destinationFileField, event -> {
			final File selected = chooseSqlFile(destinationFileField.getText(), LangResources.get("destinationFile"));
			if (selected != null) {
				destinationFileField.setText(selected.getAbsolutePath());
				checkButtonStatus();
			}
		});

		outputFileField = createFileField(LangResources.get("outputFile_help"));
		addFileRow(formPanel, 2, LangResources.get("outputFile"), outputFileField, event -> {
			final File selected = saveSqlFile(outputFileField.getText(), LangResources.get("outputFile"));
			if (selected != null) {
				outputFileField.setText(selected.getAbsolutePath());
				checkButtonStatus();
			}
		});

		final JPanel sortPanel = new JPanel(new FlowLayout(FlowLayout.LEFT, 0, 0));
		sortPanel.setOpaque(false);

		sortBySchemaCheckBox = new JCheckBox(LangResources.get("sortBySchema"));
		sortBySchemaCheckBox.setToolTipText(LangResources.get("sortBySchema_help"));
		sortBySchemaCheckBox.setOpaque(false);
		sortPanel.add(sortBySchemaCheckBox);

		sortByTableCheckBox = new JCheckBox(LangResources.get("sortByTable"));
		sortByTableCheckBox.setToolTipText(LangResources.get("sortByTable_help"));
		sortByTableCheckBox.setOpaque(false);
		sortPanel.add(sortByTableCheckBox);

		sortByColumnCheckBox = new JCheckBox(LangResources.get("sortByColumn"));
		sortByColumnCheckBox.setToolTipText(LangResources.get("sortByColumn_help"));
		sortByColumnCheckBox.setOpaque(false);
		sortPanel.add(sortByColumnCheckBox);

		final GridBagConstraints sortConstraints = new GridBagConstraints();
		sortConstraints.gridx = 1;
		sortConstraints.gridy = 3;
		sortConstraints.gridwidth = 2;
		sortConstraints.anchor = GridBagConstraints.WEST;
		sortConstraints.insets = new Insets(8, 0, 0, 0);
		formPanel.add(sortPanel, sortConstraints);

		getContentArea().add(formPanel);

		generateMigrationButton = addButton(LangResources.get("generateMigration"), null, ButtonStyle.PRIMARY, event -> runMigration(this));
		generateMigrationButton.setEnabled(false);
		generateMergeButton = addButton(LangResources.get("generateMerge"), null, ButtonStyle.SECONDARY, event -> runMerge(this));
		generateMergeButton.setEnabled(false);
		addButton(LangResources.get("cancel"), null, ButtonStyle.SECONDARY, event -> dispose());

		pack();
		setLocationRelativeTo(parent);

		checkButtonStatus();
	}

	/**
	 * Creates a styled path field that re-checks the buttons on every change.
	 */
	private JTextField createFileField(final String toolTipText) {
		final JTextField field = new JTextField();
		ModernUi.styleTextField(field, false);
		field.setPreferredSize(new Dimension(FIELD_WIDTH, field.getPreferredSize().height));
		field.setToolTipText(toolTipText);
		field.getDocument().addDocumentListener(new DocumentListener() {
			@Override
			public void insertUpdate(final DocumentEvent e) {
				checkButtonStatus();
			}

			@Override
			public void removeUpdate(final DocumentEvent e) {
				checkButtonStatus();
			}

			@Override
			public void changedUpdate(final DocumentEvent e) {
				checkButtonStatus();
			}
		});
		return field;
	}

	/**
	 * Adds a row with label, path field and browse button to the form.
	 */
	private static void addFileRow(final JPanel formPanel, final int row, final String labelText, final JTextField field, final java.awt.event.ActionListener browseAction) {
		final GridBagConstraints labelConstraints = new GridBagConstraints();
		labelConstraints.gridx = 0;
		labelConstraints.gridy = row;
		labelConstraints.anchor = GridBagConstraints.WEST;
		labelConstraints.insets = new Insets(3, 0, 3, 12);
		formPanel.add(ModernUi.createMutedLabel(labelText + ":"), labelConstraints);

		final GridBagConstraints fieldConstraints = new GridBagConstraints();
		fieldConstraints.gridx = 1;
		fieldConstraints.gridy = row;
		fieldConstraints.weightx = 1;
		fieldConstraints.fill = GridBagConstraints.HORIZONTAL;
		fieldConstraints.insets = new Insets(3, 0, 3, 6);
		formPanel.add(field, fieldConstraints);

		final JButton browseButton = new ModernUi.Button(null, IconKind.FOLDER, ButtonStyle.SECONDARY);
		browseButton.setPreferredSize(new Dimension(30, 30));
		browseButton.setToolTipText(labelText);
		browseButton.addActionListener(browseAction);
		final GridBagConstraints buttonConstraints = new GridBagConstraints();
		buttonConstraints.gridx = 2;
		buttonConstraints.gridy = row;
		buttonConstraints.insets = new Insets(3, 0, 3, 0);
		formPanel.add(browseButton, buttonConstraints);
	}

	private void checkButtonStatus() {
		final boolean sourceOk = isExistingFile(sourceFileField.getText());
		final boolean destinationOk = isExistingFile(destinationFileField.getText());
		final boolean outputOk = StringUtilities.isNotBlank(outputFileField.getText())
				&& isValidPath(outputFileField.getText())
				&& !isSameFile(outputFileField.getText(), sourceFileField.getText())
				&& !isSameFile(outputFileField.getText(), destinationFileField.getText());
		sourceFileField.setBackground(sourceOk ? ModernUi.CARD : ModernUi.DANGER_LIGHT);
		destinationFileField.setBackground(destinationOk ? ModernUi.CARD : ModernUi.DANGER_LIGHT);
		outputFileField.setBackground(outputOk ? ModernUi.CARD : ModernUi.DANGER_LIGHT);
		final boolean allOk = sourceOk && destinationOk && outputOk;
		generateMigrationButton.setEnabled(allOk);
		generateMergeButton.setEnabled(allOk);
	}

	private static boolean isExistingFile(final String path) {
		if (StringUtilities.isBlank(path)) {
			return false;
		}
		final File f = new File(path.trim());
		return f.exists() && f.isFile();
	}

	/**
	 * Compares two file paths after trimming and normalization (the files are opened with trimmed paths).
	 */
	private static boolean isSameFile(final String path1, final String path2) {
		if (StringUtilities.isBlank(path1) || StringUtilities.isBlank(path2)) {
			return false;
		}
		try {
			return Paths.get(path1.trim()).toAbsolutePath().normalize().equals(Paths.get(path2.trim()).toAbsolutePath().normalize());
		} catch (@SuppressWarnings("unused") final InvalidPathException e) {
			return path1.trim().equals(path2.trim());
		}
	}

	private static boolean isValidPath(final String path) {
		if (path == null || path.isBlank()) {
			return false;
		}
		try {
			Paths.get(path);
			return true;
		} catch (@SuppressWarnings("unused") final InvalidPathException e) {
			return false;
		}
	}

	private void runMigration(final SqlDdlMigrationDialog dialog) {
		final File sourceFile      = new File(sourceFileField.getText().trim());
		final File destinationFile = new File(destinationFileField.getText().trim());
		final File outputFile      = new File(outputFileField.getText().trim());

		if (!sourceFile.exists()) {
			new QuestionDialog(dialog, DbImport.APPLICATION_NAME + " ERROR",
					"ERROR:\nSource file does not exist:\n" + sourceFile.getAbsolutePath())
							.withDialogStyle(DialogStyle.ERROR).open();
			return;
		}
		if (!destinationFile.exists()) {
			new QuestionDialog(dialog, DbImport.APPLICATION_NAME + " ERROR",
					"ERROR:\nDestination file does not exist:\n" + destinationFile.getAbsolutePath())
							.withDialogStyle(DialogStyle.ERROR).open();
			return;
		}
		if (outputFile.getParentFile() != null && !outputFile.getParentFile().exists()) {
			new QuestionDialog(dialog, DbImport.APPLICATION_NAME + " ERROR",
					"ERROR:\nOutput directory does not exist:\n" + outputFile.getParent())
							.withDialogStyle(DialogStyle.ERROR).open();
			return;
		}

		try (final FileInputStream srcStream = new FileInputStream(sourceFile);
				final FileInputStream dstStream = new FileInputStream(destinationFile);
				final FileOutputStream outStream = new FileOutputStream(outputFile)) {

			SqlDdlMigrationGenerator.diff(srcStream, dstStream, outStream,
					sortBySchemaCheckBox.isSelected(),
					sortByTableCheckBox.isSelected(),
					sortByColumnCheckBox.isSelected());

			new QuestionDialog(dialog, DbImport.APPLICATION_NAME + " OK",
					"Migration script successfully written to:\n" + outputFile.getAbsolutePath())
							.withDialogStyle(DialogStyle.INFORMATION).withBackgroundColor(ModernUi.SUCCESS_LIGHT).open();

			dispose();

		} catch (final Exception e) {
			new QuestionDialog(dialog, DbImport.APPLICATION_NAME + " ERROR",
					"ERROR:\n" + e.getClass().getSimpleName() + ":\n" + e.getMessage())
							.withDialogStyle(DialogStyle.ERROR).open();
		}
	}

	private void runMerge(final SqlDdlMigrationDialog dialog) {
		final File sourceFile      = new File(sourceFileField.getText().trim());
		final File destinationFile = new File(destinationFileField.getText().trim());
		final File outputFile      = new File(outputFileField.getText().trim());

		if (!sourceFile.exists()) {
			new QuestionDialog(dialog, DbImport.APPLICATION_NAME + " ERROR",
					"ERROR:\nSource file does not exist:\n" + sourceFile.getAbsolutePath())
							.withDialogStyle(DialogStyle.ERROR).open();
			return;
		}
		if (!destinationFile.exists()) {
			new QuestionDialog(dialog, DbImport.APPLICATION_NAME + " ERROR",
					"ERROR:\nDestination file does not exist:\n" + destinationFile.getAbsolutePath())
							.withDialogStyle(DialogStyle.ERROR).open();
			return;
		}
		if (outputFile.getParentFile() != null && !outputFile.getParentFile().exists()) {
			new QuestionDialog(dialog, DbImport.APPLICATION_NAME + " ERROR",
					"ERROR:\nOutput directory does not exist:\n" + outputFile.getParent())
							.withDialogStyle(DialogStyle.ERROR).open();
			return;
		}

		try (final FileInputStream srcStream = new FileInputStream(sourceFile);
				final FileInputStream dstStream = new FileInputStream(destinationFile);
				final FileOutputStream outStream = new FileOutputStream(outputFile)) {

			SqlDdlMergeGenerator.merge(srcStream, dstStream, outStream,
					sortBySchemaCheckBox.isSelected(),
					sortByTableCheckBox.isSelected(),
					sortByColumnCheckBox.isSelected());

			new QuestionDialog(dialog, DbImport.APPLICATION_NAME + " OK",
					"Merge DDL successfully written to:\n" + outputFile.getAbsolutePath())
							.withDialogStyle(DialogStyle.INFORMATION).withBackgroundColor(ModernUi.SUCCESS_LIGHT).open();

			dispose();

		} catch (final Exception e) {
			new QuestionDialog(dialog, DbImport.APPLICATION_NAME + " ERROR",
					"ERROR:\n" + e.getClass().getSimpleName() + ":\n" + e.getMessage())
							.withDialogStyle(DialogStyle.ERROR).open();
		}
	}

	private File chooseSqlFile(final String currentPath, final String title) {
		final JFileChooser chooser = buildChooser(currentPath, title);
		if (JFileChooser.APPROVE_OPTION == chooser.showOpenDialog(this)) {
			return chooser.getSelectedFile();
		}
		return null;
	}

	private File saveSqlFile(final String currentPath, final String title) {
		final JFileChooser chooser = buildChooser(currentPath, title);
		if (JFileChooser.APPROVE_OPTION == chooser.showSaveDialog(this)) {
			File file = chooser.getSelectedFile();
			if (!file.getName().toLowerCase().endsWith(".sql")) {
				file = new File(file.getAbsolutePath() + ".sql");
			}
			return file;
		}
		return null;
	}

	private JFileChooser buildChooser(final String currentPath, final String title) {
		String basePath = System.getProperty("user.home");
		if (StringUtilities.isNotBlank(currentPath)) {
			final File current = new File(currentPath.trim());
			if (current.getParentFile() != null && current.getParentFile().exists()) {
				basePath = current.getParent();
			}
		}
		final JFileChooser chooser = new JFileChooser(basePath);
		chooser.setDialogTitle(DbImport.APPLICATION_NAME + " - " + title);
		chooser.setFileFilter(new FileNameExtensionFilter("SQL files (*.sql)", "sql"));
		chooser.setAcceptAllFileFilterUsed(true);
		return chooser;
	}
}