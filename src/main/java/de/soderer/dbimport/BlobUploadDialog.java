package de.soderer.dbimport;

import java.awt.Cursor;
import java.awt.Dimension;
import java.awt.Frame;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.io.File;
import java.util.concurrent.ExecutionException;

import javax.swing.JComponent;
import javax.swing.JFileChooser;
import javax.swing.JPanel;
import javax.swing.JTextField;
import javax.swing.SwingWorker;
import javax.swing.event.DocumentEvent;
import javax.swing.event.DocumentListener;

import de.soderer.swing.modern.Button;
import de.soderer.swing.modern.ButtonStyle;
import de.soderer.swing.modern.DialogStyle;
import de.soderer.swing.modern.IconKind;
import de.soderer.swing.modern.ModalDialog;
import de.soderer.swing.modern.ModernUi;
import de.soderer.swing.modern.QuestionDialog;
import de.soderer.utilities.LangResources;
import de.soderer.utilities.StringUtilities;
import de.soderer.utilities.db.DbUtilities;
import de.soderer.utilities.db.data.DbConnectionDefinition;

/**
 * Dialog to import a single file into a blob or clob column with an update statement, which contains a "?" as
 * placeholder for the file data. The database connection parameters are taken from the main window.
 *
 * @serial exclude
 */
public class BlobUploadDialog extends ModalDialog<Boolean> {
	private static final long serialVersionUID = 4183513276504186457L;

	/** Width of the input fields */
	private static final int FIELD_WIDTH = 420;

	/** Last used values, kept for the next opening of the dialog in the same application session */
	private static String lastUpdateStatement = null;
	private static String lastFilePath = null;

	private final transient DbConnectionDefinition connectionDefinition;

	private final JTextField updateStatementField;
	private final JTextField filePathField;

	private final Button uploadButton;
	private final Button cancelButton;

	/**
	 * Creates the dialog.
	 *
	 * @param parent the parent window
	 * @param connectionDefinition the database connection parameters
	 */
	public BlobUploadDialog(final Frame parent, final DbConnectionDefinition connectionDefinition) {
		super(parent, DbImport.APPLICATION_NAME + " - " + LangResources.get("blobUpload"), IconKind.FILE);

		this.connectionDefinition = connectionDefinition;

		setResizable(false);

		final JPanel formPanel = new JPanel(new GridBagLayout());
		formPanel.setOpaque(false);

		final String databaseDescription = connectionDefinition.getDbVendor().name()
				+ (StringUtilities.isNotBlank(connectionDefinition.getHostnameAndPort()) ? " " + connectionDefinition.getHostnameAndPort() : "")
				+ (StringUtilities.isNotBlank(connectionDefinition.getDbName()) ? " / " + connectionDefinition.getDbName() : "");
		addRow(formPanel, 0, LangResources.get("section_database"), ModernUi.createMutedLabel(databaseDescription), null);

		updateStatementField = createField(LangResources.get("blobUpdateSql_help"));
		updateStatementField.setText(lastUpdateStatement == null ? "" : lastUpdateStatement);
		addRow(formPanel, 1, LangResources.get("blobUpdateSql"), updateStatementField, null);

		filePathField = createField(LangResources.get("blobFile_help"));
		filePathField.setText(lastFilePath == null ? "" : lastFilePath);
		final Button browseButton = new Button(null, IconKind.FOLDER, ButtonStyle.SECONDARY);
		browseButton.setPreferredSize(new Dimension(30, 30));
		browseButton.setToolTipText(LangResources.get("blobFile"));
		browseButton.addActionListener(event -> {
			final File selectedFile = chooseFile(filePathField.getText());
			if (selectedFile != null) {
				filePathField.setText(selectedFile.getAbsolutePath());
			}
		});
		addRow(formPanel, 2, LangResources.get("blobFile"), filePathField, browseButton);

		getContentArea().add(formPanel);

		uploadButton = addButton(LangResources.get("upload"), null, ButtonStyle.PRIMARY, event -> upload());
		cancelButton = addButton(LangResources.get("cancel"), null, ButtonStyle.SECONDARY, event -> dispose());

		pack();
		setLocationRelativeTo(parent);

		checkButtonStatus();
	}

	/**
	 * Creates a styled input field, which re-checks the buttons on every change.
	 */
	private JTextField createField(final String toolTipText) {
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
	 * Adds a row with label, input component and optional button to the form.
	 */
	private static void addRow(final JPanel formPanel, final int row, final String labelText, final JComponent component, final Button button) {
		final GridBagConstraints labelConstraints = new GridBagConstraints();
		labelConstraints.gridx = 0;
		labelConstraints.gridy = row;
		labelConstraints.anchor = GridBagConstraints.WEST;
		labelConstraints.insets = new Insets(3, 0, 3, 12);
		formPanel.add(ModernUi.createMutedLabel(labelText + ":"), labelConstraints);

		final GridBagConstraints componentConstraints = new GridBagConstraints();
		componentConstraints.gridx = 1;
		componentConstraints.gridy = row;
		componentConstraints.weightx = 1;
		componentConstraints.fill = GridBagConstraints.HORIZONTAL;
		componentConstraints.insets = new Insets(3, 0, 3, 6);
		formPanel.add(component, componentConstraints);

		if (button != null) {
			final GridBagConstraints buttonConstraints = new GridBagConstraints();
			buttonConstraints.gridx = 2;
			buttonConstraints.gridy = row;
			buttonConstraints.insets = new Insets(3, 0, 3, 0);
			formPanel.add(button, buttonConstraints);
		}
	}

	private void checkButtonStatus() {
		final boolean statementOk = StringUtilities.isNotBlank(updateStatementField.getText()) && updateStatementField.getText().contains("?");
		final File file = StringUtilities.isBlank(filePathField.getText()) ? null : new File(filePathField.getText().trim());
		final boolean fileOk = file != null && file.isFile();
		updateStatementField.setBackground(statementOk ? ModernUi.CARD : ModernUi.DANGER_LIGHT);
		filePathField.setBackground(fileOk ? ModernUi.CARD : ModernUi.DANGER_LIGHT);
		uploadButton.setEnabled(statementOk && fileOk);
	}

	/**
	 * Imports the file in the background, so the dialog stays responsive for large files.
	 */
	private void upload() {
		final String updateStatement = updateStatementField.getText().trim();
		final String filePath = filePathField.getText().trim();
		lastUpdateStatement = updateStatement;
		lastFilePath = filePath;

		uploadButton.setEnabled(false);
		cancelButton.setEnabled(false);
		setCursor(Cursor.getPredefinedCursor(Cursor.WAIT_CURSOR));

		new SwingWorker<Integer, Void>() {
			@Override
			protected Integer doInBackground() throws Exception {
				return DbUtilities.updateBlobData(connectionDefinition, updateStatement, filePath);
			}

			@Override
			protected void done() {
				setCursor(Cursor.getDefaultCursor());
				cancelButton.setEnabled(true);
				checkButtonStatus();

				try {
					final int updatedRows = get();
					if (updatedRows > 0) {
						new QuestionDialog(BlobUploadDialog.this, DbImport.APPLICATION_NAME + " OK", LangResources.get("blobUploadResult") + ": " + updatedRows)
								.withDialogStyle(DialogStyle.INFORMATION).withBackgroundColor(ModernUi.SUCCESS_LIGHT).open();
						dispose();
					} else {
						new QuestionDialog(BlobUploadDialog.this, DbImport.APPLICATION_NAME + " WARNING", LangResources.get("blobUploadNoRows"))
								.withDialogStyle(DialogStyle.WARNING).open();
					}
				} catch (final ExecutionException e) {
					final Throwable cause = e.getCause() == null ? e : e.getCause();
					new QuestionDialog(BlobUploadDialog.this, DbImport.APPLICATION_NAME + " ERROR", "ERROR:\n" + (cause.getMessage() != null ? cause.getMessage() : cause.toString()))
							.withDialogStyle(DialogStyle.ERROR).open();
				} catch (final InterruptedException e) {
					Thread.currentThread().interrupt();
					new QuestionDialog(BlobUploadDialog.this, DbImport.APPLICATION_NAME + " ERROR", "ERROR:\n" + e.getMessage()).withDialogStyle(DialogStyle.ERROR).open();
				}
			}
		}.execute();
	}

	private File chooseFile(final String currentPath) {
		String basePath = System.getProperty("user.home");
		if (StringUtilities.isNotBlank(currentPath)) {
			final File current = new File(currentPath.trim());
			if (current.getParentFile() != null && current.getParentFile().exists()) {
				basePath = current.getParent();
			}
		}
		final JFileChooser chooser = new JFileChooser(basePath);
		chooser.setDialogTitle(DbImport.APPLICATION_NAME + " - " + LangResources.get("blobFile"));
		if (JFileChooser.APPROVE_OPTION == chooser.showOpenDialog(this)) {
			return chooser.getSelectedFile();
		} else {
			return null;
		}
	}
}
