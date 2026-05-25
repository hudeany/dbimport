package de.soderer.dbimport;

import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.Frame;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.file.InvalidPathException;
import java.nio.file.Paths;

import javax.swing.Box;
import javax.swing.BoxLayout;
import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JDialog;
import javax.swing.JFileChooser;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JTextField;
import javax.swing.WindowConstants;
import javax.swing.event.DocumentEvent;
import javax.swing.event.DocumentListener;
import javax.swing.filechooser.FileNameExtensionFilter;

import de.soderer.utilities.LangResources;
import de.soderer.utilities.Utilities;
import de.soderer.utilities.db.SqlDdlMergeGenerator;
import de.soderer.utilities.db.SqlDdlMigrationGenerator;
import de.soderer.utilities.swing.QuestionDialog;
import de.soderer.utilities.swing.SwingColor;

public class SqlDdlMigrationDialog extends JDialog {
	private static final long serialVersionUID = -5639244545181271366L;

	private final JTextField sourceFileField;
	private final JTextField destinationFileField;
	private final JTextField outputFileField;

	private final JCheckBox sortBySchemaCheckBox;
	private final JCheckBox sortByTableCheckBox;
	private final JCheckBox sortByColumnCheckBox;

	private final JButton generateMigrationButton;
	private final JButton generateMergeButton;

	public SqlDdlMigrationDialog(final Frame parent) {
		super(parent, DbImport.APPLICATION_NAME + " - SQL DDL Migration / Merge", true);

		setDefaultCloseOperation(WindowConstants.DISPOSE_ON_CLOSE);
		setLayout(new BoxLayout(getContentPane(), BoxLayout.PAGE_AXIS));
		setResizable(false);

		final SqlDdlMigrationDialog dialog = this;

		final JPanel sourcePanel = new JPanel();
		sourcePanel.setLayout(new FlowLayout(FlowLayout.RIGHT));

		final JLabel sourceLabel = new JLabel(LangResources.get("sourceFile") + ":");
		sourceLabel.setPreferredSize(new Dimension(150, 20));
		sourcePanel.add(sourceLabel);

		sourceFileField = new JTextField();
		sourceFileField.setPreferredSize(new Dimension(350, 20));
		sourceFileField.setToolTipText(LangResources.get("sourceFile_help"));
		sourceFileField.getDocument().addDocumentListener(new DocumentListener() {
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
		sourcePanel.add(sourceFileField);

		final JButton sourceButton = new JButton("...");
		sourceButton.setPreferredSize(new Dimension(40, 20));
		sourceButton.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(final ActionEvent event) {
				final File selected = chooseSqlFile(sourceFileField.getText(), LangResources.get("sourceFile"));
				if (selected != null) {
					sourceFileField.setText(selected.getAbsolutePath());
					checkButtonStatus();
				}
			}
		});
		sourcePanel.add(sourceButton);
		add(sourcePanel);

		final JPanel destinationPanel = new JPanel();
		destinationPanel.setLayout(new FlowLayout(FlowLayout.RIGHT));

		final JLabel destinationLabel = new JLabel(LangResources.get("destinationFile") + ":");
		destinationLabel.setPreferredSize(new Dimension(150, 20));
		destinationPanel.add(destinationLabel);

		destinationFileField = new JTextField();
		destinationFileField.setPreferredSize(new Dimension(350, 20));
		destinationFileField.setToolTipText(LangResources.get("destinationFile_help"));
		destinationFileField.getDocument().addDocumentListener(new DocumentListener() {
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
		destinationPanel.add(destinationFileField);

		final JButton destinationButton = new JButton("...");
		destinationButton.setPreferredSize(new Dimension(40, 20));
		destinationButton.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(final ActionEvent event) {
				final File selected = chooseSqlFile(destinationFileField.getText(),
						LangResources.get("destinationFile"));
				if (selected != null) {
					destinationFileField.setText(selected.getAbsolutePath());
					checkButtonStatus();
				}
			}
		});
		destinationPanel.add(destinationButton);
		add(destinationPanel);

		final JPanel outputPanel = new JPanel();
		outputPanel.setLayout(new FlowLayout(FlowLayout.RIGHT));

		final JLabel outputLabel = new JLabel(LangResources.get("outputFile") + ":");
		outputLabel.setPreferredSize(new Dimension(150, 20));
		outputPanel.add(outputLabel);

		outputFileField = new JTextField();
		outputFileField.setPreferredSize(new Dimension(350, 20));
		outputFileField.setToolTipText(LangResources.get("outputFile_help"));
		outputFileField.getDocument().addDocumentListener(new DocumentListener() {
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
		outputPanel.add(outputFileField);

		final JButton outputButton = new JButton("...");
		outputButton.setPreferredSize(new Dimension(40, 20));
		outputButton.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(final ActionEvent event) {
				final File selected = saveSqlFile(outputFileField.getText(), LangResources.get("outputFile"));
				if (selected != null) {
					outputFileField.setText(selected.getAbsolutePath());
					checkButtonStatus();
				}
			}
		});
		outputPanel.add(outputButton);
		add(outputPanel);

		add(Box.createRigidArea(new Dimension(0, 5)));

		final JPanel sortPanel = new JPanel();
		sortPanel.setLayout(new FlowLayout(FlowLayout.CENTER));

		sortBySchemaCheckBox = new JCheckBox(LangResources.get("sortBySchema"));
		sortBySchemaCheckBox.setToolTipText(LangResources.get("sortBySchema_help"));
		sortPanel.add(sortBySchemaCheckBox);

		sortByTableCheckBox = new JCheckBox(LangResources.get("sortByTable"));
		sortByTableCheckBox.setToolTipText(LangResources.get("sortByTable_help"));
		sortPanel.add(sortByTableCheckBox);

		sortByColumnCheckBox = new JCheckBox(LangResources.get("sortByColumn"));
		sortByColumnCheckBox.setToolTipText(LangResources.get("sortByColumn_help"));
		sortPanel.add(sortByColumnCheckBox);

		add(sortPanel);

		add(Box.createRigidArea(new Dimension(0, 5)));

		final JPanel buttonPanel = new JPanel();
		buttonPanel.setLayout(new FlowLayout(FlowLayout.CENTER));

		generateMigrationButton = new JButton(LangResources.get("generateMigration"));
		generateMigrationButton.setEnabled(false);
		generateMigrationButton.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(final ActionEvent event) {
				runMigration(dialog);
			}
		});
		buttonPanel.add(generateMigrationButton);

		buttonPanel.add(Box.createRigidArea(new Dimension(5, 0)));

		generateMergeButton = new JButton(LangResources.get("generateMerge"));
		generateMergeButton.setEnabled(false);
		generateMergeButton.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(final ActionEvent event) {
				runMerge(dialog);
			}
		});
		buttonPanel.add(generateMergeButton);

		buttonPanel.add(Box.createRigidArea(new Dimension(5, 0)));

		final JButton cancelButton = new JButton(LangResources.get("cancel"));
		cancelButton.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(final ActionEvent event) {
				dispose();
			}
		});
		buttonPanel.add(cancelButton);

		add(buttonPanel);

		pack();
		setLocationRelativeTo(parent);
	}

	private void checkButtonStatus() {
		final boolean sourceOk      = isExistingFile(sourceFileField.getText());
		final boolean destinationOk = isExistingFile(destinationFileField.getText());
		final boolean outputOk      = Utilities.isNotBlank(outputFileField.getText())
				&& isValidPath(outputFileField.getText());
		final boolean allOk = sourceOk && destinationOk && outputOk;
		generateMigrationButton.setEnabled(allOk);
		generateMergeButton.setEnabled(allOk);
	}

	private static boolean isExistingFile(final String path) {
		if (Utilities.isBlank(path)) {
			return false;
		}
		final File f = new File(path.trim());
		return f.exists() && f.isFile();
	}

	public static boolean isValidPath(final String path) {
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
							.setBackgroundColor(SwingColor.LightRed).open();
			return;
		}
		if (!destinationFile.exists()) {
			new QuestionDialog(dialog, DbImport.APPLICATION_NAME + " ERROR",
					"ERROR:\nDestination file does not exist:\n" + destinationFile.getAbsolutePath())
							.setBackgroundColor(SwingColor.LightRed).open();
			return;
		}
		if (outputFile.getParentFile() != null && !outputFile.getParentFile().exists()) {
			new QuestionDialog(dialog, DbImport.APPLICATION_NAME + " ERROR",
					"ERROR:\nOutput directory does not exist:\n" + outputFile.getParent())
							.setBackgroundColor(SwingColor.LightRed).open();
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
							.setBackgroundColor(SwingColor.Green).open();

			dispose();

		} catch (final Exception e) {
			new QuestionDialog(dialog, DbImport.APPLICATION_NAME + " ERROR",
					"ERROR:\n" + e.getClass().getSimpleName() + ":\n" + e.getMessage())
							.setBackgroundColor(SwingColor.LightRed).open();
		}
	}

	private void runMerge(final SqlDdlMigrationDialog dialog) {
		final File sourceFile      = new File(sourceFileField.getText().trim());
		final File destinationFile = new File(destinationFileField.getText().trim());
		final File outputFile      = new File(outputFileField.getText().trim());

		if (!sourceFile.exists()) {
			new QuestionDialog(dialog, DbImport.APPLICATION_NAME + " ERROR",
					"ERROR:\nSource file does not exist:\n" + sourceFile.getAbsolutePath())
							.setBackgroundColor(SwingColor.LightRed).open();
			return;
		}
		if (!destinationFile.exists()) {
			new QuestionDialog(dialog, DbImport.APPLICATION_NAME + " ERROR",
					"ERROR:\nDestination file does not exist:\n" + destinationFile.getAbsolutePath())
							.setBackgroundColor(SwingColor.LightRed).open();
			return;
		}
		if (outputFile.getParentFile() != null && !outputFile.getParentFile().exists()) {
			new QuestionDialog(dialog, DbImport.APPLICATION_NAME + " ERROR",
					"ERROR:\nOutput directory does not exist:\n" + outputFile.getParent())
							.setBackgroundColor(SwingColor.LightRed).open();
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
							.setBackgroundColor(SwingColor.Green).open();

			dispose();

		} catch (final Exception e) {
			new QuestionDialog(dialog, DbImport.APPLICATION_NAME + " ERROR",
					"ERROR:\n" + e.getClass().getSimpleName() + ":\n" + e.getMessage())
							.setBackgroundColor(SwingColor.LightRed).open();
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
		if (Utilities.isNotBlank(currentPath)) {
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