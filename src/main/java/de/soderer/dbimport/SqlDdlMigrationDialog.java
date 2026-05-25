package de.soderer.dbimport;

import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.Frame;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;

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
	private final JCheckBox mergeCheckBox;
	private final JButton generateButton;

	public SqlDdlMigrationDialog(final Frame parent) {
		super(parent, DbImport.APPLICATION_NAME + " - DDL Migration", true);

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
		sourcePanel.add(sourceFileField);

		final JButton sourceButton = new JButton("...");
		sourceButton.setPreferredSize(new Dimension(40, 20));
		sourceButton.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(final ActionEvent event) {
				final File selected = chooseSqlFile(sourceFileField.getText(), LangResources.get("sourceFile"));
				if (selected != null) {
					sourceFileField.setText(selected.getAbsolutePath());
					checkGenerateButtonStatus();
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
		destinationPanel.add(destinationFileField);

		final JButton destinationButton = new JButton("...");
		destinationButton.setPreferredSize(new Dimension(40, 20));
		destinationButton.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(final ActionEvent event) {
				final File selected = chooseSqlFile(destinationFileField.getText(), LangResources.get("destinationFile"));
				if (selected != null) {
					destinationFileField.setText(selected.getAbsolutePath());
					checkGenerateButtonStatus();
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
		outputPanel.add(outputFileField);

		final JButton outputButton = new JButton("...");
		outputButton.setPreferredSize(new Dimension(40, 20));
		outputButton.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(final ActionEvent event) {
				final File selected = saveSqlFile(outputFileField.getText(), LangResources.get("outputFile"));
				if (selected != null) {
					outputFileField.setText(selected.getAbsolutePath());
					checkGenerateButtonStatus();
				}
			}
		});
		outputPanel.add(outputButton);
		add(outputPanel);

		add(Box.createRigidArea(new Dimension(0, 5)));

		final JPanel modePanel = new JPanel();
		modePanel.setLayout(new FlowLayout(FlowLayout.RIGHT));

		mergeCheckBox = new JCheckBox(LangResources.get("mergeMode"));
		mergeCheckBox.setToolTipText(LangResources.get("mergeMode_help"));
		mergeCheckBox.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(final ActionEvent event) {
				final boolean isMerge = mergeCheckBox.isSelected();
				generateButton.setText(LangResources.get(isMerge ? "generateMerge" : "generateMigration"));
				setTitle(DbImport.APPLICATION_NAME + " - " + LangResources.get(isMerge ? "merge" : "migration"));
			}
		});
		modePanel.add(mergeCheckBox);
		add(modePanel);

		add(Box.createRigidArea(new Dimension(0, 5)));

		final JPanel buttonPanel = new JPanel();
		buttonPanel.setLayout(new FlowLayout(FlowLayout.CENTER));

		generateButton = new JButton(LangResources.get("generateMigration"));
		generateButton.setEnabled(false);
		generateButton.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(final ActionEvent event) {
				if (mergeCheckBox.isSelected()) {
					runMerge(dialog);
				} else {
					runMigration(dialog);
				}
			}
		});
		buttonPanel.add(generateButton);

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

	private void checkGenerateButtonStatus() {
		final boolean sourceOk      = isExistingFile(sourceFileField.getText());
		final boolean destinationOk = isExistingFile(destinationFileField.getText());
		final boolean outputOk      = Utilities.isNotBlank(outputFileField.getText());
		generateButton.setEnabled(sourceOk && destinationOk && outputOk);
	}

	private static boolean isExistingFile(final String path) {
		if (Utilities.isBlank(path)) {
			return false;
		}
		final File f = new File(path.trim());
		return f.exists() && f.isFile();
	}

	private void runMigration(final SqlDdlMigrationDialog dialog) {
		final File sourceFile      = new File(sourceFileField.getText().trim());
		final File destinationFile = new File(destinationFileField.getText().trim());
		final File outputFile      = new File(outputFileField.getText().trim());

		// Basic validation
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

			SqlDdlMigrationGenerator.diff(srcStream, dstStream, outStream);

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

			SqlDdlMergeGenerator.merge(srcStream, dstStream, outStream);

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
