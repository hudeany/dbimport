package de.soderer.dbcsvimport;


import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.ItemEvent;
import java.awt.event.ItemListener;
import java.io.File;
import java.sql.Connection;
import java.util.Date;
import java.util.List;

import javax.swing.BorderFactory;
import javax.swing.Box;
import javax.swing.BoxLayout;
import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JFileChooser;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JPasswordField;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.JTextField;

import de.soderer.dbcsvimport.DbCsvImportDefinition.DataType;
import de.soderer.dbcsvimport.DbCsvImportDefinition.ImportMode;
import de.soderer.dbcsvimport.worker.AbstractDbImportWorker;
import de.soderer.utilities.ApplicationUpdateHelper;
import de.soderer.utilities.DateUtilities;
import de.soderer.utilities.DbColumnType;
import de.soderer.utilities.DbUtilities;
import de.soderer.utilities.DbUtilities.DbVendor;
import de.soderer.utilities.ExceptionUtilities;
import de.soderer.utilities.LangResources;
import de.soderer.utilities.Utilities;
import de.soderer.utilities.Version;
import de.soderer.utilities.collection.CaseInsensitiveMap;
import de.soderer.utilities.swing.BasicUpdateableGuiApplication;
import de.soderer.utilities.swing.SecurePreferencesDialog;
import de.soderer.utilities.swing.SimpleProgressDialog;
import de.soderer.utilities.swing.SimpleProgressDialog.Result;
import de.soderer.utilities.swing.TextDialog;

/**
 * TODO
 * Tests for DBs: Firebird
 */

/**
 * The GUI for DbCsvImport.
 */
public class DbCsvImportGui extends BasicUpdateableGuiApplication {

	/** The Constant serialVersionUID. */
	private static final long serialVersionUID = 5969613637206441880L;

	/** The db type combo. */
	private JComboBox<String> dbTypeCombo;

	/** The host field. */
	private JTextField hostField;

	/** The db name field. */
	private JTextField dbNameField;

	/** The user field. */
	private JTextField userField;

	/** The password field. */
	private JTextField passwordField;

	/** The tablename field. */
	private JTextField tableNameField;

	/** The data type combo. */
	private JComboBox<String> dataTypeCombo;

	/** The file log box. */
	private JCheckBox fileLogBox;

	/** The importFilePath field. */
	private JTextField importFilePathField;

	/** The separator combo. */
	private JComboBox<String> separatorCombo;

	/** The string quote combo. */
	private JComboBox<String> stringQuoteCombo;

	/** The null value string combo. */
	private JComboBox<String> nullValueStringCombo;

	/** The encoding combo. */
	private JComboBox<String> encodingCombo;
	
	private JTextField keyColumnsField;

	/** The mapping field. */
	private JTextArea mappingField;

	/** The additionalInsertValues field. */
	private JTextArea additionalInsertValuesField;

	/** The additionalUpdateValues field. */
	private JTextArea additionalUpdateValuesField;

	/** The no headers box. */
	private JCheckBox noHeadersBox;
	
	private JCheckBox allowUnderfilledLinesBox;
	
	private JCheckBox createTableBox;
	
	private JCheckBox onlyCommitOnFullSuccessBox;
	
	private JCheckBox updateWithNullDataBox;
	
	private JCheckBox trimDataBox;
	
	private JComboBox<String> importModeCombo;
	
	private JButton mappingButton;

	/** The temporary preferences password. */
	private char[] temporaryPreferencesPassword = null;

	/**
	 * Instantiates a new db csv import gui.
	 *
	 * @param dbCsvImportDefinition
	 *            the db csv import definition
	 * @throws Exception
	 *             the exception
	 */
	public DbCsvImportGui(DbCsvImportDefinition dbCsvImportDefinition) throws Exception {
		super(DbCsvImport.APPLICATION_NAME, new Version(DbCsvImport.VERSION));

		final DbCsvImportGui dbCsvImportGui = this;

		setLocationRelativeTo(null);

		setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		setLayout(new BoxLayout(getContentPane(), BoxLayout.PAGE_AXIS));

		// Parameter Panel
		JPanel parameterPanel = new JPanel();
		parameterPanel.setLayout(new BoxLayout(parameterPanel, BoxLayout.LINE_AXIS));

		// Mandatory parameter Panel
		JPanel mandatoryParameterPanel = new JPanel();
		mandatoryParameterPanel.setLayout(new BoxLayout(mandatoryParameterPanel, BoxLayout.PAGE_AXIS));

		// DBType Pane
		JPanel dbTypePanel = new JPanel();
		dbTypePanel.setLayout(new FlowLayout());
		JLabel dbTypeLabel = new JLabel(LangResources.get("dbtype"));
		dbTypePanel.add(dbTypeLabel);
		dbTypeCombo = new JComboBox<String>();
		dbTypeCombo.setToolTipText(LangResources.get("dbtype_help"));
		for (DbVendor dbVendor : DbVendor.values()) {
			dbTypeCombo.addItem(dbVendor.toString());
		}
		dbTypeCombo.addItemListener(new ItemListener() {
			@Override
			public void itemStateChanged(ItemEvent e) {
				checkButtonStatus();
			}
		});
		dbTypePanel.add(dbTypeCombo, BorderLayout.EAST);
		mandatoryParameterPanel.add(dbTypePanel);

		// Host Panel
		JPanel hostPanel = new JPanel();
		hostPanel.setLayout(new FlowLayout(FlowLayout.RIGHT));
		JLabel hostLabel = new JLabel(LangResources.get("host"));
		hostPanel.add(hostLabel);
		hostField = new JTextField();
		hostField.setToolTipText(LangResources.get("host_help"));
		hostField.setPreferredSize(new Dimension(200, hostField.getPreferredSize().height));
		hostPanel.add(hostField);
		mandatoryParameterPanel.add(hostPanel);

		// DB name Panel
		JPanel dbNamePanel = new JPanel();
		dbNamePanel.setLayout(new FlowLayout(FlowLayout.RIGHT));
		JLabel dbNameLabel = new JLabel(LangResources.get("dbname"));
		dbNamePanel.add(dbNameLabel);
		dbNameField = new JTextField();
		dbNameField.setToolTipText(LangResources.get("dbname_help"));
		dbNameField.setPreferredSize(new Dimension(200, dbNameField.getPreferredSize().height));
		dbNamePanel.add(dbNameField);
		mandatoryParameterPanel.add(dbNamePanel);

		// User Panel
		JPanel userPanel = new JPanel();
		userPanel.setLayout(new FlowLayout(FlowLayout.RIGHT));
		JLabel userLabel = new JLabel(LangResources.get("user"));
		userPanel.add(userLabel);
		userField = new JTextField();
		userField.setToolTipText(LangResources.get("user_help"));
		userField.setPreferredSize(new Dimension(200, userField.getPreferredSize().height));
		userPanel.add(userField);
		mandatoryParameterPanel.add(userPanel);

		// Password Panel
		JPanel passwordPanel = new JPanel();
		passwordPanel.setLayout(new FlowLayout(FlowLayout.RIGHT));
		JLabel passwordLabel = new JLabel(LangResources.get("password"));
		passwordPanel.add(passwordLabel);
		passwordField = new JPasswordField();
		passwordField.setToolTipText(LangResources.get("password_help"));
		passwordField.setPreferredSize(new Dimension(200, passwordField.getPreferredSize().height));
		passwordPanel.add(passwordField);
		mandatoryParameterPanel.add(passwordPanel);

		// TableName Panel
		JPanel tableNamePanel = new JPanel();
		tableNamePanel.setLayout(new FlowLayout(FlowLayout.RIGHT));
		JLabel tableNameLabel = new JLabel(LangResources.get("tableName"));
		tableNamePanel.add(tableNameLabel);
		tableNameField = new JTextField();
		tableNameField.setToolTipText(LangResources.get("tableName_help"));
		tableNameField.setPreferredSize(new Dimension(200, tableNameField.getPreferredSize().height));
		tableNamePanel.add(tableNameField);
		mandatoryParameterPanel.add(tableNamePanel);

		// Import type Pane
		JPanel dataTypePanel = new JPanel();
		dataTypePanel.setLayout(new FlowLayout());
		JLabel dataTypeLabel = new JLabel(LangResources.get("dataType"));
		dataTypePanel.add(dataTypeLabel);
		dataTypeCombo = new JComboBox<String>();
		dataTypeCombo.setToolTipText(LangResources.get("dataType_help"));
		for (DataType dataType : DataType.values()) {
			dataTypeCombo.addItem(dataType.toString());
		}
		dataTypeCombo.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				checkButtonStatus();
			}
		});
		dataTypePanel.add(dataTypeCombo, BorderLayout.EAST);
		mandatoryParameterPanel.add(dataTypePanel);

		// Outputpath Panel
		JPanel importFilePathPanel = new JPanel();
		importFilePathPanel.setLayout(new FlowLayout(FlowLayout.RIGHT));
		JLabel outputpathLabel = new JLabel(LangResources.get("importFilePath"));
		importFilePathPanel.add(outputpathLabel);
		importFilePathField = new JTextField();
		importFilePathField.setToolTipText(LangResources.get("importFilePath_help"));
		importFilePathField.setPreferredSize(new Dimension(175, importFilePathField.getPreferredSize().height));
		importFilePathField.setBorder(BorderFactory.createLineBorder(Color.GRAY));
		importFilePathPanel.add(importFilePathField);
		mandatoryParameterPanel.add(importFilePathPanel);
		
		JButton browseButton = new JButton("...");
		browseButton.setPreferredSize(new Dimension(20, importFilePathField.getPreferredSize().height));
		browseButton.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent event) {
				try {
					File importFile = selectImportFile(importFilePathField.getText());
					if (importFile != null) {
						importFilePathField.setText(importFile.getAbsolutePath());
					}
				} catch (Exception e) {
					new TextDialog(dbCsvImportGui, DbCsvImport.APPLICATION_NAME + " ERROR", "ERROR:\n" + e.getMessage(), LangResources.get("close"), false, Color.RED).setVisible(true);
				}
			}
		});
		importFilePathPanel.add(browseButton);

		// Encoding Pane
		JPanel encodingPanel = new JPanel();
		encodingPanel.setLayout(new FlowLayout(FlowLayout.RIGHT));
		JLabel encodingLabel = new JLabel(LangResources.get("encoding"));
		encodingPanel.add(encodingLabel);
		encodingCombo = new JComboBox<String>();
		encodingCombo.setToolTipText(LangResources.get("encoding_help"));
		encodingCombo.setPreferredSize(new Dimension(200, encodingCombo.getPreferredSize().height));
		encodingCombo.addItem("UTF-8");
		encodingCombo.addItem("ISO-8859-1");
		encodingCombo.addItem("ISO-8859-15");
		encodingCombo.setEditable(true);
		encodingPanel.add(encodingCombo);
		mandatoryParameterPanel.add(encodingPanel);

		// Separator Pane
		JPanel separatorPanel = new JPanel();
		separatorPanel.setLayout(new FlowLayout(FlowLayout.RIGHT));
		JLabel separatorLabel = new JLabel(LangResources.get("separator"));
		separatorPanel.add(separatorLabel);
		separatorCombo = new JComboBox<String>();
		separatorCombo.setToolTipText(LangResources.get("separator_help"));
		separatorCombo.setPreferredSize(new Dimension(200, separatorCombo.getPreferredSize().height));
		separatorCombo.addItem(";");
		separatorCombo.addItem(",");
		separatorCombo.setEditable(true);
		separatorPanel.add(separatorCombo);
		mandatoryParameterPanel.add(separatorPanel);

		// StringQuote Pane
		JPanel stringQuotePanel = new JPanel();
		stringQuotePanel.setLayout(new FlowLayout(FlowLayout.RIGHT));
		JLabel stringQuoteLabel = new JLabel(LangResources.get("stringquote"));
		stringQuotePanel.add(stringQuoteLabel);
		stringQuoteCombo = new JComboBox<String>();
		stringQuoteCombo.setToolTipText(LangResources.get("stringquote_help"));
		stringQuoteCombo.setPreferredSize(new Dimension(200, stringQuoteCombo.getPreferredSize().height));
		stringQuoteCombo.addItem("\"");
		stringQuoteCombo.addItem("'");
		stringQuoteCombo.setEditable(true);
		stringQuotePanel.add(stringQuoteCombo);
		mandatoryParameterPanel.add(stringQuotePanel);

		// NullValueString Pane
		JPanel nullValueStringPanel = new JPanel();
		nullValueStringPanel.setLayout(new FlowLayout(FlowLayout.RIGHT));
		JLabel nullValueStringLabel = new JLabel(LangResources.get("nullvaluetext"));
		nullValueStringPanel.add(nullValueStringLabel);
		nullValueStringCombo = new JComboBox<String>();
		nullValueStringCombo.setToolTipText(LangResources.get("nullvaluetext_help"));
		nullValueStringCombo.setPreferredSize(new Dimension(200, nullValueStringCombo.getPreferredSize().height));
		nullValueStringCombo.addItem("");
		nullValueStringCombo.addItem("NULL");
		nullValueStringCombo.addItem("Null");
		nullValueStringCombo.addItem("null");
		nullValueStringCombo.setEditable(true);
		nullValueStringPanel.add(nullValueStringCombo);
		mandatoryParameterPanel.add(nullValueStringPanel);

		// Importmode Pane
		JPanel importModePanel = new JPanel();
		importModePanel.setLayout(new FlowLayout(FlowLayout.RIGHT));
		JLabel importModeLabel = new JLabel(LangResources.get("importMode"));
		importModePanel.add(importModeLabel);
		importModeCombo = new JComboBox<String>();
		importModeCombo.setToolTipText(LangResources.get("importMode_help"));
		importModeCombo.setPreferredSize(new Dimension(200, importModeCombo.getPreferredSize().height));
		importModeCombo.addItem("CLEARINSERT");
		importModeCombo.addItem("INSERT");
		importModeCombo.addItem("UPDATE");
		importModeCombo.addItem("UPSERT");
		importModeCombo.setEditable(false);
		importModeCombo.addItemListener(new ItemListener() {
			@Override
			public void itemStateChanged(ItemEvent e) {
				checkButtonStatus();
			}
		});
		importModePanel.add(importModeCombo);
		mandatoryParameterPanel.add(importModePanel);

		// Key columns Panel
		JPanel keyColumnsPanel = new JPanel();
		keyColumnsPanel.setLayout(new FlowLayout(FlowLayout.RIGHT));
		JLabel keycolumnsLabel = new JLabel(LangResources.get("keyColumns"));
		keyColumnsPanel.add(keycolumnsLabel);
		keyColumnsField = new JTextField();
		keyColumnsField.setToolTipText(LangResources.get("keyColumns_help"));
		keyColumnsField.setPreferredSize(new Dimension(200, keyColumnsField.getPreferredSize().height));
		keyColumnsPanel.add(keyColumnsField);
		mandatoryParameterPanel.add(keyColumnsPanel);
		
		// Mapping Panel
		JPanel mappingPanel = new JPanel();
		mappingPanel.setLayout(new FlowLayout(FlowLayout.RIGHT));
		JLabel mappingLabel = new JLabel(LangResources.get("mapping"));
		mappingPanel.add(mappingLabel);
		mappingField = new JTextArea();
		mappingField.setToolTipText(LangResources.get("mapping_help"));
		JScrollPane mappingScrollpane = new JScrollPane(mappingField);
		mappingScrollpane.setPreferredSize(new Dimension(200, 75));
		mappingPanel.add(mappingScrollpane);
		mandatoryParameterPanel.add(mappingPanel);
		
		// Additional Insert values Panel
		JPanel additionalInsertValuesPanel = new JPanel();
		additionalInsertValuesPanel.setLayout(new FlowLayout(FlowLayout.RIGHT));
		JLabel additionalInsertValuesLabel = new JLabel(LangResources.get("additionalInsertValues"));
		additionalInsertValuesPanel.add(additionalInsertValuesLabel);
		additionalInsertValuesField = new JTextArea();
		additionalInsertValuesField.setToolTipText(LangResources.get("additionalInsertValues_help"));
		JScrollPane additionalInsertValuesScrollpane = new JScrollPane(additionalInsertValuesField);
		additionalInsertValuesScrollpane.setPreferredSize(new Dimension(200, 35));
		additionalInsertValuesPanel.add(additionalInsertValuesScrollpane);
		mandatoryParameterPanel.add(additionalInsertValuesPanel);
		
		// Additional Update values Panel
		JPanel additionalUpdateValuesPanel = new JPanel();
		additionalUpdateValuesPanel.setLayout(new FlowLayout(FlowLayout.RIGHT));
		JLabel additionalUpdateValuesLabel = new JLabel(LangResources.get("additionalUpdateValues"));
		additionalUpdateValuesPanel.add(additionalUpdateValuesLabel);
		additionalUpdateValuesField = new JTextArea();
		additionalUpdateValuesField.setToolTipText(LangResources.get("additionalUpdateValues_help"));
		JScrollPane additionalUpdateValuesScrollpane = new JScrollPane(additionalUpdateValuesField);
		additionalUpdateValuesScrollpane.setPreferredSize(new Dimension(200, 35));
		additionalUpdateValuesPanel.add(additionalUpdateValuesScrollpane);
		mandatoryParameterPanel.add(additionalUpdateValuesPanel);

		// Optional parameters Panel
		JPanel optionalParametersPanel = new JPanel();
		optionalParametersPanel.setLayout(new BoxLayout(optionalParametersPanel, BoxLayout.PAGE_AXIS));

		fileLogBox = new JCheckBox(LangResources.get("filelog"));
		fileLogBox.setToolTipText(LangResources.get("filelog_help"));
		optionalParametersPanel.add(fileLogBox);

		allowUnderfilledLinesBox = new JCheckBox(LangResources.get("allowUnderfilledLines"));
		allowUnderfilledLinesBox.setToolTipText(LangResources.get("allowUnderfilledLines_help"));
		optionalParametersPanel.add(allowUnderfilledLinesBox);

		noHeadersBox = new JCheckBox(LangResources.get("noheaders"));
		noHeadersBox.setToolTipText(LangResources.get("noheaders_help"));
		optionalParametersPanel.add(noHeadersBox);

		createTableBox = new JCheckBox(LangResources.get("createTable"));
		createTableBox.setToolTipText(LangResources.get("noheaders_help"));
		optionalParametersPanel.add(createTableBox);
		
		onlyCommitOnFullSuccessBox = new JCheckBox(LangResources.get("onlyCommitOnFullSuccess"));
		onlyCommitOnFullSuccessBox.setToolTipText(LangResources.get("onlyCommitOnFullSuccess_help"));
		optionalParametersPanel.add(onlyCommitOnFullSuccessBox);
		
		updateWithNullDataBox = new JCheckBox(LangResources.get("updateWithNullData"));
		updateWithNullDataBox.setToolTipText(LangResources.get("updateWithNullData_help"));
		optionalParametersPanel.add(updateWithNullDataBox);
		
		trimDataBox = new JCheckBox(LangResources.get("trimData"));
		trimDataBox.setToolTipText(LangResources.get("trimData_help"));
		optionalParametersPanel.add(trimDataBox);

		// Button Panel 1
		JPanel buttonPanel1 = new JPanel();
		buttonPanel1.setLayout(new BoxLayout(buttonPanel1, BoxLayout.LINE_AXIS));

		// Start Button
		JButton startButton = new JButton(LangResources.get("import"));
		startButton.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent event) {
				try {
					importData(getConfigurationAsDefinition(), dbCsvImportGui);
				} catch (Exception e) {
					new TextDialog(dbCsvImportGui, DbCsvImport.APPLICATION_NAME + " ERROR", "ERROR:\n" + e.getMessage(), LangResources.get("close"), false, Color.RED).setVisible(true);
				}
			}
		});
		buttonPanel1.add(startButton);

		buttonPanel1.add(Box.createRigidArea(new Dimension(5, 0)));

		// Mapping Button
		mappingButton = new JButton(LangResources.get("createMapping"));
		mappingButton.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent event) {
				try {
					createMapping(getConfigurationAsDefinition(), dbCsvImportGui);
				} catch (Exception e) {
					new TextDialog(dbCsvImportGui, DbCsvImport.APPLICATION_NAME + " ERROR", "ERROR:\n" + e.getMessage(), LangResources.get("close"), false, Color.RED).setVisible(true);
				}
			}
		});
		buttonPanel1.add(mappingButton);

		// Button Panel 2
		JPanel buttonPanel2 = new JPanel();
		buttonPanel2.setLayout(new BoxLayout(buttonPanel2, BoxLayout.LINE_AXIS));

		// Preferences Button
		JButton preferencesButton = new JButton(LangResources.get("preferences"));
		preferencesButton.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent event) {
				try {
					SecurePreferencesDialog credentialsDialog = new SecurePreferencesDialog(dbCsvImportGui, DbCsvImport.APPLICATION_NAME + " " + LangResources.get("preferences"),
							LangResources.get("preferences_text"), DbCsvImport.SECURE_PREFERENCES_FILE, LangResources.get("load"), LangResources.get("create"), LangResources.get("update"),
							LangResources.get("delete"), LangResources.get("preferences_save"), LangResources.get("cancel"), LangResources.get("preferences_password_text"),
							LangResources.get("username"), LangResources.get("password"), LangResources.get("ok"), LangResources.get("cancel"));

					credentialsDialog.setCurrentSecureDataEntry(getConfigurationAsDefinition());
					credentialsDialog.setPassword(temporaryPreferencesPassword);
					credentialsDialog.showDialog();
					if (credentialsDialog.getCurrentSecureDataEntry() != null) {
						setConfigurationByDefinition((DbCsvImportDefinition) credentialsDialog.getCurrentSecureDataEntry());
					}

					temporaryPreferencesPassword = credentialsDialog.getPassword();
				} catch (Exception e) {
					temporaryPreferencesPassword = null;
					TextDialog textDialog = new TextDialog(dbCsvImportGui, DbCsvImport.APPLICATION_NAME + " ERROR", "ERROR:\n" + e.getMessage(), LangResources.get("close"), false, Color.PINK);
					textDialog.setVisible(true);
				}
			}
		});
		buttonPanel2.add(preferencesButton);

		buttonPanel2.add(Box.createRigidArea(new Dimension(5, 0)));

		// Update Button
		JButton updateButton = new JButton(LangResources.get("checkupdate"));
		updateButton.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent event) {
				try {
					new ApplicationUpdateHelper(DbCsvImport.APPLICATION_NAME, DbCsvImport.VERSION, DbCsvImport.VERSIONINFO_DOWNLOAD_URL, dbCsvImportGui, "-gui").executeUpdate();
				} catch (Exception e) {
					TextDialog textDialog = new TextDialog(dbCsvImportGui, DbCsvImport.APPLICATION_NAME + " ERROR", "ERROR:\n" + e.getMessage(), LangResources.get("close"), false, Color.PINK);
					textDialog.setVisible(true);
				}
			}
		});
		buttonPanel2.add(updateButton);

		buttonPanel2.add(Box.createRigidArea(new Dimension(5, 0)));

		// Close Button
		JButton closeButton = new JButton(LangResources.get("close"));
		closeButton.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent event) {
				dispose();
			}
		});
		buttonPanel2.add(closeButton);

		parameterPanel.add(mandatoryParameterPanel);
		parameterPanel.add(optionalParametersPanel);
		add(parameterPanel);
		add(buttonPanel1);
		add(Box.createRigidArea(new Dimension(0, 5)));
		add(buttonPanel2);

		setConfigurationByDefinition(dbCsvImportDefinition);

		checkButtonStatus();

		add(Box.createRigidArea(new Dimension(0, 5)));

		pack();

		setLocationRelativeTo(null);
		setResizable(false);
	}

	protected void createMapping(DbCsvImportDefinition configurationAsDefinition, DbCsvImportGui dbCsvImportGui) throws Exception {
		if (Utilities.isBlank(importFilePathField.getText())) {
			throw new Exception("ImportFilePath is needed for mapping");
		} else if (Utilities.isBlank(tableNameField.getText())) {
			throw new Exception("TableName is needed for mapping");
		}
		
		Connection connection = null;
		try {
			connection = DbUtilities.createConnection(configurationAsDefinition.getDbVendor(), configurationAsDefinition.getHostname(), configurationAsDefinition.getDbName(), configurationAsDefinition.getUsername(), configurationAsDefinition.getPassword().toCharArray());
			CaseInsensitiveMap<DbColumnType> columnTypes = DbUtilities.getColumnDataTypes(connection, configurationAsDefinition.getTableName());
			List<String> keyColumns = DbUtilities.getPrimaryKeyColumns(connection, configurationAsDefinition.getTableName());
			if (Utilities.isBlank(keyColumnsField.getText())) {
				keyColumnsField.setText(Utilities.join(keyColumns, ", "));
			}

			List<String> dataColumns = configurationAsDefinition.getConfiguredWorker(null).getAvailableDataPropertyNames();
			DbCsvImportMappingDialog columnMappingDialog = new DbCsvImportMappingDialog(this, DbCsvImport.APPLICATION_NAME + " " + LangResources.get("mapping"), columnTypes, dataColumns);
			columnMappingDialog.setMappingString(mappingField.getText());
			if (columnMappingDialog.showDialog()) {
				mappingField.setText(columnMappingDialog.getMappingString());
			}
		} finally {
			Utilities.closeQuietly(connection);
		}
	}

	/**
	 * Gets the configuration as definition.
	 *
	 * @return the configuration as definition
	 * @throws Exception
	 *             the exception
	 */
	private DbCsvImportDefinition getConfigurationAsDefinition() throws Exception {
		DbCsvImportDefinition dbCsvImportDefinition = new DbCsvImportDefinition();

		dbCsvImportDefinition.setDbVendor(DbVendor.getDbVendorByName((String) dbTypeCombo.getSelectedItem()));
		dbCsvImportDefinition.setHostname(hostField.isEnabled() ? hostField.getText() : null);
		dbCsvImportDefinition.setDbName(dbNameField.getText());
		dbCsvImportDefinition.setUsername(userField.isEnabled() ? userField.getText() : null);
		dbCsvImportDefinition.setPassword(passwordField.isEnabled() ? passwordField.getText() : null);
		dbCsvImportDefinition.setTableName(tableNameField.getText());
		dbCsvImportDefinition.setImportFilePath(importFilePathField.getText());
		dbCsvImportDefinition.setDataType(DataType.getFromString((String) dataTypeCombo.getSelectedItem()));
		dbCsvImportDefinition.setLog(fileLogBox.isSelected());
		dbCsvImportDefinition.setEncoding((String) encodingCombo.getSelectedItem());
		dbCsvImportDefinition.setSeparator(((String) separatorCombo.getSelectedItem()).charAt(0));
		dbCsvImportDefinition.setStringQuote(((String) stringQuoteCombo.getSelectedItem()).charAt(0));
		dbCsvImportDefinition.setNoHeaders(noHeadersBox.isSelected());
		dbCsvImportDefinition.setNullValueString((String) nullValueStringCombo.getSelectedItem());
		dbCsvImportDefinition.setCompleteCommit(onlyCommitOnFullSuccessBox.isSelected());
		dbCsvImportDefinition.setAllowUnderfilledLines(allowUnderfilledLinesBox.isSelected());
		dbCsvImportDefinition.setImportmode(ImportMode.getFromString(((String) importModeCombo.getSelectedItem())));
		dbCsvImportDefinition.setUpdateNullData(updateWithNullDataBox.isSelected());
		dbCsvImportDefinition.setKeycolumns(Utilities.splitAndTrimList(keyColumnsField.getText()));
		dbCsvImportDefinition.setCreateTable(createTableBox.isSelected());
		dbCsvImportDefinition.setMapping(mappingField.getText());
		dbCsvImportDefinition.setAdditionalInsertValues(additionalInsertValuesField.getText());
		dbCsvImportDefinition.setAdditionalUpdateValues(additionalUpdateValuesField.getText());
		
		return dbCsvImportDefinition;
	}

	/**
	 * Sets the configuration by definition.
	 *
	 * @param dbCsvImportDefinition
	 *            the new configuration by definition
	 * @throws Exception
	 *             the exception
	 */
	private void setConfigurationByDefinition(DbCsvImportDefinition dbCsvImportDefinition) throws Exception {
		for (int i = 0; i < dbTypeCombo.getItemCount(); i++) {
			if (DbUtilities.DbVendor.getDbVendorByName(dbTypeCombo.getItemAt(i)) == dbCsvImportDefinition.getDbVendor()) {
				dbTypeCombo.setSelectedIndex(i);
				break;
			}
		}

		hostField.setText(dbCsvImportDefinition.getHostname());
		dbNameField.setText(dbCsvImportDefinition.getDbName());
		userField.setText(dbCsvImportDefinition.getUsername());
		passwordField.setText(dbCsvImportDefinition.getPassword());

		tableNameField.setText(dbCsvImportDefinition.getTableName());
		importFilePathField.setText(dbCsvImportDefinition.getImportFilePath());
		
		for (int i = 0; i < dataTypeCombo.getItemCount(); i++) {
			if (dataTypeCombo.getItemAt(i).equalsIgnoreCase(dbCsvImportDefinition.getDataType().toString())) {
				dataTypeCombo.setSelectedIndex(i);
				break;
			}
		}
		
		fileLogBox.setSelected(dbCsvImportDefinition.isLog());

		boolean encodingFound = false;
		for (int i = 0; i < encodingCombo.getItemCount(); i++) {
			if (encodingCombo.getItemAt(i).equalsIgnoreCase(dbCsvImportDefinition.getEncoding())) {
				encodingCombo.setSelectedIndex(i);
				encodingFound = true;
				break;
			}
		}
		if (!encodingFound) {
			encodingCombo.setSelectedItem(dbCsvImportDefinition.getEncoding());
		}
		
		boolean separatorFound = false;
		for (int i = 0; i < separatorCombo.getItemCount(); i++) {
			if (separatorCombo.getItemAt(i).equalsIgnoreCase(Character.toString(dbCsvImportDefinition.getSeparator()))) {
				separatorCombo.setSelectedIndex(i);
				separatorFound = true;
				break;
			}
		}
		if (!separatorFound) {
			separatorCombo.setSelectedItem(Character.toString(dbCsvImportDefinition.getSeparator()));
		}

		boolean stringQuoteFound = false;
		for (int i = 0; i < stringQuoteCombo.getItemCount(); i++) {
			if (stringQuoteCombo.getItemAt(i).equalsIgnoreCase(Character.toString(dbCsvImportDefinition.getStringQuote()))) {
				stringQuoteCombo.setSelectedIndex(i);
				stringQuoteFound = true;
				break;
			}
		}
		if (!stringQuoteFound) {
			stringQuoteCombo.setSelectedItem(Character.toString(dbCsvImportDefinition.getStringQuote()));
		}
		
		noHeadersBox.setSelected(dbCsvImportDefinition.isNoHeaders());
		
		boolean nullValueStringFound = false;
		for (int i = 0; i < nullValueStringCombo.getItemCount(); i++) {
			if (nullValueStringCombo.getItemAt(i).equalsIgnoreCase(dbCsvImportDefinition.getNullValueString())) {
				nullValueStringCombo.setSelectedIndex(i);
				nullValueStringFound = true;
				break;
			}
		}
		if (!nullValueStringFound) {
			nullValueStringCombo.setSelectedItem(dbCsvImportDefinition.getNullValueString());
		}
		
		onlyCommitOnFullSuccessBox.setSelected(dbCsvImportDefinition.isCompleteCommit());
		allowUnderfilledLinesBox.setSelected(dbCsvImportDefinition.isAllowUnderfilledLines());
		
		boolean importModeFound = false;
		for (int i = 0; i < importModeCombo.getItemCount(); i++) {
			if (importModeCombo.getItemAt(i).equalsIgnoreCase(dbCsvImportDefinition.getImportmode().toString())) {
				importModeCombo.setSelectedIndex(i);
				importModeFound = true;
				break;
			}
		}
		if (!importModeFound) {
			throw new Exception("Invalid import mode");
		}
		
		updateWithNullDataBox.setSelected(dbCsvImportDefinition.isUpdateNullData());
		
		keyColumnsField.setText(Utilities.join(dbCsvImportDefinition.getKeycolumns(), ", "));
		
		createTableBox.setSelected(dbCsvImportDefinition.isCreateTable());

		mappingField.setText(dbCsvImportDefinition.getMapping());
		
		additionalInsertValuesField.setText(dbCsvImportDefinition.getAdditionalInsertValues());
		
		additionalUpdateValuesField.setText(dbCsvImportDefinition.getAdditionalUpdateValues());
		
		checkButtonStatus();
	}

	/**
	 * Check button status.
	 */
	private void checkButtonStatus() {
		if (DbVendor.SQLite.toString().equalsIgnoreCase((String) dbTypeCombo.getSelectedItem())
				|| DbVendor.Derby.toString().equalsIgnoreCase((String) dbTypeCombo.getSelectedItem())) {
			hostField.setEnabled(false);
			userField.setEnabled(false);
			passwordField.setEnabled(false);
		} else {
			hostField.setEnabled(true);
			userField.setEnabled(true);
			passwordField.setEnabled(true);
		}

		if (DataType.CSV.toString().equalsIgnoreCase((String) dataTypeCombo.getSelectedItem())) {
			separatorCombo.setEnabled(true);
			stringQuoteCombo.setEnabled(true);
			noHeadersBox.setEnabled(true);
			nullValueStringCombo.setEnabled(true);
			allowUnderfilledLinesBox.setEnabled(true);
		} else if (DataType.JSON.toString().equalsIgnoreCase((String) dataTypeCombo.getSelectedItem())) {
			separatorCombo.setEnabled(false);
			stringQuoteCombo.setEnabled(false);
			noHeadersBox.setEnabled(false);
			nullValueStringCombo.setEnabled(false);
			allowUnderfilledLinesBox.setEnabled(false);
		} else if (DataType.XML.toString().equalsIgnoreCase((String) dataTypeCombo.getSelectedItem())) {
			separatorCombo.setEnabled(false);
			stringQuoteCombo.setEnabled(false);
			noHeadersBox.setEnabled(false);
			nullValueStringCombo.setEnabled(true);
			allowUnderfilledLinesBox.setEnabled(false);
		}
		
		additionalInsertValuesField.setEnabled(
			ImportMode.CLEARINSERT.toString().equalsIgnoreCase((String) importModeCombo.getSelectedItem())
			|| ImportMode.INSERT.toString().equalsIgnoreCase((String) importModeCombo.getSelectedItem())
			|| ImportMode.UPSERT.toString().equalsIgnoreCase((String) importModeCombo.getSelectedItem()));
		
		additionalUpdateValuesField.setEnabled(
			ImportMode.UPDATE.toString().equalsIgnoreCase((String) importModeCombo.getSelectedItem())
			|| ImportMode.UPSERT.toString().equalsIgnoreCase((String) importModeCombo.getSelectedItem()));
	}

	/**
	 * Import.
	 *
	 * @param dbCsvImportDefinition
	 *            the db csv import definition
	 * @param dbCsvImportGui
	 *            the db csv import gui
	 */
	private void importData(DbCsvImportDefinition dbCsvImportDefinition, final DbCsvImportGui dbCsvImportGui) {
		try {
			dbCsvImportDefinition.checkParameters();
			if (!new DbCsvImportDriverSupplier(this, dbCsvImportDefinition.getDbVendor()).supplyDriver()) {
				throw new Exception("Cannot aquire db driver for db vendor: " + dbCsvImportDefinition.getDbVendor());
			}
			
			// The worker parent is set later by the opened DualProgressDialog
			AbstractDbImportWorker worker = dbCsvImportDefinition.getConfiguredWorker(null);

			SimpleProgressDialog<AbstractDbImportWorker> progressDialog = new SimpleProgressDialog<AbstractDbImportWorker>(this, DbCsvImport.APPLICATION_NAME, worker);
			Result result = progressDialog.showDialog();
			
			if (result == Result.CANCELED) {
				new TextDialog(dbCsvImportGui, DbCsvImport.APPLICATION_NAME, LangResources.get("error.canceledbyuser"), LangResources.get("close"), Color.YELLOW).setVisible(true);
			} else if (result == Result.ERROR) {
				Exception e = progressDialog.getWorker().getError();
				if (e instanceof DbCsvImportException) {
					TextDialog textDialog = new TextDialog(dbCsvImportGui, DbCsvImport.APPLICATION_NAME + " ERROR", "ERROR:\n" + ((DbCsvImportException) e).getMessage(), LangResources.get("close"), Color.PINK);
					textDialog.setVisible(true);
				} else {
					String stacktrace = ExceptionUtilities.getStackTrace(e);
					new TextDialog(dbCsvImportGui, DbCsvImport.APPLICATION_NAME + " ERROR", "ERROR:\n" + e.getClass().getSimpleName() + ":\n" + e.getMessage() + "\n\n" + stacktrace, LangResources.get("close"), false, Color.PINK).setVisible(true);
				}
			} else {
				Date start = worker.getStartTime();
				Date end = worker.getEndTime();

				String resultText = LangResources.get("start") + ": " + start + "\n"
					+ LangResources.get("end") + ": " + end + "\n"
					+ LangResources.get("timeelapsed") + ": " + DateUtilities.getHumanReadableTimespan(end.getTime() - start.getTime(), true);

				resultText += "\n" + worker.getResultStatistics();

				Color backgroundColor = Color.WHITE;
				if (worker.getNotImportedItems().size() > 0) {
					backgroundColor = Color.PINK;
				}
				new TextDialog(dbCsvImportGui, DbCsvImport.APPLICATION_NAME, LangResources.get("result") + ":\n" + resultText, LangResources.get("close"), false, backgroundColor).setVisible(true);
			}
		} catch (Exception e) {
			new TextDialog(dbCsvImportGui, DbCsvImport.APPLICATION_NAME + " ERROR", "ERROR:\n" + e.getMessage(), LangResources.get("close"), Color.PINK).setVisible(true);
		}
	}
	
	private File selectImportFile(String basePath) {
		if (Utilities.isBlank(basePath)) {
			basePath = System.getProperty("user.home");
		} else if (basePath.contains(File.separator)) {
			basePath = basePath.substring(0, basePath.lastIndexOf(File.separator));
		}
		
		JFileChooser fileChooser = new JFileChooser(basePath);
		fileChooser.setDialogTitle(DbCsvImport.APPLICATION_NAME + " " + LangResources.get("importFilePath"));
		if (JFileChooser.APPROVE_OPTION == fileChooser.showOpenDialog(this)) {
			return fileChooser.getSelectedFile();
		} else {
			return null;
		}
	}
}