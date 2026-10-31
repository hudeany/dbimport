package de.soderer.dbimport;

import java.awt.Dimension;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.Window;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import javax.swing.BorderFactory;
import javax.swing.Box;
import de.soderer.swing.modern.Button;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;

import de.soderer.swing.modern.ButtonStyle;
import de.soderer.swing.modern.DropDown;
import de.soderer.swing.modern.IconKind;
import de.soderer.swing.modern.ModalDialog;
import de.soderer.swing.modern.ModernUi;
import de.soderer.utilities.LangResources;
import de.soderer.utilities.StringSplitter;
import de.soderer.utilities.StringUtilities;
import de.soderer.utilities.Triple;
import de.soderer.utilities.Tuple;
import de.soderer.utilities.Utilities;
import de.soderer.utilities.db.data.DbColumnType;
import de.soderer.utilities.db.data.DbSimpleDataType;
import de.soderer.utilities.db.utilities.CaseInsensitiveMap;

/**
 * Dialog to edit the mapping of the database columns to the data properties and their format.
 *
 * @serial exclude
 */
public class DbImportMappingDialog extends ModalDialog<Boolean> {
	private static final long serialVersionUID = 396542497082344683L;

	/** Width of the database column names */
	private static final int DB_COLUMN_WIDTH = 200;
	/** Width of the data property selection */
	private static final int DATA_COLUMN_WIDTH = 190;
	/** Width of the format selection */
	private static final int FORMAT_WIDTH = 150;

	private final CaseInsensitiveMap<DbColumnType> columnTypes;
	private final List<String> dataColumns;
	private String mappingString;

	private final List<Triple<JLabel, DropDown, DropDown>> mappingEntries = new ArrayList<>();

	/** Only enabled while all mapping entries are valid */
	private final Button okButton;

	/**
	 * Creates the dialog.
	 *
	 * @param parent the parent window
	 * @param title the dialog title
	 * @param columnTypes the columns of the destination table and their data types
	 * @param dataColumns the property names of the import data
	 * @param keyColumns the key columns of the import, which are listed first, or null
	 * @throws Exception if the dialog cannot be created
	 */
	public DbImportMappingDialog(final Window parent, final String title, final CaseInsensitiveMap<DbColumnType> columnTypes, final List<String> dataColumns, final List<String> keyColumns) throws Exception {
		super(parent, title, IconKind.EDIT);

		this.columnTypes = columnTypes;
		this.dataColumns = dataColumns;

		setResizable(false);

		// Grid with one row per database column: column name, data property and optional format
		final JPanel mappingPanel = new JPanel(new GridBagLayout());
		mappingPanel.setBackground(ModernUi.CARD);
		mappingPanel.setBorder(BorderFactory.createEmptyBorder(0, 0, 0, 6));

		int row = 0;
		addCell(mappingPanel, createHeaderLabel("DB-Column"), 0, row, false);
		addCell(mappingPanel, createHeaderLabel("Data-Column"), 1, row, false);
		addCell(mappingPanel, createHeaderLabel("Formatinfo"), 2, row, false);
		row++;

		final List<String> dbColumnNames = new ArrayList<>(columnTypes.keySet());
		Collections.sort(dbColumnNames);
		if (keyColumns != null) {
			for (final String keyColumn : keyColumns) {
				final boolean wasIncluded = dbColumnNames.remove(keyColumn);
				if (wasIncluded) {
					dbColumnNames.add(0, keyColumn);
				}
			}
		}

		for (final String dbColumnName : dbColumnNames) {
			final DbColumnType dbColumnType = columnTypes.get(dbColumnName);

			final JLabel dbColumnLabel = new JLabel(dbColumnName);
			dbColumnLabel.setForeground(ModernUi.TEXT);
			dbColumnLabel.setPreferredSize(new Dimension(DB_COLUMN_WIDTH, dbColumnLabel.getPreferredSize().height));
			if (keyColumns != null && keyColumns.contains(dbColumnName)) {
				// Key columns are listed first and marked by a bold name
				dbColumnLabel.setFont(dbColumnLabel.getFont().deriveFont(java.awt.Font.BOLD));
			}
			addCell(mappingPanel, dbColumnLabel, 0, row, false);

			// Fixed list: no custom values (like the former non-editable combo box), empty entry means "not mapped"
			final DropDown dataFieldCombo = createFixedDropDown();
			dataFieldCombo.addItem("");
			for (final String dataColumn : dataColumns) {
				dataFieldCombo.addItem(dataColumn);
			}
			// The former combo box preselected the first item automatically, DropDown does not
			dataFieldCombo.setText("");
			dataFieldCombo.setPreferredSize(new Dimension(DATA_COLUMN_WIDTH, dataFieldCombo.getPreferredSize().height));
			addCell(mappingPanel, dataFieldCombo, 1, row, true);

			DropDown optionalComboBox = null;
			if (dbColumnType.getSimpleDataType() == DbSimpleDataType.Float
					|| dbColumnType.getSimpleDataType() == DbSimpleDataType.Integer
					|| dbColumnType.getSimpleDataType() == DbSimpleDataType.BigInteger) {
				optionalComboBox = createFixedDropDown();
				optionalComboBox.addItem(".");
				optionalComboBox.addItem(",");
				optionalComboBox.setText(".");
			} else if (dbColumnType.getSimpleDataType() == DbSimpleDataType.DateTime) {
				optionalComboBox = createDateFormatDropDown();
			} else if (dbColumnType.getSimpleDataType() == DbSimpleDataType.Date) {
				optionalComboBox = createDateFormatDropDown();
			} else if (dbColumnType.getSimpleDataType() == DbSimpleDataType.Blob || dbColumnType.getSimpleDataType() == DbSimpleDataType.Clob) {
				optionalComboBox = createFixedDropDown();
				optionalComboBox.addItem("");
				optionalComboBox.addItem("file");
				optionalComboBox.setText("");
			} else if (dbColumnType.getSimpleDataType() == DbSimpleDataType.String) {
				optionalComboBox = createFixedDropDown();
				optionalComboBox.addItem("");
				optionalComboBox.addItem("LowerCase");
				optionalComboBox.addItem("UpperCase");
				optionalComboBox.setText("email".equalsIgnoreCase(dbColumnName) ? "LowerCase" : "");
			} else if (dbColumnType.getSimpleDataType() == DbSimpleDataType.Boolean) {
				optionalComboBox = createFixedDropDown();
				optionalComboBox.addItem("");
				optionalComboBox.setText("");
			}
			if (optionalComboBox != null) {
				optionalComboBox.setPreferredSize(new Dimension(FORMAT_WIDTH, optionalComboBox.getPreferredSize().height));
				addCell(mappingPanel, optionalComboBox, 2, row, true);
			}

			mappingEntries.add(new Triple<>(dbColumnLabel, dataFieldCombo, optionalComboBox));
			row++;
		}

		// Keeps the rows at the top
		final GridBagConstraints fillerConstraints = new GridBagConstraints();
		fillerConstraints.gridy = row;
		fillerConstraints.weighty = 1;
		mappingPanel.add(Box.createGlue(), fillerConstraints);

		final JScrollPane mappingScrollpane = new JScrollPane(mappingPanel);
		mappingScrollpane.setBorder(BorderFactory.createEmptyBorder());
		mappingScrollpane.getViewport().setBackground(ModernUi.CARD);
		ModernUi.styleScrollBars(mappingScrollpane, ModernUi.CARD);
		mappingScrollpane.setPreferredSize(new Dimension(mappingPanel.getPreferredSize().width + 16, Utilities.limitValue(100, mappingPanel.getPreferredSize().height, 500)));
		getContentArea().add(mappingScrollpane);

		okButton = addButton(LangResources.get("ok"), null, ButtonStyle.PRIMARY, event -> {
			createMappingString();
			dispose();
		});
		updateOkButtonStatus();
		addButton(LangResources.get("cancel"), null, ButtonStyle.SECONDARY, event -> dispose());
		getRootPane().setDefaultButton(okButton);

		pack();

		setLocationRelativeTo(parent);

		returnValue = true;
	}

	/**
	 * Grey bold column title of the mapping grid.
	 */
	private static JLabel createHeaderLabel(final String text) {
		final JLabel label = ModernUi.createSectionTitle(text);
		label.setForeground(ModernUi.MUTED);
		return label;
	}

	/**
	 * Adds a component to the mapping grid.
	 */
	private static void addCell(final JPanel mappingPanel, final java.awt.Component component, final int column, final int row, final boolean fill) {
		final GridBagConstraints constraints = new GridBagConstraints();
		constraints.gridx = column;
		constraints.gridy = row;
		constraints.anchor = GridBagConstraints.WEST;
		constraints.fill = fill ? GridBagConstraints.HORIZONTAL : GridBagConstraints.NONE;
		constraints.insets = new Insets(row == 0 ? 0 : 3, 0, row == 0 ? 6 : 3, column < 2 ? 10 : 0);
		mappingPanel.add(component, constraints);
	}

	private void fillMappingEntries(final CaseInsensitiveMap<DbColumnType> columnTypesToUse, final List<String> dataColumnsToUse) throws IOException, Exception {
		Map<String, Tuple<String, String>> mapping;
		if (StringUtilities.isNotBlank(mappingString)) {
			mapping = parseMappingString(mappingString);
		} else {
			// Create default mapping
			mapping = new HashMap<>();
			for (final String dbColumn : columnTypesToUse.keySet()) {
				for (final String dataColumn : dataColumnsToUse) {
					if (Utilities.trimSimultaneously(Utilities.trimSimultaneously(dbColumn, "\""), "`").equalsIgnoreCase(dataColumn)) {
						mapping.put(dbColumn, new Tuple<>(dataColumn, ""));
						break;
					}
				}
			}
		}

		for (final Triple<JLabel, DropDown, DropDown> mappingEntry : mappingEntries) {
			for (final Entry<String, Tuple<String, String>> entry : mapping.entrySet()) {
				if (entry.getKey().equalsIgnoreCase(mappingEntry.getFirst().getText())) {
					// Unknown values are ignored for fixed lists, as the former combo box did
					selectItem(mappingEntry.getSecond(), entry.getValue().getFirst());
					if (mappingEntry.getThird() != null && StringUtilities.isNotBlank(entry.getValue().getSecond())) {
						if ("lc".equalsIgnoreCase(entry.getValue().getSecond())) {
							selectItem(mappingEntry.getThird(), "LowerCase");
						} else if ("uc".equalsIgnoreCase(entry.getValue().getSecond())) {
							selectItem(mappingEntry.getThird(), "UpperCase");
						} else {
							selectItem(mappingEntry.getThird(), entry.getValue().getSecond());
						}
					}
					break;
				}
			}
		}
	}

	private void createMappingString() {
		mappingString = "";

		for (final Triple<JLabel, DropDown, DropDown> mappingEntry : mappingEntries) {
			final String dataColumn = getValue(mappingEntry.getSecond());
			if (StringUtilities.isNotBlank(dataColumn)) {
				mappingString += mappingEntry.getFirst().getText() + "=\"" + dataColumn + "\"";
				final String formatValue = mappingEntry.getThird() == null ? null : getValue(mappingEntry.getThird());
				if (StringUtilities.isNotBlank(formatValue)) {
					if ("lowercase".equalsIgnoreCase(formatValue)) {
						mappingString += " lc";
					} else if ("uppercase".equalsIgnoreCase(formatValue)) {
						mappingString += " uc";
					} else {
						mappingString += " " + formatValue;
					}
				}
				mappingString += "\n";
			}
		}
	}

	/**
	 * DropDown with a fixed item list (replaces a non-editable combo box)
	 */
	private DropDown createFixedDropDown() {
		final DropDown dropDown = new DropDown();
		dropDown.setCaseSensitive(false);
		dropDown.setMatchMode(DropDown.MatchMode.CONTAINS);
		dropDown.setAllowCustomValues(false);
		dropDown.addChangeListener(event -> updateOkButtonStatus());
		return dropDown;
	}

	/**
	 * Editable DropDown with common date formats as presets (replaces the former editable combo box).
	 * Case-sensitive, because e.g. "MM" (month) and "mm" (minute) differ in date format patterns.
	 */
	private DropDown createDateFormatDropDown() {
		final DropDown dropDown = new DropDown();
		dropDown.setCaseSensitive(true);
		dropDown.setMatchMode(DropDown.MatchMode.STARTS_WITH);
		dropDown.setAllowCustomValues(true);
		dropDown.addItem("dd.MM.yyyy HH:mm:ss");
		dropDown.addItem("dd.MM.yyyy");
		dropDown.addItem("yyyy/MM/dd HH:mm:ss");
		dropDown.addItem("yyyy/MM/dd");
		dropDown.setText("dd.MM.yyyy HH:mm:ss");
		return dropDown;
	}

	/**
	 * Current value of a DropDown: the typed text for editable ones, the matching item for fixed lists.
	 * Returns null for a fixed list whose text is invalid or only a partial input that was not accepted yet.
	 */
	private static String getValue(final DropDown dropDown) {
		if (dropDown.isAllowCustomValues()) {
			return dropDown.getText();
		}
		final String text = dropDown.getText();
		if (text == null) {
			return null;
		}
		for (final String item : dropDown.getItems()) {
			if (item.equals(text)) {
				return item;
			}
		}
		for (final String item : dropDown.getItems()) {
			if (item.equalsIgnoreCase(text)) {
				return item;
			}
		}
		return null;
	}

	/**
	 * Shows the item matching the given value (exact match preferred, then case-insensitive).
	 * If there is no such item, the value itself is shown, but only if the DropDown allows custom values.
	 */
	private static void selectItem(final DropDown dropDown, final String value) {
		final String valueToSelect = value == null ? "" : value;
		for (final String item : dropDown.getItems()) {
			if (item.equals(valueToSelect)) {
				dropDown.setText(item);
				return;
			}
		}
		for (final String item : dropDown.getItems()) {
			if (item.equalsIgnoreCase(valueToSelect)) {
				dropDown.setText(item);
				return;
			}
		}
		if (dropDown.isAllowCustomValues()) {
			dropDown.setText(valueToSelect);
		}
	}

	/**
	 * OK is only possible while every fixed-list DropDown shows one of its items,
	 * so no mapping gets silently dropped because of an incomplete input
	 */
	private void updateOkButtonStatus() {
		if (okButton == null) {
			// Still building the dialog
			return;
		}
		boolean allValid = true;
		for (final Triple<JLabel, DropDown, DropDown> mappingEntry : mappingEntries) {
			if (getValue(mappingEntry.getSecond()) == null || (mappingEntry.getThird() != null && getValue(mappingEntry.getThird()) == null)) {
				allValid = false;
				break;
			}
		}
		okButton.setEnabled(allValid);
	}

	/**
	 * Sets the mapping to show in the dialog.
	 *
	 * @param mappingString the mapping in the format of {@link #parseMappingString(String)}
	 * @throws Exception if the mapping is invalid
	 */
	public void setMappingString(final String mappingString) throws Exception {
		this.mappingString = mappingString;
		fillMappingEntries(columnTypes, dataColumns);
	}

	/**
	 * Returns the mapping edited in the dialog.
	 *
	 * @return the mapping in the format of {@link #parseMappingString(String)}
	 */
	public String getMappingString() {
		return mappingString;
	}

	/**
	 * Parses a mapping definition.
	 *
	 * <p>
	 * Each mapping line ("dbcolumn = 'dataproperty' formatinfo") is separated by ';' or a line break. The result
	 * contains the database column (lower case) as key, the data property as first and the format info as second
	 * value of the tuple.
	 * </p>
	 *
	 * @param mappingString the mapping definition
	 * @return the mapping by database column
	 * @throws IOException if the mapping definition cannot be read
	 * @throws Exception if a mapping line is invalid
	 */
	public static Map<String, Tuple<String, String>> parseMappingString(final String mappingString) throws IOException, Exception {
		final Map<String, Tuple<String, String>> mapping = new HashMap<>();
		final List<String> mappingLines = StringSplitter.splitAndTrimList(mappingString, ';', '\n', '\r');
		for (final String mappingLine : mappingLines) {
			final int dbColumnEnd = mappingLine.indexOf("=");
			if (dbColumnEnd <= 0) {
				throw new DbImportException("Invalid mapping line: " + mappingLine);
			}
			final String dbColumn = mappingLine.substring(0, dbColumnEnd).toLowerCase(Locale.ROOT).trim();
			if (mapping.containsKey(dbColumn)) {
				throw new DbImportException("Invalid mapping line with duplicate database column: " + mappingLine);
			}
			String rest = mappingLine.substring(dbColumnEnd + 1).trim();
			if (StringUtilities.isNotBlank(rest)) {
				if (rest.length() < 2 || (!rest.startsWith("\"") && !rest.startsWith("'"))) {
					throw new DbImportException("Invalid mapping line: " + mappingLine);
				}
				final int dataColumnEnd = rest.indexOf(rest.charAt(0), 1);
				if (dataColumnEnd <= 0) {
					throw new DbImportException("Invalid mapping line: " + mappingLine);
				}
				final String dataColumn = rest.substring(1, dataColumnEnd);
				rest = rest.substring(dataColumnEnd + 1).trim();
				if ("".equals(rest)) {
					mapping.put(dbColumn, new Tuple<>(dataColumn, ""));
				} else if (".".equals(rest)) {
					mapping.put(dbColumn, new Tuple<>(dataColumn, "."));
				} else if (",".equals(rest)) {
					mapping.put(dbColumn, new Tuple<>(dataColumn, ","));
				} else if ("file".equalsIgnoreCase(rest)) {
					mapping.put(dbColumn, new Tuple<>(dataColumn, "file"));
				} else if (Pattern.matches("[ yYuUmMdDhHsSnNxXzZtT:.'\\[\\]/-]+", rest)) {
					mapping.put(dbColumn, new Tuple<>(dataColumn, rest));
				} else if ("lc".equalsIgnoreCase(rest)) {
					mapping.put(dbColumn, new Tuple<>(dataColumn, "lc"));
				} else if ("uc".equalsIgnoreCase(rest)) {
					mapping.put(dbColumn, new Tuple<>(dataColumn, "uc"));
				} else if ("email".equalsIgnoreCase(rest)) {
					// Documented in help.txt and handled by DbImportWorker, but was rejected here as invalid mapping line
					mapping.put(dbColumn, new Tuple<>(dataColumn, "email"));
				} else {
					throw new DbImportException("Invalid mapping line: " + mappingLine);
				}
			}
		}
		return mapping;
	}
}
