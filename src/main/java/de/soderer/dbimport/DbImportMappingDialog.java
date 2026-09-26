package de.soderer.dbimport;

import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.Label;
import java.awt.Window;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import javax.swing.Box;
import javax.swing.BoxLayout;
import javax.swing.JButton;
import javax.swing.JPanel;
import javax.swing.JScrollPane;

import de.soderer.utilities.LangResources;
import de.soderer.utilities.Triple;
import de.soderer.utilities.Tuple;
import de.soderer.utilities.Utilities;
import de.soderer.utilities.db.data.DbColumnType;
import de.soderer.utilities.db.data.DbSimpleDataType;
import de.soderer.utilities.db.utilities.CaseInsensitiveMap;
import de.soderer.utilities.swing.DropDown;
import de.soderer.utilities.swing.ModalDialog;

public class DbImportMappingDialog extends ModalDialog<Boolean> {
	private static final long serialVersionUID = 396542497082344683L;

	private final CaseInsensitiveMap<DbColumnType> columnTypes;
	private final List<String> dataColumns;
	private String mappingString;

	private final List<Triple<Label, DropDown, DropDown>> mappingEntries = new ArrayList<>();

	/** Not final, so the DropDown listeners (lambdas) may read it before it is created */
	private final JButton okButton;

	public DbImportMappingDialog(final Window parent, final String title, final CaseInsensitiveMap<DbColumnType> columnTypes, final List<String> dataColumns, final List<String> keyColumns) throws Exception {
		super(parent, title);

		this.columnTypes = columnTypes;
		this.dataColumns = dataColumns;

		setResizable(false);

		final JPanel panel = new JPanel();
		panel.setLayout(new BoxLayout(panel, BoxLayout.PAGE_AXIS));

		add(panel);

		panel.add(Box.createRigidArea(new Dimension(0, 5)));

		final JPanel mappingPanel = new JPanel();
		mappingPanel.setLayout(new BoxLayout(mappingPanel, BoxLayout.PAGE_AXIS));

		final JPanel mappingEntryPanelHeader = new JPanel(new FlowLayout(FlowLayout.LEFT));

		mappingEntryPanelHeader.add(Box.createRigidArea(new Dimension(5, 0)));

		final Label dbColumnLabelHeader1 = new Label("DB-Column");
		dbColumnLabelHeader1.setPreferredSize(new Dimension(200, 18));
		mappingEntryPanelHeader.add(dbColumnLabelHeader1);

		mappingEntryPanelHeader.add(Box.createRigidArea(new Dimension(5, 0)));

		final Label dbColumnLabelHeader2 = new Label("Data-Column");
		dbColumnLabelHeader2.setPreferredSize(new Dimension(130, 18));
		mappingEntryPanelHeader.add(dbColumnLabelHeader2);

		mappingEntryPanelHeader.add(Box.createRigidArea(new Dimension(5, 0)));

		final Label dbColumnLabelHeader3 = new Label("Formatinfo");
		mappingEntryPanelHeader.add(dbColumnLabelHeader3);

		mappingEntryPanelHeader.add(Box.createRigidArea(new Dimension(5, 0)));
		mappingPanel.add(mappingEntryPanelHeader);

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

			final JPanel mappingEntryPanel = new JPanel(new FlowLayout(FlowLayout.LEFT));

			mappingEntryPanel.add(Box.createRigidArea(new Dimension(5, 0)));

			final Label dbColumnLabel = new Label(dbColumnName);
			dbColumnLabel.setPreferredSize(new Dimension(200, 18));
			mappingEntryPanel.add(dbColumnLabel);

			mappingEntryPanel.add(Box.createRigidArea(new Dimension(5, 0)));

			// Fixed list: no custom values (like the former non-editable combo box), empty entry means "not mapped"
			final DropDown dataFieldCombo = createFixedDropDown();
			dataFieldCombo.addItem("");
			for (final String dataColumn : dataColumns) {
				dataFieldCombo.addItem(dataColumn);
			}
			// The former combo box preselected the first item automatically, DropDown does not
			dataFieldCombo.setText("");
			mappingEntryPanel.add(dataFieldCombo);

			mappingEntryPanel.add(Box.createRigidArea(new Dimension(5, 0)));

			DropDown optionalComboBox = null;
			if (dbColumnType.getSimpleDataType() == DbSimpleDataType.Float
					|| dbColumnType.getSimpleDataType() == DbSimpleDataType.Integer
					|| dbColumnType.getSimpleDataType() == DbSimpleDataType.BigInteger) {
				optionalComboBox = createFixedDropDown();
				optionalComboBox.addItem(".");
				optionalComboBox.addItem(",");
				optionalComboBox.setText(".");
				mappingEntryPanel.add(optionalComboBox);
			} else if (dbColumnType.getSimpleDataType() == DbSimpleDataType.DateTime) {
				optionalComboBox = createDateFormatDropDown();
				mappingEntryPanel.add(optionalComboBox);
			} else if (dbColumnType.getSimpleDataType() == DbSimpleDataType.Date) {
				optionalComboBox = createDateFormatDropDown();
				mappingEntryPanel.add(optionalComboBox);
			} else if (dbColumnType.getSimpleDataType() == DbSimpleDataType.Blob || dbColumnType.getSimpleDataType() == DbSimpleDataType.Clob) {
				optionalComboBox = createFixedDropDown();
				optionalComboBox.addItem("");
				optionalComboBox.addItem("file");
				optionalComboBox.setText("");
				mappingEntryPanel.add(optionalComboBox);
			} else if (dbColumnType.getSimpleDataType() == DbSimpleDataType.String) {
				optionalComboBox = createFixedDropDown();
				optionalComboBox.addItem("");
				optionalComboBox.addItem("LowerCase");
				optionalComboBox.addItem("UpperCase");
				optionalComboBox.setText("email".equalsIgnoreCase(dbColumnName) ? "LowerCase" : "");
				mappingEntryPanel.add(optionalComboBox);
			} else if (dbColumnType.getSimpleDataType() == DbSimpleDataType.Boolean) {
				optionalComboBox = createFixedDropDown();
				optionalComboBox.addItem("");
				optionalComboBox.setText("");
				mappingEntryPanel.add(optionalComboBox);
			}

			mappingPanel.add(mappingEntryPanel);

			mappingEntryPanel.add(Box.createRigidArea(new Dimension(5, 0)));

			mappingEntries.add(new Triple<>(dbColumnLabel, dataFieldCombo, optionalComboBox));
		}

		final JScrollPane mappingScrollpane = new JScrollPane(mappingPanel);
		mappingScrollpane.setPreferredSize(new Dimension(533, Utilities.limitValue(100, mappingPanel.getPreferredSize().height, 500)));

		panel.add(mappingScrollpane);

		panel.add(Box.createRigidArea(new Dimension(0, 5)));

		final JPanel buttonPanel = new JPanel();
		panel.add(buttonPanel);

		buttonPanel.add(Box.createRigidArea(new Dimension(5, 0)));

		okButton = new JButton(LangResources.get("ok"));
		okButton.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(final ActionEvent event) {
				createMappingString();
				dispose();
			}
		});
		buttonPanel.add(okButton);
		updateOkButtonStatus();

		buttonPanel.add(Box.createRigidArea(new Dimension(5, 0)));

		final JButton cancelButton = new JButton(LangResources.get("cancel"));
		cancelButton.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(final ActionEvent event) {
				dispose();
			}
		});
		buttonPanel.add(cancelButton);

		buttonPanel.add(Box.createRigidArea(new Dimension(5, 0)));

		panel.add(Box.createRigidArea(new Dimension(0, 5)));

		pack();

		setLocationRelativeTo(parent);

		returnValue = true;
	}

	private void fillMappingEntries(final CaseInsensitiveMap<DbColumnType> columnTypesToUse, final List<String> dataColumnsToUse) throws IOException, Exception {
		Map<String, Tuple<String, String>> mapping;
		if (Utilities.isNotBlank(mappingString)) {
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

		for (final Triple<Label, DropDown, DropDown> mappingEntry : mappingEntries) {
			for (final Entry<String, Tuple<String, String>> entry : mapping.entrySet()) {
				if (entry.getKey().equalsIgnoreCase(mappingEntry.getFirst().getText())) {
					// Unknown values are ignored for fixed lists, as the former combo box did
					selectItem(mappingEntry.getSecond(), entry.getValue().getFirst());
					if (mappingEntry.getThird() != null && Utilities.isNotBlank(entry.getValue().getSecond())) {
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

		for (final Triple<Label, DropDown, DropDown> mappingEntry : mappingEntries) {
			final String dataColumn = getValue(mappingEntry.getSecond());
			if (Utilities.isNotBlank(dataColumn)) {
				mappingString += mappingEntry.getFirst().getText() + "=\"" + dataColumn + "\"";
				final String formatValue = mappingEntry.getThird() == null ? null : getValue(mappingEntry.getThird());
				if (Utilities.isNotBlank(formatValue)) {
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
		for (final Triple<Label, DropDown, DropDown> mappingEntry : mappingEntries) {
			if (getValue(mappingEntry.getSecond()) == null || (mappingEntry.getThird() != null && getValue(mappingEntry.getThird()) == null)) {
				allValid = false;
				break;
			}
		}
		okButton.setEnabled(allValid);
	}

	public void setMappingString(final String mappingString) throws Exception {
		this.mappingString = mappingString;
		fillMappingEntries(columnTypes, dataColumns);
	}

	public String getMappingString() {
		return mappingString;
	}

	/**
	 * Mapping Map contains dbColumn as key, csvFileColumn as valueTuples first and formatString as valeTuples second
	 *
	 * @param mappingString
	 * @return
	 * @throws IOException
	 * @throws Exception
	 */
	public static Map<String, Tuple<String, String>> parseMappingString(final String mappingString) throws IOException, Exception {
		final Map<String, Tuple<String, String>> mapping = new HashMap<>();
		final List<String> mappingLines = Utilities.splitAndTrimList(mappingString, ';', '\n', '\r');
		for (final String mappingLine : mappingLines) {
			final int dbColumnEnd = mappingLine.indexOf("=");
			if (dbColumnEnd <= 0) {
				throw new DbImportException("Invalid mapping line: " + mappingLine);
			}
			final String dbColumn = mappingLine.substring(0, dbColumnEnd).toLowerCase().trim();
			if (mapping.containsKey(dbColumn)) {
				throw new DbImportException("Invalid mapping line with duplicate database column: " + mappingLine);
			}
			String rest = mappingLine.substring(dbColumnEnd + 1).trim();
			if (Utilities.isNotBlank(rest)) {
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
				} else {
					throw new DbImportException("Invalid mapping line: " + mappingLine);
				}
			}
		}
		return mapping;
	}
}
