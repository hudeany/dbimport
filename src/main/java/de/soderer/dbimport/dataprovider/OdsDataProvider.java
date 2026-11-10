package de.soderer.dbimport.dataprovider;

import java.io.File;
import java.io.InputStream;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamReader;

import de.soderer.dbimport.DbImportException;
import de.soderer.utilities.DateUtilities;
import de.soderer.utilities.StringUtilities;
import de.soderer.utilities.Tuple;
import de.soderer.utilities.db.data.DbColumnType;
import de.soderer.utilities.db.data.DbSimpleDataType;

/**
 * Data provider for OpenDocument spreadsheet data (ODS format).
 * <p>
 * Repeated cells and rows (attributes "number-columns-repeated" and "number-rows-repeated") are expanded,
 * covered cells of merged areas occupy their column, and the text of a cell is read including formatted parts,
 * links, spaces, tabs and line breaks. Multiple paragraphs of a cell are joined by line breaks.
 * The data starts at the first non empty row and ends at the first empty row after it or at the end of the sheet.
 * <p>
 * With headers, only columns with a header name are imported, data in columns without header is ignored and duplicate header names are rejected.
 */
public class OdsDataProvider extends DataProvider {
	private static final String NAMESPACE_OFFICE = "urn:oasis:names:tc:opendocument:xmlns:office:1.0";
	private static final String NAMESPACE_TABLE = "urn:oasis:names:tc:opendocument:xmlns:table:1.0";
	private static final String NAMESPACE_TEXT = "urn:oasis:names:tc:opendocument:xmlns:text:1.0";

	/** Maximum number of columns of a spreadsheet (LibreOffice and Excel limit), to protect against huge repeat attributes */
	private static final int MAXIMUM_NUMBER_OF_COLUMNS = 16384;
	/** Maximum number of rows of a spreadsheet (LibreOffice and Excel limit) */
	private static final int MAXIMUM_NUMBER_OF_ROWS = 1048576;

	private String sheetname = null;
	private InputStream contentXmlZipInputStream = null;
	private XMLStreamReader xmlReader = null;

	private String nullValueText = null;
	private boolean allowUnderfilledLines = false;
	private boolean noHeaders = false;
	private boolean trimData = false;

	private Integer itemsAmount = null;
	private Map<String, DbColumnType> dataTypes = null;

	private List<String> columnNames = null;
	/** Sheet column positions of the columns in columnNames (only with headers, null for positional columns without headers) */
	private List<Integer> columnIndices = null;
	private int currentRowNumber;

	/** Values of a row with "number-rows-repeated", which still has to be delivered again */
	private List<Object> repeatedRowValues = null;
	/** Number of further deliveries of repeatedRowValues */
	private int repeatedRowCount = 0;
	/** True after the end of the selected sheet was reached, so no rows of the following sheets are read */
	private boolean endOfSheetReached = false;
	/** True after the first non empty row was read, so leading empty rows are skipped and the next empty row ends the data */
	private boolean dataStarted = false;

	/**
	 * Creates the data provider.
	 *
	 * @param importFilePath path of the import file
	 * @param zipPassword password of a zip import file, or null
	 * @param allowUnderfilledLines true to accept rows with fewer values than columns
	 * @param noHeaders true if the first row contains data instead of the column names
	 * @param nullValueText text which represents null values, or null
	 * @param trimData true to trim the values
	 * @param sheetname name of the sheet to import, or null for the first sheet
	 */
	public OdsDataProvider(final String importFilePath, final char[] zipPassword, final boolean allowUnderfilledLines, final boolean noHeaders, final String nullValueText, final boolean trimData, final String sheetname) {
		super(false, importFilePath, zipPassword);
		this.allowUnderfilledLines = allowUnderfilledLines;
		this.noHeaders = noHeaders;
		this.nullValueText = nullValueText;
		this.trimData = trimData;
		this.sheetname = sheetname;
	}

	@Override
	public String getConfigurationLogString() {
		return super.getConfigurationLogString()
				+ "Format: ODS" + "\n"
				+ "AllowUnderfilledLines: " + allowUnderfilledLines + "\n"
				+ "NoHeaders: " + noHeaders + "\n"
				+ "TrimData: " + trimData + "\n"
				+ (StringUtilities.isNotBlank(sheetname) ? "Sheetname: " + sheetname + "\n" : "")
				+ "Null value text: " + (nullValueText == null ? "none" : "\"" + nullValueText + "\"") + "\n";
	}

	@Override
	public Map<String, DbColumnType> scanDataPropertyTypes(final Map<String, Tuple<String, String>> mapping) throws Exception {
		if (dataTypes == null) {
			if (xmlReader == null) {
				openReader();
			}
			currentRowNumber = 0;

			if (!noHeaders) {
				columnNames = readHeaderRow();
			}

			dataTypes = new HashMap<>();

			// Scan all data for maximum
			List<Object> values;
			while ((values = readNextRow()) != null) {
				final int numberOfColumns = columnIndices != null ? columnIndices.size() : values.size();
				for (int i = 0; i < numberOfColumns; i++) {
					final String columnName;
					final Object currentValue;
					if (columnIndices != null) {
						columnName = columnNames.get(i);
						currentValue = columnIndices.get(i) < values.size() ? values.get(columnIndices.get(i)) : null;
					} else {
						columnName = (columnNames == null || columnNames.size() <= i ? "column_" + Integer.toString(i + 1) : columnNames.get(i));
						currentValue = values.get(i);
					}
					if (currentValue instanceof String) {
						detectNextDataType(mapping, dataTypes, columnName, (String) currentValue);
					} else if (currentValue != null) {
						detectNextTypedDataType(dataTypes, columnName, currentValue);
					}
				}
			}

			close();
		}

		return dataTypes;
	}

	/**
	 * Detects the data type of a column for values, which are already typed in the ODS data (numbers, dates and booleans).
	 * Values of different kinds in one column result in a VARCHAR column, string and blob columns are kept.
	 *
	 * @param dataTypes the data types detected so far
	 * @param columnName name of the column
	 * @param value the typed value
	 */
	private static void detectNextTypedDataType(final Map<String, DbColumnType> dataTypes, final String columnName, final Object value) {
		final DbColumnType currentColumnType = dataTypes.get(columnName);
		final DbSimpleDataType currentType = currentColumnType == null ? null : currentColumnType.getSimpleDataType();

		if (currentType == DbSimpleDataType.Blob) {
			return;
		} else if (currentType == DbSimpleDataType.String) {
			// Keep the string column, but make sure the value fits into it
			dataTypes.put(columnName, createVarcharType(currentColumnType, value));
		} else if (value instanceof Double) {
			final double doubleValue = (Double) value;
			final boolean isIntegerValue = doubleValue == Math.rint(doubleValue) && Math.abs(doubleValue) <= Integer.MAX_VALUE;
			if ((currentType == null || currentType == DbSimpleDataType.Integer) && isIntegerValue) {
				dataTypes.put(columnName, new DbColumnType("INTEGER", -1, -1, -1, true, false, null));
			} else if (currentType == null || currentType == DbSimpleDataType.Integer || currentType == DbSimpleDataType.BigInteger || currentType == DbSimpleDataType.Float) {
				dataTypes.put(columnName, new DbColumnType("DOUBLE", -1, -1, -1, true, false, null));
			} else {
				dataTypes.put(columnName, createVarcharType(currentColumnType, value));
			}
		} else if (value instanceof ZonedDateTime) {
			final boolean isDateOnly = ((ZonedDateTime) value).toLocalTime().equals(LocalTime.MIDNIGHT);
			if ((currentType == null || currentType == DbSimpleDataType.Date) && isDateOnly) {
				dataTypes.put(columnName, new DbColumnType("DATE", -1, -1, -1, true, false, null));
			} else if (currentType == null || currentType == DbSimpleDataType.Date || currentType == DbSimpleDataType.DateTime) {
				dataTypes.put(columnName, new DbColumnType("TIMESTAMP", -1, -1, -1, true, false, null));
			} else {
				dataTypes.put(columnName, createVarcharType(currentColumnType, value));
			}
		} else if (value instanceof Boolean) {
			if (currentType == null || (currentType != DbSimpleDataType.Float && currentType != DbSimpleDataType.Integer && currentType != DbSimpleDataType.BigInteger && currentType != DbSimpleDataType.Date && currentType != DbSimpleDataType.DateTime)) {
				dataTypes.put(columnName, new DbColumnType("BOOLEAN", -1, -1, -1, true, false, null));
			} else {
				dataTypes.put(columnName, createVarcharType(currentColumnType, value));
			}
		} else {
			dataTypes.put(columnName, createVarcharType(currentColumnType, value));
		}
	}

	/**
	 * Creates a VARCHAR column type, which is large enough for the current column type and the given value.
	 */
	private static DbColumnType createVarcharType(final DbColumnType currentColumnType, final Object value) {
		final int valueByteSize = value.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
		final boolean isCurrentVarchar = currentColumnType != null && currentColumnType.getSimpleDataType() == DbSimpleDataType.String;
		return new DbColumnType("VARCHAR", Math.max(isCurrentVarchar ? currentColumnType.getCharacterByteSize() : 0, valueByteSize), -1, -1, true, false, null);
	}

	@Override
	public List<String> getAvailableDataPropertyNames() throws Exception {
		if (columnNames == null) {
			if (xmlReader == null) {
				openReader();
			}
			currentRowNumber = 0;

			columnNames = new ArrayList<>();
			if (noHeaders) {
				final List<String> returnList = new ArrayList<>();
				if (allowUnderfilledLines) {
					// Scan all data for maximum
					List<Object> values;
					int maxColumns = 0;
					while ((values = readNextRow()) != null) {
						maxColumns = Math.max(maxColumns, values.size());
					}
					for (int i = 0; i < maxColumns; i++) {
						returnList.add("column_" + Integer.toString(i + 1));
					}
				} else {
					// Only take first data as example for all other data
					final List<Object> values = readNextRow();
					if (values != null) {
						for (int i = 0; i < values.size(); i++) {
							returnList.add("column_" + Integer.toString(i + 1));
						}
					}
				}
				columnNames = returnList;
			} else {
				columnNames = readHeaderRow();
			}

			close();
		}

		return columnNames;
	}

	/**
	 * Reads the header row with the column names. Only columns with a non blank header name are used, their sheet positions are stored in columnIndices.
	 *
	 * @return the column names, empty if there is no data at all
	 * @throws Exception if the XML data cannot be read or a header name is duplicate
	 */
	private List<String> readHeaderRow() throws Exception {
		final List<String> headerNames = new ArrayList<>();
		final List<Integer> headerColumnIndices = new ArrayList<>();

		final List<Object> headerValues = readNextRow();
		if (headerValues != null) {
			final Map<String, Integer> columnIndexByName = new HashMap<>();
			for (int columnIndex = 0; columnIndex < headerValues.size(); columnIndex++) {
				final String headerName = headerValues.get(columnIndex) == null ? "" : headerValues.get(columnIndex).toString();
				if (StringUtilities.isNotBlank(headerName)) {
					final Integer previousColumnIndex = columnIndexByName.put(headerName, columnIndex);
					if (previousColumnIndex != null) {
						throw new DbImportException("Duplicate column header '" + headerName + "' in columns " + ExcelDataProvider.columnIndexToLetters(previousColumnIndex) + " and " + ExcelDataProvider.columnIndexToLetters(columnIndex));
					}
					headerNames.add(headerName);
					headerColumnIndices.add(columnIndex);
				}
			}
		}

		columnIndices = headerColumnIndices;
		return headerNames;
	}

	/**
	 * Reads the values of the next row of the selected sheet.
	 * <p>
	 * Empty cells are delivered as empty text, empty trailing cells are left out.
	 *
	 * @return the values of the next row, or null at the first empty row after the data or at the end of the sheet
	 * @throws Exception if the XML data cannot be read
	 */
	private List<Object> readNextRow() throws Exception {
		if (repeatedRowCount > 0) {
			repeatedRowCount--;
			currentRowNumber++;
			return new ArrayList<>(repeatedRowValues);
		}

		if (endOfSheetReached) {
			return null;
		}

		while (xmlReader.hasNext()) {
			final int eventType = xmlReader.next();
			if (eventType == XMLStreamConstants.START_ELEMENT && isElement(NAMESPACE_TABLE, "table-row")) {
				final int rowsRepeated = getRepeatAttribute("number-rows-repeated", MAXIMUM_NUMBER_OF_ROWS);
				final List<Object> rowValues = readRowCells();
				currentRowNumber++;

				if (rowValues.isEmpty()) {
					if (!dataStarted) {
						// Skip empty rows before the data
						continue;
					}
					// Empty row: end of data (also the typical huge repeated empty row at the end of a sheet)
					endOfSheetReached = true;
					return null;
				}
				dataStarted = true;

				if (rowsRepeated > 1) {
					repeatedRowValues = Collections.unmodifiableList(new ArrayList<>(rowValues));
					repeatedRowCount = rowsRepeated - 1;
				}
				return rowValues;
			} else if (eventType == XMLStreamConstants.END_ELEMENT && isElement(NAMESPACE_TABLE, "table")) {
				endOfSheetReached = true;
				return null;
			}
		}

		endOfSheetReached = true;
		return null;
	}

	/**
	 * Reads all cells of the current row. The reader is positioned at the start element of the row and afterwards at its end element.
	 *
	 * @return the cell values without empty trailing cells
	 * @throws Exception if the XML data cannot be read
	 */
	private List<Object> readRowCells() throws Exception {
		final List<Object> rowValues = new ArrayList<>();
		// Empty cells are only added, when a non empty cell follows, so empty trailing cells (often thousands) are never created
		int pendingEmptyCells = 0;

		while (xmlReader.hasNext()) {
			final int eventType = xmlReader.next();
			if (eventType == XMLStreamConstants.START_ELEMENT && (isElement(NAMESPACE_TABLE, "table-cell") || isElement(NAMESPACE_TABLE, "covered-table-cell"))) {
				final int columnsRepeated = getRepeatAttribute("number-columns-repeated", MAXIMUM_NUMBER_OF_COLUMNS);
				final Object cellValue = readCellValue();

				if (cellValue == null || "".equals(cellValue)) {
					pendingEmptyCells = Math.min(pendingEmptyCells + columnsRepeated, MAXIMUM_NUMBER_OF_COLUMNS);
				} else {
					for (int i = 0; i < pendingEmptyCells && rowValues.size() < MAXIMUM_NUMBER_OF_COLUMNS; i++) {
						rowValues.add("");
					}
					pendingEmptyCells = 0;
					for (int i = 0; i < columnsRepeated && rowValues.size() < MAXIMUM_NUMBER_OF_COLUMNS; i++) {
						rowValues.add(cellValue);
					}
				}
			} else if (eventType == XMLStreamConstants.END_ELEMENT && isElement(NAMESPACE_TABLE, "table-row")) {
				break;
			}
		}

		return rowValues;
	}

	/**
	 * Reads the value of the current cell. The reader is positioned at the start element of the cell and afterwards at its end element.
	 *
	 * @return the typed value (Double, ZonedDateTime, Boolean) or the text of the cell (empty text for empty cells)
	 * @throws Exception if the XML data cannot be read
	 */
	private Object readCellValue() throws Exception {
		// Attributes must be read before moving on to the cell content
		final String valueType = xmlReader.getAttributeValue(NAMESPACE_OFFICE, "value-type");
		final String numberValue = xmlReader.getAttributeValue(NAMESPACE_OFFICE, "value");
		final String dateValue = xmlReader.getAttributeValue(NAMESPACE_OFFICE, "date-value");
		final String timeValue = xmlReader.getAttributeValue(NAMESPACE_OFFICE, "time-value");
		final String booleanValue = xmlReader.getAttributeValue(NAMESPACE_OFFICE, "boolean-value");
		final String stringValue = xmlReader.getAttributeValue(NAMESPACE_OFFICE, "string-value");

		final String cellText = readCellText();

		if (valueType == null) {
			return cellText;
		}

		switch (valueType.toLowerCase()) {
			case "float":
			case "percentage":
			case "currency":
				return numberValue == null ? cellText : (Object) Double.parseDouble(numberValue);
			case "date":
				return dateValue == null ? cellText : parseDateValue(dateValue);
			case "time":
				return timeValue == null ? cellText : formatTimeValue(timeValue);
			case "boolean":
				return booleanValue == null ? cellText : (Object) Boolean.parseBoolean(booleanValue);
			case "string":
				return stringValue != null ? stringValue : cellText;
			default:
				// Unknown value types are imported by their displayed text
				return cellText;
		}
	}

	/**
	 * Reads the text content of the current cell including formatted parts and links.
	 * Paragraphs are joined by line breaks, "text:s", "text:tab" and "text:line-break" are converted,
	 * annotations (comments) are skipped. The reader is positioned at the start element of the cell and afterwards at its end element.
	 *
	 * @return the text of the cell, empty for an empty cell
	 * @throws Exception if the XML data cannot be read
	 */
	private String readCellText() throws Exception {
		final StringBuilder text = new StringBuilder();
		int depth = 0;
		int paragraphDepth = -1;
		int skippedElementDepth = -1;
		boolean isFirstParagraph = true;

		while (xmlReader.hasNext()) {
			final int eventType = xmlReader.next();
			if (eventType == XMLStreamConstants.START_ELEMENT) {
				depth++;
				if (skippedElementDepth >= 0) {
					continue;
				}

				if (isElement(NAMESPACE_OFFICE, "annotation")) {
					skippedElementDepth = depth;
				} else if (paragraphDepth < 0 && (isElement(NAMESPACE_TEXT, "p") || isElement(NAMESPACE_TEXT, "h"))) {
					if (!isFirstParagraph) {
						text.append('\n');
					}
					isFirstParagraph = false;
					paragraphDepth = depth;
				} else if (paragraphDepth >= 0 && isElement(NAMESPACE_TEXT, "s")) {
					final String countAttribute = xmlReader.getAttributeValue(NAMESPACE_TEXT, "c");
					int numberOfSpaces = 1;
					if (countAttribute != null) {
						try {
							numberOfSpaces = Math.max(1, Math.min(Integer.parseInt(countAttribute.trim()), 10000));
						} catch (@SuppressWarnings("unused") final NumberFormatException e) {
							numberOfSpaces = 1;
						}
					}
					text.append(" ".repeat(numberOfSpaces));
				} else if (paragraphDepth >= 0 && isElement(NAMESPACE_TEXT, "tab")) {
					text.append('\t');
				} else if (paragraphDepth >= 0 && isElement(NAMESPACE_TEXT, "line-break")) {
					text.append('\n');
				}
			} else if (eventType == XMLStreamConstants.END_ELEMENT) {
				if (depth == 0) {
					// End element of the cell itself
					break;
				}
				if (depth == skippedElementDepth) {
					skippedElementDepth = -1;
				} else if (depth == paragraphDepth) {
					paragraphDepth = -1;
				}
				depth--;
			} else if ((eventType == XMLStreamConstants.CHARACTERS || eventType == XMLStreamConstants.CDATA) && paragraphDepth >= 0 && skippedElementDepth < 0) {
				// Whitespace characters in ODF text are collapsed to single blanks, real multiple spaces are stored as "text:s"
				text.append(xmlReader.getText().replaceAll("[ \\t\\r\\n]+", " "));
			}
		}

		return text.toString();
	}

	/**
	 * Parses an ODF date value, which may be a date only ("2024-01-31") or a date time ("2024-01-31T10:15:00").
	 */
	private static ZonedDateTime parseDateValue(final String dateValue) throws Exception {
		final String value = dateValue.trim();
		if (value.indexOf('T') < 0) {
			return LocalDate.parse(value).atStartOfDay(ZoneId.systemDefault());
		} else {
			return DateUtilities.parseIso8601DateTimeString(value);
		}
	}

	/**
	 * Formats an ODF time value (ISO 8601 duration like "PT12H30M00S") as "HH:mm:ss". Hours may exceed 23 for durations.
	 */
	private static String formatTimeValue(final String timeValue) {
		try {
			final Duration duration = Duration.parse(timeValue.trim());
			final long totalSeconds = duration.getSeconds();
			final String sign = totalSeconds < 0 ? "-" : "";
			final long absoluteSeconds = Math.abs(totalSeconds);
			return String.format("%s%02d:%02d:%02d", sign, absoluteSeconds / 3600, (absoluteSeconds % 3600) / 60, absoluteSeconds % 60);
		} catch (@SuppressWarnings("unused") final Exception e) {
			// Keep unparsable values as they are
			return timeValue;
		}
	}

	/**
	 * Checks whether the current element of the reader has the given namespace and local name.
	 */
	private boolean isElement(final String namespace, final String localName) {
		return localName.equals(xmlReader.getLocalName()) && namespace.equals(xmlReader.getNamespaceURI());
	}

	/**
	 * Reads a repeat attribute of the table namespace from the current element.
	 *
	 * @param attributeName "number-columns-repeated" or "number-rows-repeated"
	 * @param maximum upper limit of the returned value
	 * @return the repeat count, at least 1
	 */
	private int getRepeatAttribute(final String attributeName, final int maximum) {
		final String value = xmlReader.getAttributeValue(NAMESPACE_TABLE, attributeName);
		if (value == null) {
			return 1;
		}
		try {
			return Math.max(1, Math.min(Integer.parseInt(value.trim()), maximum));
		} catch (@SuppressWarnings("unused") final NumberFormatException e) {
			return 1;
		}
	}

	@Override
	public long getItemsAmountToImport() throws Exception {
		if (itemsAmount == null) {
			if (xmlReader == null) {
				openReader();
			}
			currentRowNumber = 0;

			int numberOfRows = 0;
			while (readNextRow() != null) {
				numberOfRows++;
			}

			if (noHeaders) {
				itemsAmount = numberOfRows;
			} else {
				itemsAmount = Math.max(0, numberOfRows - 1);
			}

			close();
		}
		return itemsAmount;
	}

	@Override
	public String getItemsUnitSign() throws Exception {
		return null;
	}

	/**
	 * Reads the text content of the current element including its child elements. Space elements
	 * ("text:s") are converted to the given number of spaces.
	 *
	 * @param xmlStreamReader the reader positioned at the start of the element
	 * @return the text content of the element
	 * @throws Exception if the XML data cannot be read
	 */
	public static String readElementBody(final XMLStreamReader xmlStreamReader) throws Exception {
		final StringBuilder buffer = new StringBuilder();
		int depth = 0;
		while (xmlStreamReader.hasNext()) {
			final int xmlType = xmlStreamReader.next();

			if (xmlType == XMLStreamConstants.START_ELEMENT) {
				if ("s".equals(xmlStreamReader.getLocalName())) {
					int numberOfSpaces = 1;
					for (int i = 0; i < xmlStreamReader.getAttributeCount(); i++) {
						if ("c".equals(xmlStreamReader.getAttributeLocalName(i))) {
							numberOfSpaces = Integer.parseInt(xmlStreamReader.getAttributeValue(i));
							break;
						}
					}
					for (int i = 0; i < numberOfSpaces; i++) {
						buffer.append(" ");
					}
				}

				depth++;
			} else if (xmlType == XMLStreamConstants.END_ELEMENT) {
				depth--;

				if (depth < 0) {
					break;
				}
			} else {
				buffer.append(xmlStreamReader.getText());
			}
		}

		return buffer.toString();
	}

	private void openReader() throws Exception {
		if (xmlReader != null) {
			throw new Exception("Reader was already opened before");
		}

		repeatedRowValues = null;
		repeatedRowCount = 0;
		endOfSheetReached = false;
		dataStarted = false;

		@SuppressWarnings("resource")
		ZipInputStream zipInputStream = null;
		try {
			zipInputStream = new ZipInputStream(getInputStream());
			ZipEntry nextEntry;
			while ((nextEntry = zipInputStream.getNextEntry()) != null) {
				if (nextEntry.getName().equals("content.xml")) {
					contentXmlZipInputStream = zipInputStream;
					break;
				}
			}

			if (contentXmlZipInputStream == null) {
				throw new DbImportException("Cannot find ods data");
			}

			final XMLInputFactory xmlInputFactory = XMLInputFactory.newInstance();
			xmlInputFactory.setProperty(XMLInputFactory.SUPPORT_DTD, false);
			xmlInputFactory.setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, false);

			xmlReader = xmlInputFactory.createXMLStreamReader(contentXmlZipInputStream);

			final String dataPath = "document-content/body/spreadsheet/table";
			final Stack<String> currentPath = new Stack<>();
			String currentSheetname = null;
			xmlReader.next();
			currentPath.push(xmlReader.getLocalName());
			while (currentPath.size() > 0 && !(StringUtilities.join(currentPath, "/").equals(dataPath) && (sheetname == null || sheetname.equals(currentSheetname)))) {
				xmlReader.next();
				if (xmlReader.isStartElement()) {
					currentPath.push(xmlReader.getLocalName());

					if ("table".equals(xmlReader.getLocalName())) {
						currentSheetname = null;
						for (int i = 0; i < xmlReader.getAttributeCount(); i++) {
							if (xmlReader.getAttributeLocalName(i).equals("name")) {
								currentSheetname = xmlReader.getAttributeValue(i);
								break;
							}
						}
					} else {
						currentSheetname = null;
					}
				} else if (xmlReader.isEndElement()) {
					currentPath.pop();
				}
			}
			if (currentPath.size() == 0) {
				if (StringUtilities.isNotBlank(sheetname)) {
					throw new DbImportException("Sheet '" + sheetname + "' was not found in the ods data");
				} else {
					throw new DbImportException("Path '" + dataPath + "' is not part of the xml data");
				}
			}

			currentRowNumber = 0;
		} catch (final Exception e) {
			if (zipInputStream != null) {
				zipInputStream.close();
			}
			close();
			throw e;
		}
	}

	@Override
	public Map<String, Object> getNextItemData() throws Exception {
		if (columnNames == null) {
			getAvailableDataPropertyNames();
		}

		if (xmlReader == null) {
			openReader();
		}

		if (currentRowNumber == 0 && !noHeaders) {
			// Skip Header line
			readNextRow();
		}

		final List<Object> values = readNextRow();
		if (values != null) {
			final Map<String, Object> returnMap = new HashMap<>();
			for (int i = 0; i < getAvailableDataPropertyNames().size(); i++) {
				final String columnName = getAvailableDataPropertyNames().get(i);
				// Empty trailing cells are not part of the row values, but are empty cells like all others
				final int valueIndex = columnIndices != null ? columnIndices.get(i) : i;
				final Object value = values.size() > valueIndex ? values.get(valueIndex) : "";
				if (nullValueText != null && nullValueText.equals(value)) {
					returnMap.put(columnName, null);
				} else {
					returnMap.put(columnName, value);
				}
			}
			return returnMap;
		} else {
			return null;
		}
	}

	@Override
	public void close() {
		if (contentXmlZipInputStream != null) {
			try {
				contentXmlZipInputStream.close();
			} catch (final Exception e) {
				e.printStackTrace();
			}
			contentXmlZipInputStream = null;
		}
		if (xmlReader != null) {
			try {
				xmlReader.close();
			} catch (final Exception e) {
				e.printStackTrace();
			}
			xmlReader = null;
		}
		repeatedRowValues = null;
		repeatedRowCount = 0;
		endOfSheetReached = false;
		dataStarted = false;
		super.close();
	}

	@Override
	public File filterDataItems(final List<Integer> indexList, final String fileSuffix) throws Exception {
		throw new Exception("filterDataItems method is not supported by OdsDataProvider");
	}
}
