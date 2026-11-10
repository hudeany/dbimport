package de.soderer.dbimport.dataprovider;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PushbackInputStream;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import de.soderer.dbimport.DbImportException;
import de.soderer.utilities.DateUtilities;
import de.soderer.utilities.StringUtilities;
import de.soderer.utilities.Tuple;
import de.soderer.utilities.Utilities;
import de.soderer.utilities.db.data.DbColumnType;
import de.soderer.zip.TarGzUtilities;
import de.soderer.zip.Zip4jUtilities;
import de.soderer.zip.ZipUtilities;

/**
 * Data provider for Excel data (XLS and XLSX format).
 * <p>
 * The data starts at the first non empty row of the sheet (the header row, unless noHeaders is set) and ends at the
 * first empty row after it or at the last row of the sheet. Cells, which only carry formatting, count as empty.
 * <p>
 * With headers, the columns are defined by the non empty cells of the header row. Data in columns without header is
 * ignored or rejected (see {@link #setIgnoreDataWithoutHeader(boolean)}). Without headers, the columns span from the
 * leftmost to the rightmost non empty cell of all data rows and are named "column_1", "column_2" and so on.
 */
public class ExcelDataProvider extends DataProvider {
	/** Excel data format used for date time values in the file of filtered data items */
	private static final String FILTERED_DATA_DATE_TIME_FORMAT = "yyyy-mm-dd hh:mm:ss";

	// Default optional parameters
	private String nullValueText = null;
	private boolean allowUnderfilledLines = false;
	private boolean ignoreDataWithoutHeader = true;
	private boolean noHeaders = false;
	private boolean trimData = false;

	private String dataPath = null;
	private Workbook workbook = null;
	private Sheet sheet = null;
	/** True if the import file is in XLS format (Excel 97-2003), false for XLSX */
	private boolean isXlsFormat = false;

	/** Index of the next data row to read */
	private int currentRowNumber;
	/** Index of the first data row (behind the header row) */
	private int firstDataRowNumber;
	/** Index behind the last data row (exclusive) */
	private int endRowNumber;

	private List<String> columnNames = null;
	/** Sheet column indices of the columns in columnNames, in the same order */
	private List<Integer> columnIndices = null;
	private Map<String, DbColumnType> dataTypes = null;
	private Integer itemsAmount = null;

	/**
	 * Creates the data provider.
	 *
	 * @param importFilePath path of the import file
	 * @param zipPassword password of a zip import file, or null
	 * @param allowUnderfilledLines true to accept rows with fewer values than columns. Excel rows may always contain fewer values, missing cells are imported as null
	 * @param noHeaders true if the first row contains data instead of the column names
	 * @param nullValueText text which represents null values, or null
	 * @param trimData true to trim the values
	 * @param dataPath name of the sheet to import, or null for the first sheet
	 * @throws Exception if the data provider cannot be created
	 */
	public ExcelDataProvider(final String importFilePath, final char[] zipPassword, final boolean allowUnderfilledLines, final boolean noHeaders, final String nullValueText, final boolean trimData, final String dataPath) throws Exception {
		super(false, importFilePath, zipPassword);
		this.allowUnderfilledLines = allowUnderfilledLines;
		this.noHeaders = noHeaders;
		this.nullValueText = nullValueText;
		this.trimData = trimData;
		this.dataPath = dataPath;
	}

	/**
	 * Sets whether data in columns without a header is ignored. Otherwise the import fails on such data.
	 *
	 * @param ignoreDataWithoutHeader true to ignore data in columns without a header (default)
	 */
	public void setIgnoreDataWithoutHeader(final boolean ignoreDataWithoutHeader) {
		this.ignoreDataWithoutHeader = ignoreDataWithoutHeader;
	}

	@Override
	public String getConfigurationLogString() {
		return super.getConfigurationLogString()
				+ "Format: EXCEL" + "\n"
				+ "AllowUnderfilledLines: " + allowUnderfilledLines + "\n"
				+ "IgnoreDataWithoutHeader: " + ignoreDataWithoutHeader + "\n"
				+ "TrimData: " + trimData + "\n"
				+ (StringUtilities.isNotBlank(dataPath) ? "Sheetname: " + dataPath + "\n" : "")
				+ "Null value text: " + (nullValueText == null ? "none" : "\"" + nullValueText + "\"") + "\n";
	}

	@Override
	public Map<String, DbColumnType> scanDataPropertyTypes(final Map<String, Tuple<String, String>> mapping) throws Exception {
		if (dataTypes == null) {
			try {
				openReader();

				dataTypes = new HashMap<>();

				// Scan all data for maximum
				for (int rowIndex = firstDataRowNumber; rowIndex < endRowNumber; rowIndex++) {
					final Row row = sheet.getRow(rowIndex);
					for (int i = 0; i < columnNames.size(); i++) {
						detectNextDataType(mapping, dataTypes, columnNames.get(i), getCellValueAsString(getCell(row, columnIndices.get(i))));
					}
				}
			} finally {
				close();
			}
		}

		return dataTypes;
	}

	@Override
	public List<String> getAvailableDataPropertyNames() throws Exception {
		if (columnNames == null) {
			openReader();
		}
		return columnNames;
	}

	@Override
	public long getItemsAmountToImport() throws Exception {
		if (itemsAmount == null) {
			openReader();
			itemsAmount = endRowNumber - firstDataRowNumber;
		}
		return itemsAmount;
	}

	@Override
	public String getItemsUnitSign() throws Exception {
		return null;
	}

	@Override
	public Map<String, Object> getNextItemData() throws Exception {
		if (workbook == null) {
			openReader();
		}

		if (currentRowNumber >= endRowNumber) {
			return null;
		}

		final Row row = sheet.getRow(currentRowNumber);
		final Map<String, Object> returnMap = new HashMap<>();
		for (int i = 0; i < columnNames.size(); i++) {
			final Object value = getCellValue(getCell(row, columnIndices.get(i)));
			if (nullValueText != null && nullValueText.equals(value)) {
				returnMap.put(columnNames.get(i), null);
			} else {
				returnMap.put(columnNames.get(i), value);
			}
		}

		currentRowNumber++;

		return returnMap;
	}

	@Override
	public File filterDataItems(final List<Integer> indexList, final String fileSuffix) throws Exception {
		OutputStream outputStream = null;
		File tempFile = null;
		try {
			// The workbook of the import run is still open at this point and positioned behind the last row, so open it again from the start
			close();
			openReader();

			final String fileExtension = isXlsFormat ? ".xls" : ".xlsx";
			File filteredDataFile;
			if (StringUtilities.endsWithIgnoreCase(getImportFilePath(), ".zip")) {
				filteredDataFile = new File(getImportFilePath() + "." + fileSuffix + fileExtension + ".zip");
				outputStream = ZipUtilities.openNewZipOutputStream(filteredDataFile, null);
				((ZipOutputStream) outputStream).putNextEntry(new ZipEntry(new File(getImportFilePath() + "." + fileSuffix + fileExtension).getName()));
			} else if (StringUtilities.endsWithIgnoreCase(getImportFilePath(), ".tar.gz")) {
				filteredDataFile = new File(getImportFilePath() + "." + fileSuffix + fileExtension + ".tar.gz");
				tempFile = File.createTempFile(new File(getImportFilePath()).getName(), fileSuffix);
				outputStream = new FileOutputStream(tempFile);
			} else if (StringUtilities.endsWithIgnoreCase(getImportFilePath(), ".tgz")) {
				filteredDataFile = new File(getImportFilePath() + "." + fileSuffix + fileExtension + ".tgz");
				tempFile = File.createTempFile(new File(getImportFilePath()).getName(), fileSuffix);
				outputStream = new FileOutputStream(tempFile);
			} else if (StringUtilities.endsWithIgnoreCase(getImportFilePath(), ".gz")) {
				filteredDataFile = new File(getImportFilePath() + "." + fileSuffix + fileExtension + ".gz");
				outputStream = new GZIPOutputStream(new FileOutputStream(filteredDataFile));
			} else {
				filteredDataFile = new File(getImportFilePath() + "." + fileSuffix + fileExtension);
				outputStream = new FileOutputStream(filteredDataFile);
			}

			try (Workbook filteredWorkbook = isXlsFormat ? new HSSFWorkbook() : new XSSFWorkbook()) {
				final Sheet filteredSheet = filteredWorkbook.createSheet();
				final CellStyle dateTimeCellStyle = filteredWorkbook.createCellStyle();
				dateTimeCellStyle.setDataFormat(filteredWorkbook.getCreationHelper().createDataFormat().getFormat(FILTERED_DATA_DATE_TIME_FORMAT));

				int rowIndex = 0;
				if (!noHeaders) {
					final Row headerRow = filteredSheet.createRow(rowIndex++);
					int columnIndex = 0;
					for (final String propertyName : columnNames) {
						headerRow.createCell(columnIndex++).setCellValue(propertyName);
					}
				}

				Map<String, Object> item;
				int itemIndex = 0;
				while ((item = getNextItemData()) != null) {
					itemIndex++;
					if (indexList.contains(itemIndex)) {
						final Row currentRow = filteredSheet.createRow(rowIndex++);
						int columnIndex = 0;
						for (final String propertyName : columnNames) {
							writeTypedCellValue(currentRow.createCell(columnIndex++), item.get(propertyName), dateTimeCellStyle);
						}
					}
				}

				filteredWorkbook.write(outputStream);
			}

			if (StringUtilities.endsWithIgnoreCase(getImportFilePath(), ".zip") && zipPassword != null) {
				Zip4jUtilities.createPasswordSecuredZipFile(filteredDataFile.getAbsolutePath(), zipPassword, false);
			} else if (StringUtilities.endsWithIgnoreCase(getImportFilePath(), ".tar.gz")) {
				TarGzUtilities.compress(filteredDataFile, tempFile, new File(getImportFilePath()).getName() + "." + fileSuffix);
			} else if (StringUtilities.endsWithIgnoreCase(getImportFilePath(), ".tgz")) {
				TarGzUtilities.compress(filteredDataFile, tempFile, new File(getImportFilePath()).getName() + "." + fileSuffix);
			}

			return filteredDataFile;
		} finally {
			close();
			Utilities.closeQuietly(outputStream);
			if (tempFile != null && tempFile.exists()) {
				tempFile.delete();
				tempFile = null;
			}
		}
	}

	/**
	 * Writes a value with its type into a cell, so a corrected file of filtered data items can be imported again with the same data types.
	 */
	private static void writeTypedCellValue(final Cell cell, final Object value, final CellStyle dateTimeCellStyle) {
		if (value == null) {
			return;
		} else if (value instanceof Double) {
			cell.setCellValue((Double) value);
		} else if (value instanceof Boolean) {
			cell.setCellValue((Boolean) value);
		} else if (value instanceof LocalDateTime) {
			cell.setCellValue((LocalDateTime) value);
			cell.setCellStyle(dateTimeCellStyle);
		} else {
			cell.setCellValue(value.toString());
		}
	}

	@Override
	public void close() {
		if (workbook != null) {
			Utilities.closeQuietly(workbook);
			workbook = null;
			sheet = null;
		}
		super.close();
	}

	/**
	 * Opens the workbook, selects the sheet and determines the data rows and columns. Does nothing if the workbook is already open.
	 *
	 * @throws Exception if the file cannot be read, the sheet is missing or data was found in a column without header
	 */
	private void openReader() throws Exception {
		if (workbook != null) {
			return;
		}

		try {
			final PushbackInputStream excelInputStream = new PushbackInputStream(getInputStream(), 8);
			isXlsFormat = isXlsFile(excelInputStream);
			workbook = isXlsFormat ? new HSSFWorkbook(excelInputStream) : new XSSFWorkbook(excelInputStream);

			if (StringUtilities.isNotBlank(dataPath)) {
				sheet = workbook.getSheet(dataPath);
				if (sheet == null) {
					throw new DbImportException("Sheet '" + dataPath + "' was not found in the excel data");
				}
			} else if (workbook.getNumberOfSheets() > 0) {
				sheet = workbook.getSheetAt(0);
			} else {
				throw new DbImportException("Cannot find data sheet");
			}

			determineDataArea();
			currentRowNumber = firstDataRowNumber;
		} catch (final Exception e) {
			close();
			throw e;
		}
	}

	/**
	 * Determines the data rows (from the first non empty row to the first empty row) and the data columns.
	 *
	 * @throws DbImportException if data was found in a column without header and such data is not ignored
	 */
	private void determineDataArea() throws DbImportException {
		// Find the first non empty row: the header row, or the first data row without headers
		int startRowNumber = -1;
		for (int rowIndex = Math.max(0, sheet.getFirstRowNum()); rowIndex <= sheet.getLastRowNum(); rowIndex++) {
			if (!isEmptyRow(sheet.getRow(rowIndex))) {
				startRowNumber = rowIndex;
				break;
			}
		}

		if (startRowNumber < 0) {
			// No data at all
			firstDataRowNumber = 0;
			endRowNumber = 0;
			if (columnNames == null) {
				columnNames = new ArrayList<>();
				columnIndices = new ArrayList<>();
			}
			return;
		}

		firstDataRowNumber = noHeaders ? startRowNumber : startRowNumber + 1;

		// The data ends at the first empty row. Microsoft Excel shows ~1.046.000 available lines, and formatted but empty rows may follow the data.
		endRowNumber = firstDataRowNumber;
		while (endRowNumber <= sheet.getLastRowNum() && !isEmptyRow(sheet.getRow(endRowNumber))) {
			endRowNumber++;
		}

		final List<String> detectedColumnNames = new ArrayList<>();
		final List<Integer> detectedColumnIndices = new ArrayList<>();
		if (noHeaders) {
			// Columns from the leftmost to the rightmost non empty cell of all data rows, so no data is cut off
			int minColumnIndex = Integer.MAX_VALUE;
			int maxColumnIndex = -1;
			for (int rowIndex = firstDataRowNumber; rowIndex < endRowNumber; rowIndex++) {
				final Row row = sheet.getRow(rowIndex);
				if (row != null) {
					for (int columnIndex = Math.max(0, row.getFirstCellNum()); columnIndex < row.getLastCellNum(); columnIndex++) {
						if (!isEmptyValue(getCellValue(row.getCell(columnIndex)))) {
							minColumnIndex = Math.min(minColumnIndex, columnIndex);
							maxColumnIndex = Math.max(maxColumnIndex, columnIndex);
						}
					}
				}
			}
			for (int columnIndex = minColumnIndex; columnIndex <= maxColumnIndex; columnIndex++) {
				detectedColumnNames.add("column_" + Integer.toString(columnIndex - minColumnIndex + 1));
				detectedColumnIndices.add(columnIndex);
			}
		} else {
			// Columns of the non empty header cells
			final Row headerRow = sheet.getRow(startRowNumber);
			final Map<String, Integer> columnIndexByName = new HashMap<>();
			for (int columnIndex = Math.max(0, headerRow.getFirstCellNum()); columnIndex < headerRow.getLastCellNum(); columnIndex++) {
				final String headerName = getCellValueAsString(headerRow.getCell(columnIndex));
				if (StringUtilities.isNotBlank(headerName)) {
					final Integer previousColumnIndex = columnIndexByName.put(headerName, columnIndex);
					if (previousColumnIndex != null) {
						throw new DbImportException("Duplicate column header '" + headerName + "' in columns " + columnIndexToLetters(previousColumnIndex) + " and " + columnIndexToLetters(columnIndex));
					}
					detectedColumnNames.add(headerName);
					detectedColumnIndices.add(columnIndex);
				}
			}

			if (!ignoreDataWithoutHeader) {
				checkForDataWithoutHeader(detectedColumnIndices);
			}
		}

		if (columnNames == null) {
			columnNames = detectedColumnNames;
			columnIndices = detectedColumnIndices;
		} else {
			// Reopened workbook: keep the known column names, but use the current column positions
			columnIndices = detectedColumnIndices;
		}
	}

	/**
	 * Checks all data rows for data in columns, which have no header (also left and right of the header columns).
	 *
	 * @param headerColumnIndices sheet column indices of the columns with header
	 * @throws DbImportException if data was found in a column without header
	 */
	private void checkForDataWithoutHeader(final List<Integer> headerColumnIndices) throws DbImportException {
		for (int rowIndex = firstDataRowNumber; rowIndex < endRowNumber; rowIndex++) {
			final Row row = sheet.getRow(rowIndex);
			if (row != null) {
				for (int columnIndex = Math.max(0, row.getFirstCellNum()); columnIndex < row.getLastCellNum(); columnIndex++) {
					if (!headerColumnIndices.contains(columnIndex) && !isEmptyValue(getCellValue(row.getCell(columnIndex)))) {
						throw new DbImportException("Found data in a column without header: " + columnIndexToLetters(columnIndex) + " (row " + (rowIndex + 1) + ")");
					}
				}
			}
		}
	}

	/**
	 * Checks whether a row has no value. Cells, which only carry formatting, are empty.
	 */
	private boolean isEmptyRow(final Row row) {
		if (row == null) {
			return true;
		}
		for (int columnIndex = Math.max(0, row.getFirstCellNum()); columnIndex < row.getLastCellNum(); columnIndex++) {
			if (!isEmptyValue(getCellValue(row.getCell(columnIndex)))) {
				return false;
			}
		}
		return true;
	}

	/**
	 * Checks whether a cell value is empty (null or empty text).
	 */
	private static boolean isEmptyValue(final Object value) {
		return value == null || (value instanceof String && ((String) value).isEmpty());
	}

	/**
	 * Returns a cell of a row, which may be null for empty rows.
	 */
	private static Cell getCell(final Row row, final int columnIndex) {
		return row == null ? null : row.getCell(columnIndex);
	}

	/**
	 * Returns the value of a cell for the import, depending on its type (formula cells by their cached result).
	 *
	 * @return null for empty or erroneous cells, a LocalDateTime for date formatted cells, a text "HH:mm:ss" for time
	 *         only formatted cells, a Double for other numeric cells, a Boolean for boolean cells, and the (optionally
	 *         trimmed) text otherwise
	 */
	private Object getCellValue(final Cell cell) {
		if (cell == null) {
			return null;
		}
		CellType cellType = cell.getCellType();
		if (cellType == CellType.FORMULA) {
			cellType = cell.getCachedFormulaResultType();
		}
		switch (cellType) {
			case NUMERIC:
				if (DateUtil.isCellDateFormatted(cell)) {
					final double numericValue = cell.getNumericCellValue();
					if (numericValue >= 0 && numericValue < 1) {
						// Time without date (Excel day 0 is no valid date)
						return formatTimeOfDay(numericValue);
					} else {
						// Read directly as LocalDateTime: the former way via java.util.Date in the default time zone
						// shifted times, which do not exist in that time zone (daylight saving time switch)
						return cell.getLocalDateTimeCellValue();
					}
				} else {
					return cell.getNumericCellValue();
				}
			case BOOLEAN:
				return cell.getBooleanCellValue();
			case BLANK:
			case ERROR:
				return null;
			case STRING:
			default:
				return trimData ? StringUtilities.trim(cell.getStringCellValue()) : cell.getStringCellValue();
		}
	}

	/**
	 * Formats the fraction of an Excel day as time of day "HH:mm:ss".
	 */
	private static String formatTimeOfDay(final double dayFraction) {
		final long totalSeconds = Math.min(Math.round(dayFraction * 86400), 86399);
		return String.format("%02d:%02d:%02d", totalSeconds / 3600, (totalSeconds % 3600) / 60, totalSeconds % 60);
	}

	/**
	 * Returns the value of a cell as text, as needed for header names and the data type detection.
	 */
	private String getCellValueAsString(final Cell cell) {
		final Object value = getCellValue(cell);
		if (value == null) {
			return null;
		} else if (value instanceof LocalDateTime) {
			// Same pattern as used by the data type detection in DataProvider.detectNextDataType()
			return DateUtilities.formatDate(DateUtilities.getDateTimeFormatWithSecondsPattern(Locale.getDefault()), (LocalDateTime) value);
		} else if (value instanceof Double) {
			// Integral numbers without ".0", so they are detected as integer values
			return BigDecimal.valueOf((Double) value).stripTrailingZeros().toPlainString();
		} else {
			return value.toString();
		}
	}

	/**
	 * Checks whether a file is in XLS format (Excel 97-2003) by its magic bytes.
	 *
	 * @param potentialXlsFile the file to check
	 * @return true if the file is in XLS format
	 * @throws FileNotFoundException if the file does not exist
	 * @throws IOException if the file cannot be read
	 */
	public static boolean isXlsFile(final File potentialXlsFile) throws FileNotFoundException, IOException {
		try (FileInputStream inputStream = new FileInputStream(potentialXlsFile)) {
			final byte[] magicBytes = new byte[8];
			final int readBytes = inputStream.read(magicBytes);
			return readBytes == 8 && Byte.toUnsignedInt(magicBytes[0]) == 0xD0 && Byte.toUnsignedInt(magicBytes[1]) == 0xCF && Byte.toUnsignedInt(magicBytes[2]) == 0x11
					&& Byte.toUnsignedInt(magicBytes[3]) == 0xE0 && Byte.toUnsignedInt(magicBytes[4]) == 0xA1 && Byte.toUnsignedInt(magicBytes[5]) == 0xB1
					&& Byte.toUnsignedInt(magicBytes[6]) == 0x1A && Byte.toUnsignedInt(magicBytes[7]) == 0xE1;
		}
	}

	/**
	 * Checks whether a stream contains data in XLS format (Excel 97-2003) by its magic bytes. The read bytes are
	 * pushed back into the stream.
	 *
	 * @param inputStream the stream to check, which must allow to push back at least 8 bytes
	 * @return true if the data is in XLS format
	 * @throws FileNotFoundException not thrown, only declared for compatibility
	 * @throws IOException if the stream cannot be read
	 */
	public static boolean isXlsFile(final PushbackInputStream inputStream) throws FileNotFoundException, IOException {
		final byte[] magicBytes = new byte[8];
		final int readBytes = inputStream.read(magicBytes);
		if (readBytes > 0) {
			inputStream.unread(magicBytes, 0, readBytes);
		}
		return readBytes == 8 && Byte.toUnsignedInt(magicBytes[0]) == 0xD0 && Byte.toUnsignedInt(magicBytes[1]) == 0xCF && Byte.toUnsignedInt(magicBytes[2]) == 0x11
				&& Byte.toUnsignedInt(magicBytes[3]) == 0xE0 && Byte.toUnsignedInt(magicBytes[4]) == 0xA1 && Byte.toUnsignedInt(magicBytes[5]) == 0xB1
				&& Byte.toUnsignedInt(magicBytes[6]) == 0x1A && Byte.toUnsignedInt(magicBytes[7]) == 0xE1;
	}

	/**
	 * Converts a column index to the Excel column name.
	 *
	 * @param columnIndex the 0-based column index
	 * @return the Excel column name, e.g. "A" for 0, "Z" for 25 and "AA" for 26
	 */
	public static String columnIndexToLetters(final int columnIndex) {
		// Bijective base 26 (A..Z, AA..ZZ, AAA..). The former calculation failed for index 676 ("ZA") and above.
		final StringBuilder columnLetters = new StringBuilder();
		int remaining = columnIndex + 1;
		while (remaining > 0) {
			remaining--;
			columnLetters.insert(0, (char) ('A' + (remaining % 26)));
			remaining = remaining / 26;
		}
		return columnLetters.toString();
	}
}
