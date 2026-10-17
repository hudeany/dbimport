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
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import de.soderer.dbimport.DbImportException;
import de.soderer.utilities.DateUtilities;
import de.soderer.utilities.Tuple;
import de.soderer.utilities.Utilities;
import de.soderer.utilities.db.data.DbColumnType;
import de.soderer.zip.TarGzUtilities;
import de.soderer.zip.Zip4jUtilities;
import de.soderer.zip.ZipUtilities;

/**
 * Data provider for Excel data (XLS and XLSX format).
 */
public class ExcelDataProvider extends DataProvider {
	// Default optional parameters
	private String nullValueText = null;
	private boolean allowUnderfilledLines = false;
	private boolean ignoreDataWithoutHeader = true;
	private boolean noHeaders = false;
	private boolean trimData = false;

	private String dataPath = null;
	private HSSFWorkbook hssfWorkbook;
	private HSSFSheet hssfSheet;
	private XSSFWorkbook xssfWorkbook;
	private XSSFSheet xssfSheet;
	private int currentRowNumber;
	private int maxRowNumber;

	private List<String> columnNames = null;
	private Map<String, DbColumnType> dataTypes = null;
	private Integer itemsAmount = null;

	/**
	 * Creates the data provider.
	 *
	 * @param importFilePath path of the import file
	 * @param zipPassword password of a zip import file, or null
	 * @param allowUnderfilledLines true to accept rows with fewer values than columns
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
				+ "Null value text: " + (nullValueText == null ? "none" : "\"" + nullValueText + "\"") + "\n";
	}

	@Override
	public Map<String, DbColumnType> scanDataPropertyTypes(final Map<String, Tuple<String, String>> mapping) throws Exception {
		getAvailableDataPropertyNames();

		if (dataTypes == null) {
			try {
				openReader();

				dataTypes = new HashMap<>();

				// Scan all data for maximum
				while (currentRowNumber <= maxRowNumber) {
					final List<String> values = new ArrayList<>();
					if (hssfSheet != null) {
						for (int i = hssfSheet.getRow(hssfSheet.getFirstRowNum()).getFirstCellNum(); i < hssfSheet.getRow(hssfSheet.getFirstRowNum()).getLastCellNum(); i++) {
							values.add(getCellValueAsString(getCell(hssfSheet.getRow(currentRowNumber), i)));
						}
					} else {
						for (int i = xssfSheet.getRow(xssfSheet.getFirstRowNum()).getFirstCellNum(); i < xssfSheet.getRow(xssfSheet.getFirstRowNum()).getLastCellNum(); i++) {
							values.add(getCellValueAsString(getCell(xssfSheet.getRow(currentRowNumber), i)));
						}
					}

					for (int i = 0; i < values.size(); i++) {
						final String columnName = (columnNames == null || columnNames.size() <= i ? "column_" + Integer.toString(i + 1) : columnNames.get(i));
						final String currentValue = values.get(i);
						detectNextDataType(mapping, dataTypes, columnName, currentValue);
					}

					currentRowNumber++;
				}
			} catch (final Exception e) {
				throw e;
			} finally {
				close();
			}
		}

		return dataTypes;
	}

	@Override
	public List<String> getAvailableDataPropertyNames() throws Exception {
		if (columnNames == null) {
			try {
				openReader();

				columnNames = new ArrayList<>();
				if (noHeaders) {
					final List<String> returnList = new ArrayList<>();
					if (allowUnderfilledLines) {
						// Scan all data for maximum
						int maxColumns = 0;
						if (hssfSheet != null) {
							for (int i = hssfSheet.getFirstRowNum(); i <= maxRowNumber; i++) {
								if (hssfSheet.getRow(i) != null) {
									maxColumns = Math.max(maxColumns, hssfSheet.getRow(i).getLastCellNum() - hssfSheet.getRow(i).getFirstCellNum());
								}
							}
						} else {
							for (int i = xssfSheet.getFirstRowNum(); i <= maxRowNumber; i++) {
								if (xssfSheet.getRow(i) != null) {
									maxColumns = Math.max(maxColumns, xssfSheet.getRow(i).getLastCellNum() - xssfSheet.getRow(i).getFirstCellNum());
								}
							}
						}
						for (int i = 0; i < maxColumns; i++) {
							returnList.add("column_" + Integer.toString(i + 1));
						}
						columnNames = returnList;
					} else {
						// Only take first data as example for all other data
						int maxColumns = 0;
						if (hssfSheet != null) {
							maxColumns = hssfSheet.getRow(hssfSheet.getFirstRowNum()).getLastCellNum() - hssfSheet.getRow(hssfSheet.getFirstRowNum()).getFirstCellNum();
						} else {
							maxColumns = xssfSheet.getRow(xssfSheet.getFirstRowNum()).getLastCellNum() - xssfSheet.getRow(xssfSheet.getFirstRowNum()).getFirstCellNum();
						}
						for (int i = 0; i < maxColumns; i++) {
							returnList.add("column_" + Integer.toString(i + 1));
						}
						columnNames = returnList;
					}
				} else {
					// Read headers from file
					if (hssfSheet != null) {
						for (int i = hssfSheet.getRow(hssfSheet.getFirstRowNum()).getFirstCellNum(); i < hssfSheet.getRow(hssfSheet.getFirstRowNum()).getLastCellNum(); i++) {
							final String cellValue = getCellValueAsString(hssfSheet.getRow(hssfSheet.getFirstRowNum()).getCell(i));
							if (Utilities.isNotBlank(cellValue)) {
								columnNames.add(cellValue);
							} else {
								columnNames.add(null);
							}
						}

						// Check for data in columns without Header
						if (!ignoreDataWithoutHeader) {
							for (int columnIndex = 0; columnIndex < columnNames.size(); columnIndex++) {
								if (Utilities.isBlank(columnNames.get(columnIndex))) {
									for (int rowIndex = hssfSheet.getFirstRowNum(); rowIndex <= maxRowNumber; rowIndex++) {
										final int maxRowColumn = hssfSheet.getRow(rowIndex) == null ? 0 : hssfSheet.getRow(rowIndex).getLastCellNum() - hssfSheet.getRow(rowIndex).getFirstCellNum();
										if (columnIndex < maxRowColumn) {
											final String cellValue = getCellValueAsString(hssfSheet.getRow(rowIndex).getCell(columnIndex));
											if (Utilities.isNotBlank(cellValue)) {
												throw new DbImportException("Found data in a column without header: " + columnIndexToLetters(columnIndex));
											}
										}
									}
								}
							}
						}
					} else {
						for (int i = xssfSheet.getRow(xssfSheet.getFirstRowNum()).getFirstCellNum(); i < xssfSheet.getRow(xssfSheet.getFirstRowNum()).getLastCellNum(); i++) {
							final String cellValue = getCellValueAsString(xssfSheet.getRow(xssfSheet.getFirstRowNum()).getCell(i));
							if (Utilities.isNotBlank(cellValue)) {
								columnNames.add(cellValue);
							} else {
								columnNames.add(null);
							}
						}

						// Check for data in columns without Header
						if (!ignoreDataWithoutHeader) {
							for (int columnIndex = 0; columnIndex < columnNames.size(); columnIndex++) {
								if (Utilities.isBlank(columnNames.get(columnIndex))) {
									for (int rowIndex = xssfSheet.getFirstRowNum(); rowIndex <= maxRowNumber; rowIndex++) {
										final int maxRowColumn = xssfSheet.getRow(rowIndex) == null ? 0 : xssfSheet.getRow(rowIndex).getLastCellNum() - xssfSheet.getRow(rowIndex).getFirstCellNum();
										if (columnIndex < maxRowColumn) {
											final String cellValue = getCellValueAsString(xssfSheet.getRow(rowIndex).getCell(columnIndex));
											if (Utilities.isNotBlank(cellValue)) {
												throw new DbImportException("Found data in a column without header: " + columnIndexToLetters(columnIndex));
											}
										}
									}
								}
							}
						}
					}
				}
			} catch (final Exception e) {
				throw e;
			}
		}

		return columnNames;
	}

	@Override
	public long getItemsAmountToImport() throws Exception {
		if (itemsAmount == null) {
			try {
				openReader();

				if (hssfSheet != null) {
					itemsAmount = maxRowNumber - hssfSheet.getFirstRowNum() + 1;
				} else {
					itemsAmount = maxRowNumber - xssfSheet.getFirstRowNum() + 1;
				}
				if (!noHeaders) {
					// Skip header row
					itemsAmount--;
				}
			} catch (final Exception e) {
				throw e;
			}
		}
		return itemsAmount;
	}

	@Override
	public String getItemsUnitSign() throws Exception {
		return null;
	}

	@Override
	public Map<String, Object> getNextItemData() throws Exception {
		if (hssfWorkbook == null && xssfWorkbook == null) {
			openReader();
		}

		if (hssfSheet != null) {
			if (currentRowNumber > maxRowNumber) {
				return null;
			}
		} else {
			if (currentRowNumber > maxRowNumber) {
				return null;
			}
		}

		final List<Object> values = new ArrayList<>();
		if (hssfSheet != null) {
			for (int i = hssfSheet.getRow(hssfSheet.getFirstRowNum()).getFirstCellNum(); i < hssfSheet.getRow(hssfSheet.getFirstRowNum()).getLastCellNum(); i++) {
				values.add(getCellValue(getCell(hssfSheet.getRow(currentRowNumber), i)));
			}
		} else {
			for (int i = xssfSheet.getRow(xssfSheet.getFirstRowNum()).getFirstCellNum(); i < xssfSheet.getRow(xssfSheet.getFirstRowNum()).getLastCellNum(); i++) {
				values.add(getCellValue(getCell(xssfSheet.getRow(currentRowNumber), i)));
			}
		}

		final Map<String, Object> returnMap = new HashMap<>();
		for (int i = 0; i < getAvailableDataPropertyNames().size(); i++) {
			final String columnName = getAvailableDataPropertyNames().get(i);
			if (values.size() > i) {
				if (nullValueText != null && nullValueText.equals(values.get(i))) {
					returnMap.put(columnName, null);
				} else {
					returnMap.put(columnName, values.get(i));
				}
			} else {
				returnMap.put(columnName, null);
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
			// The workbook of the import run is still open at this point and positioned behind the last row, so
			// openReader() did nothing and the erroneous data file was written without any data rows
			close();
			openReader();

			File filteredDataFile;
			if (Utilities.endsWithIgnoreCase(getImportFilePath(), ".zip")) {
				if (hssfSheet != null) {
					filteredDataFile = new File(getImportFilePath() + "." + fileSuffix + ".xls.zip");
					outputStream = ZipUtilities.openNewZipOutputStream(filteredDataFile, null);
					((ZipOutputStream) outputStream).putNextEntry(new ZipEntry(new File(getImportFilePath() + "." + fileSuffix + ".xls").getName()));
				} else {
					filteredDataFile = new File(getImportFilePath() + "." + fileSuffix + ".xlsx.zip");
					outputStream = ZipUtilities.openNewZipOutputStream(filteredDataFile, null);
					((ZipOutputStream) outputStream).putNextEntry(new ZipEntry(new File(getImportFilePath() + "." + fileSuffix + ".xlsx").getName()));
				}
			} else if (Utilities.endsWithIgnoreCase(getImportFilePath(), ".tar.gz")) {
				if (hssfSheet != null) {
					filteredDataFile = new File(getImportFilePath() + "." + fileSuffix + ".xls.tar.gz");
				} else {
					filteredDataFile = new File(getImportFilePath() + "." + fileSuffix + ".xlsx.tar.gz");
				}
				tempFile = File.createTempFile(new File(getImportFilePath()).getName(), fileSuffix);
				outputStream = new FileOutputStream(tempFile);
			} else if (Utilities.endsWithIgnoreCase(getImportFilePath(), ".tgz")) {
				if (hssfSheet != null) {
					filteredDataFile = new File(getImportFilePath() + "." + fileSuffix + ".xls.tgz");
				} else {
					filteredDataFile = new File(getImportFilePath() + "." + fileSuffix + ".xlsx.tgz");
				}
				tempFile = File.createTempFile(new File(getImportFilePath()).getName(), fileSuffix);
				outputStream = new FileOutputStream(tempFile);
			} else if (Utilities.endsWithIgnoreCase(getImportFilePath(), ".gz")) {
				if (hssfSheet != null) {
					filteredDataFile = new File(getImportFilePath() + "." + fileSuffix + ".xls.gz");
				} else {
					filteredDataFile = new File(getImportFilePath() + "." + fileSuffix + ".xlsx.gz");
				}
				outputStream = new GZIPOutputStream(new FileOutputStream(filteredDataFile));
			} else {
				if (hssfSheet != null) {
					filteredDataFile = new File(getImportFilePath() + "." + fileSuffix + ".xls");
				} else {
					filteredDataFile = new File(getImportFilePath() + "." + fileSuffix + ".xlsx");
				}
				outputStream = new FileOutputStream(filteredDataFile);
			}

			if (hssfSheet != null) {
				try (HSSFWorkbook workbook = new HSSFWorkbook()) {
					final HSSFSheet sheet = workbook.createSheet();
					int rowIndex = 0;
					HSSFRow currentRow;

					if (!noHeaders) {
						currentRow = sheet.createRow(rowIndex++);
						int columnIndex = 0;
						for (final String propertyName : getAvailableDataPropertyNames()) {
							final HSSFCell cell = currentRow.createCell(columnIndex++);
							cell.setCellValue(propertyName);
						}
					}

					Map<String, Object> item;
					int itemIndex = 0;
					while ((item = getNextItemData()) != null) {
						itemIndex++;
						if (indexList.contains(itemIndex)) {
							currentRow = sheet.createRow(rowIndex++);
							int columnIndex = 0;
							for (final String propertyName : getAvailableDataPropertyNames()) {
								final HSSFCell cell = currentRow.createCell(columnIndex++);
								final Object cellValue = item.get(propertyName);
								if (cellValue != null) {
									cell.setCellValue(cellValue.toString());
								}
							}
						}
					}

					workbook.write(outputStream);
				}
			} else {
				try (XSSFWorkbook workbook = new XSSFWorkbook()) {
					final XSSFSheet sheet = workbook.createSheet();
					int rowIndex = 0;
					XSSFRow currentRow;

					if (!noHeaders) {
						currentRow = sheet.createRow(rowIndex++);
						int columnIndex = 0;
						for (final String propertyName : getAvailableDataPropertyNames()) {
							final XSSFCell cell = currentRow.createCell(columnIndex++);
							cell.setCellValue(propertyName);
						}
					}

					Map<String, Object> item;
					int itemIndex = 0;
					while ((item = getNextItemData()) != null) {
						itemIndex++;
						if (indexList.contains(itemIndex)) {
							currentRow = sheet.createRow(rowIndex++);
							int columnIndex = 0;
							for (final String propertyName : getAvailableDataPropertyNames()) {
								final XSSFCell cell = currentRow.createCell(columnIndex++);
								final Object cellValue = item.get(propertyName);
								if (cellValue != null) {
									cell.setCellValue(cellValue.toString());
								}
							}
						}
					}

					workbook.write(outputStream);
				}
			}

			if (Utilities.endsWithIgnoreCase(getImportFilePath(), ".zip") && zipPassword != null) {
				Zip4jUtilities.createPasswordSecuredZipFile(filteredDataFile.getAbsolutePath(), zipPassword, false);
			} else if (Utilities.endsWithIgnoreCase(getImportFilePath(), ".tar.gz")) {
				TarGzUtilities.compress(filteredDataFile, tempFile, new File(getImportFilePath()).getName() + "." + fileSuffix);
			} else if (Utilities.endsWithIgnoreCase(getImportFilePath(), ".tgz")) {
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

	@Override
	public void close() {
		if (hssfWorkbook != null) {
			Utilities.closeQuietly(hssfWorkbook);
			hssfWorkbook = null;
			hssfSheet = null;
		}
		if (xssfWorkbook != null) {
			Utilities.closeQuietly(xssfWorkbook);
			xssfWorkbook = null;
			xssfSheet = null;
		}
		super.close();
	}

	private void openReader() throws Exception {
		try {
			if (xssfSheet == null && hssfSheet == null) {
				final PushbackInputStream excelInputStream = new PushbackInputStream(getInputStream(), 8);
				final boolean isXlsFormatFile = isXlsFile(excelInputStream);

				if (isXlsFormatFile) {
					hssfWorkbook = new HSSFWorkbook(excelInputStream);
					if (Utilities.isNotBlank(dataPath)) {
						hssfSheet = hssfWorkbook.getSheet(dataPath);
					} else {
						hssfSheet = hssfWorkbook.getSheetAt(0);
					}

					if (hssfSheet != null) {
						currentRowNumber = hssfSheet.getFirstRowNum();
						maxRowNumber = hssfSheet.getLastRowNum();
						// Microsoft Excel shows ~1.046.000 available lines, but
						// they are not used at all. So detect the last used line
						for (int rowIndex = maxRowNumber; rowIndex >= currentRowNumber; rowIndex--) {
							final HSSFRow row = hssfSheet.getRow(rowIndex);
							if (row != null) {
								boolean rowHasValue = false;
								for (int columnIndex = row.getFirstCellNum(); columnIndex < row.getLastCellNum(); columnIndex++) {
									if (row.getCell(columnIndex) != null) {
										rowHasValue = true;
										break;
									}
								}
								if (rowHasValue) {
									maxRowNumber = rowIndex;
									break;
								}
							}
						}
					} else {
						throw new DbImportException("Cannot find data sheet");
					}
				} else {
					xssfWorkbook = new XSSFWorkbook(excelInputStream);
					if (Utilities.isNotBlank(dataPath)) {
						xssfSheet = xssfWorkbook.getSheet(dataPath);
					} else {
						xssfSheet = xssfWorkbook.getSheetAt(0);
					}

					if (xssfSheet != null) {
						currentRowNumber = xssfSheet.getFirstRowNum();
						maxRowNumber = xssfSheet.getLastRowNum();
						// Microsoft Excel shows ~1.046.000 available lines, but
						// they are not used at all. So detect the last used line
						for (int rowIndex = maxRowNumber; rowIndex >= currentRowNumber; rowIndex--) {
							final XSSFRow row = xssfSheet.getRow(rowIndex);
							if (row != null) {
								boolean rowHasValue = false;
								for (int columnIndex = row.getFirstCellNum(); columnIndex < row.getLastCellNum(); columnIndex++) {
									if (row.getCell(columnIndex) != null) {
										rowHasValue = true;
										break;
									}
								}
								if (rowHasValue) {
									maxRowNumber = rowIndex;
									break;
								}
							}
						}
					} else {
						throw new DbImportException("Cannot find data sheet");
					}
				}

				if (!noHeaders) {
					currentRowNumber++;
				}
			}
		} catch (final Exception e) {
			close();
			throw e;
		}
	}

	/**
	 * Returns a cell of a row, which may be null for empty rows.
	 */
	private static Cell getCell(final Row row, final int columnIndex) {
		return row == null ? null : row.getCell(columnIndex);
	}

	/**
	 * Returns the value of a cell for the import, depending on its type (formula cells by their cached result).
	 * Before, getStringCellValue() was also called for numeric, boolean and formula cells, which throws an
	 * IllegalStateException, and empty rows caused a NullPointerException.
	 *
	 * @return null for empty or erroneous cells, a LocalDateTime for date formatted cells, a Double for other numeric
	 *         cells, a Boolean for boolean cells, and the (optionally trimmed) text otherwise
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
					// POI interprets the date in the default time zone. As LocalDateTime it is converted between the import data
					// and the database time zone like all other date values (a java.util.Date was passed to setObject() before)
					return LocalDateTime.ofInstant(cell.getDateCellValue().toInstant(), ZoneId.systemDefault());
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
				return trimData ? Utilities.trim(cell.getStringCellValue()) : cell.getStringCellValue();
		}
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
		inputStream.unread(magicBytes, 0, readBytes);
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
