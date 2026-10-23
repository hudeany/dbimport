package de.soderer.dbimport.dataprovider;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import de.soderer.dbimport.DbImportException;
import de.soderer.utilities.CountingInputStream;
import de.soderer.utilities.DateUtilities;
import de.soderer.utilities.InputStreamWithOtherItemsToClose;
import de.soderer.utilities.IoUtilities;
import de.soderer.utilities.NumberUtilities;
import de.soderer.utilities.StringUtilities;
import de.soderer.utilities.Tuple;
import de.soderer.utilities.db.data.DbColumnType;
import de.soderer.utilities.db.data.DbSimpleDataType;
import de.soderer.zip.TarGzUtilities;
import de.soderer.zip.Zip4jUtilities;
import de.soderer.zip.ZipUtilities;

/**
 * Base class of the readers for the supported import data formats.
 *
 * <p>
 * The data is read from a file (optionally compressed as zip, tar.gz, tgz or gz) or given as inline data.
 * </p>
 */
public abstract class DataProvider implements Closeable {
	/**
	 * Returns the names of the data properties (e.g. the CSV column names).
	 *
	 * @return the names of the data properties
	 * @throws Exception if the data cannot be read
	 */
	abstract public List<String> getAvailableDataPropertyNames() throws Exception;
	/**
	 * Returns the amount of data to import, used for the progress display.
	 *
	 * @return the number of data items, or the number of bytes (see {@link #getItemsUnitSign()})
	 * @throws Exception if the data cannot be read
	 */
	abstract public long getItemsAmountToImport() throws Exception;
	/**
	 * Returns the unit of the amount returned by {@link #getItemsAmountToImport()}.
	 *
	 * @return the unit sign (e.g. "B" for bytes), or null if the amount is a number of data items
	 * @throws Exception if the data cannot be read
	 */
	abstract public String getItemsUnitSign() throws Exception;
	/**
	 * Reads the next data item.
	 *
	 * @return the property values of the next data item by property name, or null if there are no more items
	 * @throws Exception if the data cannot be read or is invalid
	 */
	abstract public Map<String, Object> getNextItemData() throws Exception;
	/**
	 * Reads all data items and detects the data type of each data property, e.g. to create a new table.
	 *
	 * @param mapping mapping of database columns to data properties and their format info, or null
	 * @return the detected data types by property name
	 * @throws Exception if the data cannot be read
	 */
	abstract public Map<String, DbColumnType> scanDataPropertyTypes(Map<String, Tuple<String, String>> mapping) throws Exception;
	/**
	 * Writes the data items with the given indices to a new file next to the import file, e.g. to keep the
	 * erroneous data items.
	 *
	 * @param indexList the 1-based indices of the data items to write
	 * @param fileSuffix suffix inserted into the new file name
	 * @return the new file
	 * @throws Exception if the data cannot be read or the new file cannot be written
	 */
	abstract public File filterDataItems(List<Integer> indexList, String fileSuffix) throws Exception;

	private final boolean isInlineData;
	private final String importInlineData;
	private final File importFile;
	/**
	 * Password of a zip import file, or null.
	 */
	protected char[] zipPassword;

	private CountingInputStream inputStream = null;

	/**
	 * Creates the data provider.
	 *
	 * @param isInlineData true if importFilePathOrData contains the data itself, false if it is a file path
	 * @param importFilePathOrData path of the import file, or the data itself
	 * @param zipPassword password of a zip import file, or null (ignored for inline data)
	 */
	public DataProvider(final boolean isInlineData, final String importFilePathOrData, final char[] zipPassword) {
		this.isInlineData = isInlineData;
		if (isInlineData) {
			importInlineData = importFilePathOrData;
			importFile = null;
			this.zipPassword = null;
		} else {
			importFile = new File(importFilePathOrData);
			this.zipPassword = zipPassword;
			importInlineData = null;
		}
	}

	/**
	 * Returns the password of a zip import file.
	 *
	 * @return the password, or null
	 */
	public char[] getZipPassword() {
		return zipPassword;
	}

	static void detectNextDataType(final Map<String, Tuple<String, String>> mapping, final Map<String, DbColumnType> dataTypes, final String propertyKey, final String currentValue) {
		String formatInfo = null;
		if (mapping != null) {
			for (final Tuple<String, String> mappingValue : mapping.values()) {
				if (mappingValue.getFirst().equals(propertyKey)) {
					if (StringUtilities.isNotBlank(mappingValue.getSecond())) {
						formatInfo = mappingValue.getSecond();
						break;
					}
				}
			}
		}

		final DbSimpleDataType currentType = dataTypes.get(propertyKey) == null ? null : dataTypes.get(propertyKey).getSimpleDataType();
		if (currentType != DbSimpleDataType.Blob) {
			if (StringUtilities.isEmpty(currentValue)) {
				if (!dataTypes.containsKey(propertyKey)) {
					dataTypes.put(propertyKey, null);
				}
			} else if ("file".equalsIgnoreCase(formatInfo) || currentValue.length() > 4000) {
				dataTypes.put(propertyKey, new DbColumnType("BLOB", -1, -1, -1, true, false, null));
			} else if (currentType != DbSimpleDataType.String && currentType != DbSimpleDataType.Integer && currentType != DbSimpleDataType.BigInteger && currentType != DbSimpleDataType.Float && currentType != DbSimpleDataType.Blob && currentType != DbSimpleDataType.Clob && StringUtilities.isNotBlank(formatInfo) && !".".equals(formatInfo) && !",".equals(formatInfo) && !"file".equalsIgnoreCase(formatInfo) && !"lc".equalsIgnoreCase(formatInfo) && !"uc".equalsIgnoreCase(formatInfo)) {
				try {
					DateUtilities.parseLocalDateTime(formatInfo, currentValue.trim());
					if (formatInfo != null && (formatInfo.toLowerCase().contains("h") || formatInfo.contains("m") || formatInfo.toLowerCase().contains("s"))) {
						dataTypes.put(propertyKey, new DbColumnType("TIMESTAMP", -1, -1, -1, true, false, null));
					} else {
						dataTypes.put(propertyKey, new DbColumnType("DATE", -1, -1, -1, true, false, null));
					}
				} catch (@SuppressWarnings("unused") final Exception e) {
					if (NumberUtilities.isInteger(currentValue)) {
						dataTypes.put(propertyKey, new DbColumnType("INTEGER", -1, -1, -1, true, false, null));
					} else if (NumberUtilities.isBigInteger(currentValue)) {
						dataTypes.put(propertyKey, new DbColumnType("BIGINT", -1, -1, -1, true, false, null));
					} else if (NumberUtilities.isDouble(currentValue) && currentValue.trim().length() <= 20) {
						dataTypes.put(propertyKey, new DbColumnType("DOUBLE", -1, -1, -1, true, false, null));
					} else {
						dataTypes.put(propertyKey, new DbColumnType("VARCHAR", Math.max(dataTypes.get(propertyKey) == null ? 0 : dataTypes.get(propertyKey).getCharacterByteSize(), currentValue.getBytes(StandardCharsets.UTF_8).length), -1, -1, true, false, null));
					}
				}
			} else if (currentType != DbSimpleDataType.String && currentType != DbSimpleDataType.Integer && currentType != DbSimpleDataType.BigInteger && currentType != DbSimpleDataType.Float && currentType != DbSimpleDataType.Blob && currentType != DbSimpleDataType.Clob && currentType != DbSimpleDataType.Date && StringUtilities.isBlank(formatInfo)) {
				try {
					DateUtilities.parseLocalDateTime(DateUtilities.getDateTimeFormatWithSecondsPattern(Locale.getDefault()), currentValue.trim());
					dataTypes.put(propertyKey, new DbColumnType("TIMESTAMP", -1, -1, -1, true, false, null));
				} catch (@SuppressWarnings("unused") final Exception e) {
					try {
						DateUtilities.parseLocalDate(DateUtilities.getDateFormatPattern(Locale.getDefault()), currentValue.trim());
						dataTypes.put(propertyKey, new DbColumnType("DATE", -1, -1, -1, true, false, null));
					} catch (@SuppressWarnings("unused") final Exception e1) {
						if (NumberUtilities.isInteger(currentValue)) {
							dataTypes.put(propertyKey, new DbColumnType("INTEGER", -1, -1, -1, true, false, null));
						} else if (NumberUtilities.isBigInteger(currentValue)) {
							dataTypes.put(propertyKey, new DbColumnType("BIGINT", -1, -1, -1, true, false, null));
						} else if (NumberUtilities.isDouble(currentValue) && currentValue.trim().length() <= 20) {
							dataTypes.put(propertyKey, new DbColumnType("DOUBLE", -1, -1, -1, true, false, null));
						} else {
							dataTypes.put(propertyKey, new DbColumnType("VARCHAR", Math.max(dataTypes.get(propertyKey) == null ? 0 : dataTypes.get(propertyKey).getCharacterByteSize(), currentValue.getBytes(StandardCharsets.UTF_8).length), -1, -1, true, false, null));
						}
					}
				}
			} else if (currentType != DbSimpleDataType.String && currentType != DbSimpleDataType.Integer && currentType != DbSimpleDataType.BigInteger && currentType != DbSimpleDataType.Float && currentType != DbSimpleDataType.Blob && currentType != DbSimpleDataType.Clob && currentType != DbSimpleDataType.DateTime && StringUtilities.isBlank(formatInfo)) {
				try {
					DateUtilities.parseLocalDate(DateUtilities.getDateFormatPattern(Locale.getDefault()), currentValue.trim());
					dataTypes.put(propertyKey, new DbColumnType("DATE", -1, -1, -1, true, false, null));
				} catch (@SuppressWarnings("unused") final Exception e) {
					if (NumberUtilities.isInteger(currentValue)) {
						dataTypes.put(propertyKey, new DbColumnType("INTEGER", -1, -1, -1, true, false, null));
					} else if (NumberUtilities.isBigInteger(currentValue)) {
						dataTypes.put(propertyKey, new DbColumnType("BIGINT", -1, -1, -1, true, false, null));
					} else if (NumberUtilities.isDouble(currentValue) && currentValue.trim().length() <= 20) {
						dataTypes.put(propertyKey, new DbColumnType("DOUBLE", -1, -1, -1, true, false, null));
					} else {
						dataTypes.put(propertyKey, new DbColumnType("VARCHAR", Math.max(dataTypes.get(propertyKey) == null ? 0 : dataTypes.get(propertyKey).getCharacterByteSize(), currentValue.getBytes(StandardCharsets.UTF_8).length), -1, -1, true, false, null));
					}
				}
			} else if (currentType != DbSimpleDataType.String && currentType != DbSimpleDataType.Date && currentType != DbSimpleDataType.DateTime && currentType != DbSimpleDataType.Float && NumberUtilities.isInteger(currentValue)) {
				dataTypes.put(propertyKey, new DbColumnType("INTEGER", -1, -1, -1, true, false, null));
			} else if (currentType != DbSimpleDataType.String && currentType != DbSimpleDataType.Date && currentType != DbSimpleDataType.DateTime && currentType != DbSimpleDataType.Float && NumberUtilities.isBigInteger(currentValue)) {
				dataTypes.put(propertyKey, new DbColumnType("BIGINT", -1, -1, -1, true, false, null));
			} else if (currentType != DbSimpleDataType.String && currentType != DbSimpleDataType.Date && currentType != DbSimpleDataType.DateTime && NumberUtilities.isDouble(currentValue) && currentValue.trim().length() <= 20) {
				dataTypes.put(propertyKey, new DbColumnType("DOUBLE", -1, -1, -1, true, false, null));
			} else if (currentType != DbSimpleDataType.String && currentType != DbSimpleDataType.Date && currentType != DbSimpleDataType.Float && currentType != DbSimpleDataType.Integer && currentType != DbSimpleDataType.BigInteger && currentType != DbSimpleDataType.DateTime && ("true".equalsIgnoreCase(currentValue) || "false".equalsIgnoreCase(currentValue))) {
				dataTypes.put(propertyKey, new DbColumnType("BOOLEAN", -1, -1, -1, true, false, null));
			} else {
				dataTypes.put(propertyKey, new DbColumnType("VARCHAR", Math.max(dataTypes.get(propertyKey) == null ? 0 : dataTypes.get(propertyKey).getCharacterByteSize(), currentValue.getBytes(StandardCharsets.UTF_8).length), -1, -1, true, false, null));
			}
		}
	}

	/**
	 * Opens the import data, unpacks it if it is compressed and counts the read bytes
	 * (see {@link #getReadDataSize()}).
	 *
	 * @return the input stream of the (uncompressed) import data
	 * @throws Exception if the inline data is empty or the file cannot be opened
	 */
	protected InputStream getInputStream() throws Exception {
		if (isInlineData) {
			if (StringUtilities.isBlank(importInlineData)) {
				throw new DbImportException("Import inline data is empty");
			} else {
				return new ByteArrayInputStream(importInlineData.getBytes(StandardCharsets.UTF_8));
			}
		} else {
			if (!importFile.exists()) {
				throw new DbImportException("Import file does not exist: " + importFile.getAbsolutePath());
			} else if (importFile.isDirectory()) {
				throw new DbImportException("Import path is a directory: " + importFile.getAbsolutePath());
			} else if (importFile.length() == 0) {
				throw new DbImportException("Import file is empty: " + importFile.getAbsolutePath());
			} else {
				try {
					if (StringUtilities.endsWithIgnoreCase(importFile.getAbsolutePath(), ".zip")) {
						if (zipPassword != null) {
							if (Zip4jUtilities.getZipFileEntries(importFile, zipPassword).size() != 1) {
								throw new DbImportException("Compressed import file does not contain a single compressed file: " + importFile.getAbsolutePath());
							} else {
								inputStream = new CountingInputStream(Zip4jUtilities.openPasswordSecuredZipFile(importFile.getAbsolutePath(), zipPassword));
							}
						} else {
							if (ZipUtilities.getZipFileEntries(importFile).size() != 1) {
								throw new DbImportException("Compressed import file does not contain a single compressed file: " + importFile.getAbsolutePath());
							} else {
								inputStream = new CountingInputStream(ZipUtilities.openZipFile(importFile.getAbsolutePath()));
							}
						}
					} else if (StringUtilities.endsWithIgnoreCase(importFile.getAbsolutePath(), ".tar.gz")) {
						if (TarGzUtilities.getFilesCount(importFile) != 1) {
							throw new DbImportException("Compressed import file does not contain a single compressed file: " + importFile.getAbsolutePath());
						} else {
							inputStream = new CountingInputStream(TarGzUtilities.openCompressedFile(importFile));
						}
					} else if (StringUtilities.endsWithIgnoreCase(importFile.getAbsolutePath(), ".tgz")) {
						if (TarGzUtilities.getFilesCount(importFile) != 1) {
							throw new DbImportException("Compressed import file does not contain a single compressed file: " + importFile.getAbsolutePath());
						} else {
							inputStream = new CountingInputStream(TarGzUtilities.openCompressedFile(importFile));
						}
					} else if (StringUtilities.endsWithIgnoreCase(importFile.getAbsolutePath(), ".gz")) {
						inputStream = new CountingInputStream(new GZIPInputStream(new FileInputStream(importFile)));
					} else {
						final InputStreamWithOtherItemsToClose inputStreamWithOtherItemsToClose = new InputStreamWithOtherItemsToClose(new FileInputStream(importFile), importFile.getAbsolutePath());
						inputStream = new CountingInputStream(inputStreamWithOtherItemsToClose);
					}
					return inputStream;
				} catch (final Exception e) {
					if (inputStream != null) {
						try {
							inputStream.close();
						} catch (@SuppressWarnings("unused") final IOException e1) {
							// do nothing
						}
					}
					throw e;
				}
			}
		}
	}

	/**
	 * Returns the configuration of this data provider for the import log.
	 *
	 * @return the configuration as text with one line per parameter
	 */
	public String getConfigurationLogString() {
		if (isInlineData) {
			return "Inline data: true\n";
		} else {
			String configurationLogString = "File: " + importFile.getAbsolutePath() + "\n";
			if (StringUtilities.endsWithIgnoreCase(importFile.getAbsolutePath(), ".zip")) {
				configurationLogString += "Compression: zip\n";
				if (zipPassword != null) {
					configurationLogString += "ZipPassword: true\n";
				}
			} else if (StringUtilities.endsWithIgnoreCase(importFile.getAbsolutePath(), ".tar.gz")) {
				configurationLogString += "Compression: targz\n";
			} else if (StringUtilities.endsWithIgnoreCase(importFile.getAbsolutePath(), ".tgz")) {
				configurationLogString += "Compression: tgz\n";
			} else if (StringUtilities.endsWithIgnoreCase(importFile.getAbsolutePath(), ".gz")) {
				configurationLogString += "Compression: gz\n";
			}
			return configurationLogString;
		}
	}

	@Override
	public void close() {
		if (inputStream != null) {
			try {
				inputStream.close();
			} catch (final Exception e) {
				e.printStackTrace();
			}
			inputStream = null;
		}
	}

	/**
	 * Returns the absolute path of the import file.
	 *
	 * @return the absolute path of the import file
	 * @throws UnsupportedOperationException for inline data
	 */
	public String getImportFilePath() {
		if (isInlineData) {
			throw new UnsupportedOperationException("Import file path is not available for inline data imports");
		} else {
			return importFile.getAbsolutePath();
		}
	}

	/**
	 * Returns the raw import data size.
	 * This means, the data might be compressed and this is the size of the <b>compressed</b> data.
	 *
	 * @return the size of the import file or inline data in bytes
	 */
	public long getImportDataSize() {
		if (isInlineData) {
			return importInlineData.getBytes(StandardCharsets.UTF_8).length;
		} else {
			return importFile.length();
		}
	}

	/**
	 * Returns the real import data size.
	 * This means, the data might be compressed and this is the size of the <b>uncompressed</b> data.
	 *
	 * @return the size of the uncompressed import data in bytes
	 * @throws Exception if the compressed file cannot be read
	 */
	public long getImportDataAmount() throws Exception {
		if (isInlineData) {
			return importInlineData.getBytes(StandardCharsets.UTF_8).length;
		} else {
			if (StringUtilities.endsWithIgnoreCase(importFile.getAbsolutePath(), ".zip")) {
				if (zipPassword != null) {
					return Zip4jUtilities.getUncompressedSize(importFile, zipPassword);
				} else {
					return ZipUtilities.getDataSizeUncompressed(importFile);
				}
			} else if (StringUtilities.endsWithIgnoreCase(importFile.getAbsolutePath(), ".tar.gz")) {
				return TarGzUtilities.getUncompressedSize(importFile);
			} else if (StringUtilities.endsWithIgnoreCase(importFile.getAbsolutePath(), ".tgz")) {
				return TarGzUtilities.getUncompressedSize(importFile);
			} else if (StringUtilities.endsWithIgnoreCase(importFile.getAbsolutePath(), ".gz")) {
				try (InputStream gzStream = new GZIPInputStream(new FileInputStream(importFile))) {
					return IoUtilities.getStreamSize(gzStream);
				}
			} else {
				return importFile.length();
			}
		}
	}

	/**
	 * Returns the number of (uncompressed) bytes read so far.
	 *
	 * @return the number of bytes read so far
	 */
	public long getReadDataSize() {
		if (inputStream != null) {
			return inputStream.getByteCount();
		} else {
			return 0;
		}
	}
}
