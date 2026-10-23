package de.soderer.dbimport.dataprovider;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Stack;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import javax.xml.XMLConstants;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.transform.stax.StAXSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;

import de.soderer.dbimport.DbImportException;
import de.soderer.utilities.DateUtilities;
import de.soderer.utilities.StringUtilities;
import de.soderer.utilities.Tuple;
import de.soderer.utilities.Utilities;
import de.soderer.utilities.db.data.DbColumnType;
import de.soderer.utilities.xml.IndentedXMLStreamWriter;
import de.soderer.zip.TarGzUtilities;
import de.soderer.zip.Zip4jUtilities;
import de.soderer.zip.ZipUtilities;

/**
 * Data provider for XML data. The data items are the child elements of the data path element.
 */
public class XmlDataProvider extends DataProvider {
	// Default optional parameters
	private String nullValueText = null;

	private XMLStreamReader xmlReader = null;
	private Integer itemsAmount = null;
	private Map<String, DbColumnType> dataTypes = null;
	private List<String> dataPropertyNames = null;
	private String itemEntryTagName = null;
	private String listEnclosingTagName = null;
	private String dataPath = null;
	private String schemaFilePath = null;

	private final Charset encoding = StandardCharsets.UTF_8;

	/**
	 * Creates the data provider.
	 *
	 * @param isInlineData true if importFilePathOrData contains the data itself, false if it is a file path
	 * @param importFilePathOrData path of the import file, or the data itself
	 * @param zipPassword password of a zip import file, or null
	 * @param nullValueText text which represents null values, or null
	 * @param dataPath path of the element containing the data items (e.g. "root/items"), or null for the root element
	 * @param schemaFilePath path of an XSD schema file to validate the data, or null
	 * @throws Exception if the data provider cannot be created
	 */
	public XmlDataProvider(final boolean isInlineData, final String importFilePathOrData, final char[] zipPassword, final String nullValueText, final String dataPath, final String schemaFilePath) throws Exception {
		super(isInlineData, importFilePathOrData, zipPassword);
		this.nullValueText = nullValueText;
		this.dataPath = dataPath;
		this.schemaFilePath = schemaFilePath;
	}

	@Override
	public String getConfigurationLogString() {
		return super.getConfigurationLogString()
				+ "Format: XML" + "\n"
				+ "Encoding: " + encoding + "\n"
				+ "Null value text: " + (nullValueText == null ? "none" : "\"" + nullValueText + "\"") + "\n";
	}

	@Override
	public Map<String, DbColumnType> scanDataPropertyTypes(final Map<String, Tuple<String, String>> mapping) throws Exception {
		if (dataTypes == null) {
			openReader();

			int itemCount = 0;
			dataTypes = new HashMap<>();

			Map<String, Object> nextItem;
			while ((nextItem = getNextItemData()) != null) {
				for (final Entry<String, Object> itemProperty : nextItem.entrySet()) {
					final String propertyName = itemProperty.getKey();
					final Object propertyValue = itemProperty.getValue();
					detectNextDataType(mapping, dataTypes, propertyName, (String) propertyValue);
				}

				itemCount++;
			}

			close();

			itemsAmount = itemCount;
			dataPropertyNames = new ArrayList<>(dataTypes.keySet());
		}

		return dataTypes;
	}

	@Override
	public List<String> getAvailableDataPropertyNames() throws Exception {
		if (dataPropertyNames == null) {
			openReader();

			int itemCount = 0;
			dataPropertyNames = new ArrayList<>();

			Map<String, Object> nextItem;
			while ((nextItem = getNextItemData()) != null) {
				for (final Entry<String, Object> itemProperty : nextItem.entrySet()) {
					final String propertyName = itemProperty.getKey();
					// Each name only once (before, it was added once per data item)
					if (!dataPropertyNames.contains(propertyName)) {
						dataPropertyNames.add(propertyName);
					}
				}

				itemCount++;
			}

			itemsAmount = itemCount;

			close();
		}

		return dataPropertyNames;
	}

	@Override
	public long getItemsAmountToImport() throws Exception {
		if (itemsAmount == null) {
			getAvailableDataPropertyNames();
		}

		return itemsAmount;
	}

	@Override
	public String getItemsUnitSign() throws Exception {
		return null;
	}

	@Override
	public Map<String, Object> getNextItemData() throws Exception {
		if (xmlReader == null) {
			openReader();
		}

		// Read item entries
		if (xmlReader.nextTag() > 0) {
			if (xmlReader.isStartElement()) {
				if (itemEntryTagName == null) {
					itemEntryTagName = xmlReader.getLocalName();
				} else if (!itemEntryTagName.equals(xmlReader.getLocalName())) {
					throw new DbImportException("Invalid xml data. tagname: " + xmlReader.getLocalName());
				}
			} else if (xmlReader.isEndElement()) {
				return null;
			}

			// Read properties
			final Map<String, Object> returnMap = new HashMap<>();
			while (xmlReader.next() > 0) {
				if (xmlReader.isStartElement()) {
					final String propertyName = xmlReader.getLocalName();
					xmlReader.next();
					if (xmlReader.isCharacters()) {
						String value = xmlReader.getText();
						if (nullValueText != null && nullValueText.equals(value)) {
							value = null;
						}
						returnMap.put(propertyName, value);

						xmlReader.next();
						if (!xmlReader.isEndElement()) {
							throw new DbImportException("Invalid xml data. Missing closing tag for: " + propertyName);
						}
					} else if (xmlReader.isEndElement()) {
						returnMap.put(propertyName, "");
					} else {
						throw new DbImportException("Invalid xml data");
					}
				} else if (xmlReader.isEndElement()) {
					break;
				} else {
					throw new DbImportException("Invalid xml data");
				}
			}
			return returnMap;
		} else {
			return null;
		}
	}

	@Override
	public void close() {
		if (xmlReader != null) {
			// Caution: XMLStreamReader.close() doesn't close the underlying stream
			try {
				xmlReader.close();
			} catch (@SuppressWarnings("unused") final XMLStreamException e) {
				// Do nothing
			}
			xmlReader = null;
		}
		super.close();
	}

	@Override
	public File filterDataItems(final List<Integer> indexList, final String fileSuffix) throws Exception {
		OutputStream outputStream = null;
		File tempFile = null;
		try {
			// The reader of the import run is still open at this point (positioned at the end of the data), so start
			// a fresh one. Before, openReader() failed with "Reader was already opened before" and the erroneous data
			// file was never written.
			close();
			openReader();

			File filteredDataFile;
			if (StringUtilities.endsWithIgnoreCase(getImportFilePath(), ".zip")) {
				filteredDataFile = new File(getImportFilePath() + "." + fileSuffix + ".xml.zip");
				outputStream = ZipUtilities.openNewZipOutputStream(filteredDataFile, null);
				((ZipOutputStream) outputStream).putNextEntry(new ZipEntry(new File(getImportFilePath() + "." + fileSuffix + ".xml").getName()));
			} else if (StringUtilities.endsWithIgnoreCase(getImportFilePath(), ".tar.gz")) {
				filteredDataFile = new File(getImportFilePath() + "." + fileSuffix + ".xml.tar.gz");
				tempFile = File.createTempFile(new File(getImportFilePath()).getName(), fileSuffix);
				outputStream = new FileOutputStream(tempFile);
			} else if (StringUtilities.endsWithIgnoreCase(getImportFilePath(), ".tgz")) {
				filteredDataFile = new File(getImportFilePath() + "." + fileSuffix + ".xml.tgz");
				tempFile = File.createTempFile(new File(getImportFilePath()).getName(), fileSuffix);
				outputStream = new FileOutputStream(tempFile);
			} else if (StringUtilities.endsWithIgnoreCase(getImportFilePath(), ".gz")) {
				filteredDataFile = new File(getImportFilePath() + "." + fileSuffix + ".xml.gz");
				outputStream = new GZIPOutputStream(new FileOutputStream(filteredDataFile));
			} else {
				filteredDataFile = new File(getImportFilePath() + "." + fileSuffix + ".xml");
				outputStream = new FileOutputStream(filteredDataFile);
			}

			try (IndentedXMLStreamWriter xmlWriter = new IndentedXMLStreamWriter(outputStream, encoding, "\t")) {
				xmlWriter.writeStartDocument(encoding.name(), "1.0");
				xmlWriter.writeStartElement(listEnclosingTagName);

				Map<String, Object> item;
				int itemIndex = 0;
				while ((item = getNextItemData()) != null) {
					itemIndex++;
					if (indexList.contains(itemIndex)) {
						xmlWriter.writeStartElement("entry");

						for (final Entry<String, Object> entry : item.entrySet()) {
							xmlWriter.writeStartElement(entry.getKey());
							if (entry.getValue() == null) {
								xmlWriter.writeCharacters(nullValueText);
							} else if (entry.getValue() instanceof Date) {
								xmlWriter.writeCharacters(DateUtilities.formatDate(DateUtilities.YYYY_MM_DD_HHMMSS, (Date) entry.getValue()));
							} else if (entry.getValue() instanceof Number) {
								xmlWriter.writeCharacters(entry.getValue().toString());
							} else if (entry.getValue() instanceof String) {
								xmlWriter.writeCharacters((String) entry.getValue());
							} else {
								xmlWriter.writeCharacters(entry.getValue().toString());
							}
							xmlWriter.writeEndElement();
						}

						xmlWriter.writeEndElement();
					}
				}
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

	private void openReader() throws Exception {
		if (xmlReader != null) {
			throw new Exception("Reader was already opened before");
		}

		try {
			if (StringUtilities.isNotEmpty(schemaFilePath)) {
				XMLStreamReader xmlStreamReader = null;
				try (InputStream validationStream = getInputStream()) {
					final XMLInputFactory validationXmlInputFactory = XMLInputFactory.newInstance();
					validationXmlInputFactory.setProperty(XMLInputFactory.SUPPORT_DTD, false);
					validationXmlInputFactory.setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, false);
					xmlStreamReader = validationXmlInputFactory.createXMLStreamReader(validationStream);
					final SchemaFactory factory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
					final Schema schema = factory.newSchema(new File(schemaFilePath));

					final Validator validator = schema.newValidator();
					validator.validate(new StAXSource(xmlStreamReader));
				} catch (final Exception e) {
					throw new DbImportException("XML data does not comply to XSD '" + schemaFilePath + "': " + e.getMessage());
				} finally {
					if (xmlStreamReader != null) {
						xmlStreamReader.close();
					}
				}
			}

			final XMLInputFactory xmlInputFactory = XMLInputFactory.newInstance();
			xmlInputFactory.setProperty(XMLInputFactory.SUPPORT_DTD, false);
			xmlInputFactory.setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, false);
			// getNextItemData() expects the whole text of a value element in one CHARACTERS event. Without coalescing,
			// the parser may split it (e.g. around "&amp;" or CDATA sections) and the value was rejected.
			xmlInputFactory.setProperty(XMLInputFactory.IS_COALESCING, true);
			xmlReader = xmlInputFactory.createXMLStreamReader(getInputStream());

			if (StringUtilities.isNotEmpty(dataPath)) {
				// Read xml path
				if (dataPath.startsWith("/")) {
					dataPath = dataPath.substring(1);
				}
				if (dataPath.endsWith("/")) {
					dataPath = dataPath.substring(0, dataPath.length() - 1);
				}
				final Stack<String> currentPath = new Stack<>();
				xmlReader.nextTag();
				currentPath.push(xmlReader.getLocalName());
				while (currentPath.size() > 0 && !StringUtilities.join(currentPath, "/").equals(dataPath)) {
					if (XMLStreamConstants.START_ELEMENT == xmlReader.nextTag()) {
						currentPath.push(xmlReader.getLocalName());
					} else {
						currentPath.pop();
					}
				}
				if (currentPath.size() == 0) {
					throw new DbImportException("Path '" + dataPath + "' is not part of the xml data");
				}
			} else {
				// Read root tag
				xmlReader.nextTag();
			}
			listEnclosingTagName = xmlReader.getLocalName();
			if (!xmlReader.isStartElement()) {
				throw new DbImportException("Invalid xml data. roottagname: " + xmlReader.getLocalName());
			}
		} catch (final Exception e) {
			close();
			throw e;
		}
	}
}
