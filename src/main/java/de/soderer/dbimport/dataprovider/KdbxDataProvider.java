package de.soderer.dbimport.dataprovider;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import de.soderer.utilities.DateUtilities;
import de.soderer.utilities.StringUtilities;
import de.soderer.utilities.Tuple;
import de.soderer.utilities.Utilities;
import de.soderer.utilities.db.data.DbColumnType;
import de.soderer.utilities.db.data.DbSimpleDataType;
import de.soderer.utilities.kdbx.KdbxDatabase;
import de.soderer.utilities.kdbx.KdbxReader;
import de.soderer.utilities.kdbx.KdbxWriter;
import de.soderer.utilities.kdbx.data.KdbxEntry;
import de.soderer.utilities.kdbx.data.KdbxGroup;
import de.soderer.zip.TarGzUtilities;
import de.soderer.zip.Zip4jUtilities;
import de.soderer.zip.ZipUtilities;

/**
 * Data provider for KeePass database files (KDBX format). The data items are the entries of the database.
 * <p>
 * Optionally the group path of each entry is provided as additional data property (e.g. "Internet/Mail").
 * If the database has a single top level group and no entries on root level (the standard KeePass structure),
 * this root group is not part of the path, matching the group path column of DbExport.
 */
public class KdbxDataProvider extends DataProvider {
	/** Default separator of group names within a group path */
	public static final String DEFAULT_GROUP_PATH_SEPARATOR = "/";

	/**
	 * Entry of the database together with the chain of its parent groups, from the top level group down to the direct parent group.
	 */
	private record KdbxEntryWithGroups(KdbxEntry entry, List<KdbxGroup> groupChain) {
	}

	private KdbxDatabase kdbxDatabase = null;
	private Iterator<KdbxEntryWithGroups> kdbxEntryIterator = null;
	/** True if the single top level group is the root group, which is not part of the group path */
	private boolean hasSingleRootGroup = false;
	private List<String> dataPropertyNames = null;
	private Map<String, DbColumnType> dataTypes = null;
	private Integer itemsAmount = null;
	private char[] password = null;

	/** Name of the additional data property containing the group path of each entry, null for none */
	private String groupPathPropertyName = null;
	/** Separator of group names within a group path */
	private String groupPathSeparator = DEFAULT_GROUP_PATH_SEPARATOR;

	/**
	 * Creates the data provider.
	 *
	 * @param importFilePath path of the KeePass database file
	 * @param password password of the KeePass database
	 */
	public KdbxDataProvider(final String importFilePath, final char[] password) {
		super(false, importFilePath, null);
		this.password = password;
	}

	/**
	 * Sets the name of the additional data property containing the group path of each entry.
	 *
	 * @param groupPathPropertyName name of the data property, or null or blank for no group path property
	 */
	public void setGroupPathPropertyName(final String groupPathPropertyName) {
		this.groupPathPropertyName = groupPathPropertyName == null || groupPathPropertyName.isBlank() ? null : groupPathPropertyName.trim();
	}

	/**
	 * Sets the separator of group names within a group path.
	 *
	 * @param groupPathSeparator the separator, default "/"
	 */
	public void setGroupPathSeparator(final String groupPathSeparator) {
		if (groupPathSeparator == null || groupPathSeparator.isEmpty()) {
			throw new IllegalArgumentException("Group path separator must not be empty");
		} else {
			this.groupPathSeparator = groupPathSeparator;
		}
	}

	@Override
	public String getConfigurationLogString() {
		String configurationLogString = super.getConfigurationLogString()
				+ "Format: Kdbx" + "\n";
		if (groupPathPropertyName != null) {
			configurationLogString += "KdbxGroupPathProperty: " + groupPathPropertyName + "\n"
					+ "KdbxGroupPathSeparator: " + groupPathSeparator + "\n";
		}
		return configurationLogString;
	}

	@Override
	public Map<String, DbColumnType> scanDataPropertyTypes(final Map<String, Tuple<String, String>> mapping) throws Exception {
		if (dataTypes == null) {
			openKdbxDatabase();

			int itemCount = 0;
			dataPropertyNames = new ArrayList<>();
			dataTypes = new HashMap<>();

			Map<String, Object> nextItem;
			while ((nextItem = getNextItemData()) != null) {
				for (final Entry<String, Object> itemProperty : nextItem.entrySet()) {
					final String propertyName = itemProperty.getKey();
					final Object propertyValue = itemProperty.getValue();

					String formatInfo = null;
					if (mapping != null) {
						for (final Tuple<String, String> mappingValue : mapping.values()) {
							if (mappingValue.getFirst().equals(propertyName)) {
								if (StringUtilities.isNotBlank(mappingValue.getSecond())) {
									formatInfo = mappingValue.getSecond();
									break;
								}
							}
						}
					}

					final DbSimpleDataType currentType = dataTypes.get(propertyName) == null ? null : dataTypes.get(propertyName).getSimpleDataType();
					if (currentType != DbSimpleDataType.Blob) {
						if (propertyValue == null) {
							if (!dataTypes.containsKey(propertyName)) {
								dataTypes.put(propertyName, null);
							}
						} else if ("file".equalsIgnoreCase(formatInfo) || (propertyValue instanceof String && ((String) propertyValue).length() > 4000)) {
							dataTypes.put(propertyName, new DbColumnType("BLOB", -1, -1, -1, true, false, null));
						} else if (currentType != DbSimpleDataType.String && StringUtilities.isNotBlank(formatInfo) && !".".equals(formatInfo) && !",".equals(formatInfo) && !"file".equalsIgnoreCase(formatInfo) && propertyValue instanceof String) {
							final String value = ((String) propertyValue).trim();
							try {
								DateUtilities.parseLocalDateTime(formatInfo, value);
								dataTypes.put(propertyName, new DbColumnType("TIMESTAMP", -1, -1, -1, true, false, null));
							} catch (@SuppressWarnings("unused") final Exception e) {
								try {
									DateUtilities.parseLocalDateTime(DateUtilities.ISO_8601_DATETIME_FORMAT, value);
									dataTypes.put(propertyName, new DbColumnType("TIMESTAMP", -1, -1, -1, true, false, null));
								} catch (@SuppressWarnings("unused") final Exception e2) {
									dataTypes.put(propertyName, new DbColumnType("VARCHAR", Math.max(dataTypes.get(propertyName) == null ? 0 : dataTypes.get(propertyName).getCharacterByteSize(), value.getBytes(StandardCharsets.UTF_8).length), -1, -1, true, false, null));
								}
							}
						} else if (currentType != DbSimpleDataType.String && StringUtilities.isBlank(formatInfo) && propertyValue instanceof String) {
							final String value = ((String) propertyValue).trim();
							try {
								DateUtilities.parseLocalDateTime(DateUtilities.ISO_8601_DATETIME_FORMAT, value);
								dataTypes.put(propertyName, new DbColumnType("TIMESTAMP", -1, -1, -1, true, false, null));
							} catch (@SuppressWarnings("unused") final Exception e) {
								try {
									DateUtilities.parseDateTime(DateUtilities.ISO_8601_DATE_FORMAT, value);
									dataTypes.put(propertyName, new DbColumnType("DATE", -1, -1, -1, true, false, null));
								} catch (@SuppressWarnings("unused") final Exception e2) {
									dataTypes.put(propertyName, new DbColumnType("VARCHAR", Math.max(dataTypes.get(propertyName) == null ? 0 : dataTypes.get(propertyName).getCharacterByteSize(), value.getBytes(StandardCharsets.UTF_8).length), -1, -1, true, false, null));
								}
							}
						} else if (currentType != DbSimpleDataType.String && currentType != DbSimpleDataType.DateTime && currentType != DbSimpleDataType.Float && propertyValue instanceof Integer) {
							dataTypes.put(propertyName, new DbColumnType("INTEGER", -1, -1, -1, true, false, null));
						} else if (currentType != DbSimpleDataType.String && currentType != DbSimpleDataType.DateTime && (propertyValue instanceof Float || propertyValue instanceof Double)) {
							dataTypes.put(propertyName, new DbColumnType("DOUBLE", -1, -1, -1, true, false, null));
						} else if (currentType != DbSimpleDataType.String && currentType != DbSimpleDataType.Date && currentType != DbSimpleDataType.Float && currentType != DbSimpleDataType.Integer && currentType != DbSimpleDataType.BigInteger && currentType != DbSimpleDataType.DateTime && propertyValue instanceof Boolean) {
							dataTypes.put(propertyName, new DbColumnType("BOOLEAN", -1, -1, -1, true, false, null));
						} else {
							dataTypes.put(propertyName, new DbColumnType("VARCHAR", Math.max(dataTypes.get(propertyName) == null ? 0 : dataTypes.get(propertyName).getCharacterByteSize(), propertyValue.toString().getBytes(StandardCharsets.UTF_8).length), -1, -1, true, false, null));
						}
					}
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
			openKdbxDatabase();

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

			close();

			itemsAmount = itemCount;
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
		if (kdbxEntryIterator == null) {
			openKdbxDatabase();
		}

		if (!kdbxEntryIterator.hasNext()) {
			return null;
		}

		final KdbxEntryWithGroups kdbxEntryWithGroups = kdbxEntryIterator.next();
		final KdbxEntry kdbxEntry = kdbxEntryWithGroups.entry();

		final Map<String, Object> returnMap = new HashMap<>();
		for (final Entry<String, Object> itemEntry : kdbxEntry.getItems().entrySet()) {
			returnMap.put(itemEntry.getKey(), itemEntry.getValue());
		}

		if (groupPathPropertyName != null) {
			if (returnMap.containsKey(groupPathPropertyName)) {
				throw new Exception("Entry '" + kdbxEntry.getTitle() + "' (" + kdbxEntry.getUuid().toHex() + ") already contains a field named like the group path property '" + groupPathPropertyName + "'");
			}
			returnMap.put(groupPathPropertyName, getGroupPath(kdbxEntryWithGroups.groupChain()));
		}

		return returnMap;
	}

	/**
	 * Creates the group path of an entry, leaving out the root group of a standard KeePass structure.
	 *
	 * @param groupChain parent groups of the entry from top level down to the direct parent group
	 * @return the group path, empty for entries in the root group or on root level of the database
	 */
	private String getGroupPath(final List<KdbxGroup> groupChain) {
		final StringBuilder groupPath = new StringBuilder();
		for (int i = hasSingleRootGroup ? 1 : 0; i < groupChain.size(); i++) {
			if (groupPath.length() > 0) {
				groupPath.append(groupPathSeparator);
			}
			groupPath.append(groupChain.get(i).getName() == null ? "" : groupChain.get(i).getName());
		}
		return groupPath.toString();
	}

	@Override
	public void close() {
		kdbxDatabase = null;
		kdbxEntryIterator = null;
		hasSingleRootGroup = false;
		super.close();
	}

	@Override
	public File filterDataItems(final List<Integer> indexList, final String fileSuffix) throws Exception {
		OutputStream outputStream = null;
		File tempFile = null;
		try {
			// The database of the import run is still open at this point, so open it again from the start.
			// Before, openKdbxDatabase() failed with "KdbxDatabase was already opened before".
			close();
			openKdbxDatabase();

			File filteredDataFile;
			if (StringUtilities.endsWithIgnoreCase(getImportFilePath(), ".zip")) {
				filteredDataFile = new File(getImportFilePath() + "." + fileSuffix + ".kdbx.zip");
				outputStream = ZipUtilities.openNewZipOutputStream(filteredDataFile, null);
				((ZipOutputStream) outputStream).putNextEntry(new ZipEntry(new File(getImportFilePath() + "." + fileSuffix + ".kdbx").getName()));
			} else if (StringUtilities.endsWithIgnoreCase(getImportFilePath(), ".tar.gz")) {
				filteredDataFile = new File(getImportFilePath() + "." + fileSuffix + ".kdbx.tar.gz");
				tempFile = File.createTempFile(new File(getImportFilePath()).getName(), fileSuffix);
				outputStream = new FileOutputStream(tempFile);
			} else if (StringUtilities.endsWithIgnoreCase(getImportFilePath(), ".tgz")) {
				filteredDataFile = new File(getImportFilePath() + "." + fileSuffix + ".kdbx.tgz");
				tempFile = File.createTempFile(new File(getImportFilePath()).getName(), fileSuffix);
				outputStream = new FileOutputStream(tempFile);
			} else if (StringUtilities.endsWithIgnoreCase(getImportFilePath(), ".gz")) {
				filteredDataFile = new File(getImportFilePath() + "." + fileSuffix + ".kdbx.gz");
				outputStream = new GZIPOutputStream(new FileOutputStream(filteredDataFile));
			} else {
				filteredDataFile = new File(getImportFilePath() + "." + fileSuffix + ".kdbx");
				outputStream = new FileOutputStream(filteredDataFile);
			}

			final KdbxDatabase filteredKdbxDatabase = new KdbxDatabase();
			// Copies of the original groups, so the filtered entries keep their group structure
			final Map<KdbxGroup, KdbxGroup> groupCopies = new IdentityHashMap<>();
			// The item indices are 1-based (like in the other data providers), so count before checking
			int count = 0;
			for (final KdbxEntryWithGroups entryWithGroups : collectEntriesWithGroups(kdbxDatabase)) {
				count++;
				if (indexList.contains(count)) {
					getOrCreateGroupCopy(filteredKdbxDatabase, groupCopies, entryWithGroups.groupChain()).add(entryWithGroups.entry());
				}
			}

			try (KdbxWriter kdbxWriter = new KdbxWriter(outputStream)) {
				kdbxWriter.writeKdbxDatabase(filteredKdbxDatabase, password);
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
	 * Returns the entry list of the copied group chain in the filtered database, creating missing group copies on the way.
	 *
	 * @param filteredKdbxDatabase the filtered database
	 * @param groupCopies already created group copies by original group
	 * @param groupChain parent groups of the entry in the original database
	 * @return the list to add the entry to
	 */
	private static List<KdbxEntry> getOrCreateGroupCopy(final KdbxDatabase filteredKdbxDatabase, final Map<KdbxGroup, KdbxGroup> groupCopies, final List<KdbxGroup> groupChain) {
		List<KdbxGroup> siblings = filteredKdbxDatabase.getGroups();
		List<KdbxEntry> entries = filteredKdbxDatabase.getEntries();
		for (final KdbxGroup originalGroup : groupChain) {
			KdbxGroup groupCopy = groupCopies.get(originalGroup);
			if (groupCopy == null) {
				// Only the descriptive group data is copied, not the subgroups and entries
				groupCopy = new KdbxGroup()
						.withName(originalGroup.getName())
						.withUuid(originalGroup.getUuid())
						.withNotes(originalGroup.getNotes())
						.withIconID(originalGroup.getIconID())
						.withExpanded(originalGroup.isExpanded());
				siblings.add(groupCopy);
				groupCopies.put(originalGroup, groupCopy);
			}
			siblings = groupCopy.getGroups();
			entries = groupCopy.getEntries();
		}
		return entries;
	}

	/**
	 * Collects all entries of the database with their parent groups.
	 * The order is the same as in {@link KdbxDatabase#getAllEntries()}, so item indices stay comparable.
	 *
	 * @param database the database
	 * @return all entries with their group chains
	 */
	private static List<KdbxEntryWithGroups> collectEntriesWithGroups(final KdbxDatabase database) {
		final List<KdbxEntryWithGroups> result = new ArrayList<>();
		for (final KdbxEntry entry : database.getEntries()) {
			result.add(new KdbxEntryWithGroups(entry, List.of()));
		}
		for (final KdbxGroup group : database.getGroups()) {
			collectEntriesWithGroups(group, new ArrayList<>(), result);
		}
		return result;
	}

	/**
	 * Collects the entries of a group and its subgroups recursively: first the entries of the group itself, then those of the subgroups.
	 *
	 * @param group the group
	 * @param parentChain parent groups of this group
	 * @param result list to add the entries to
	 */
	private static void collectEntriesWithGroups(final KdbxGroup group, final List<KdbxGroup> parentChain, final List<KdbxEntryWithGroups> result) {
		final List<KdbxGroup> groupChain = new ArrayList<>(parentChain);
		groupChain.add(group);
		final List<KdbxGroup> unmodifiableGroupChain = List.copyOf(groupChain);
		for (final KdbxEntry entry : group.getEntries()) {
			result.add(new KdbxEntryWithGroups(entry, unmodifiableGroupChain));
		}
		for (final KdbxGroup subGroup : group.getGroups()) {
			collectEntriesWithGroups(subGroup, groupChain, result);
		}
	}

	private void openKdbxDatabase() throws Exception {
		if (kdbxEntryIterator != null) {
			throw new Exception("KdbxDatabase was already opened before");
		}

		try (KdbxReader kdbxReader = new KdbxReader(getInputStream())) {
			kdbxDatabase = kdbxReader.readKdbxDatabase(password);
			hasSingleRootGroup = kdbxDatabase.getEntries().isEmpty() && kdbxDatabase.getGroups().size() == 1;
			kdbxEntryIterator = collectEntriesWithGroups(kdbxDatabase).iterator();
		} catch (final Exception e) {
			close();
			throw e;
		}
	}
}
