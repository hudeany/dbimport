# DbImport

**Import data from files into relational and NoSQL databases: from the command line, an interactive console menu or a Swing GUI.**

DbImport reads CSV, JSON, YAML, XML, Excel, ODS, vCard, KeePass and SQL files (optionally compressed) and writes them into a database table. It maps the data to the table columns, detects duplicates via key columns, inserts or updates existing rows and can create missing tables on the fly.

---

## Features

- **Many data formats**: CSV, JSON, YAML, XML, SQL scripts, Excel (XLS/XLSX), OpenDocument spreadsheets, vCards and KeePass databases
- **Compressed input**: zip (also password protected, AES or ZipCrypto), tar.gz, tgz and gz
- **10 database vendors**: MySQL, MariaDB, Oracle, PostgreSQL, MS SQL Server, Firebird, SQLite, Derby, HSQL and Cassandra
- **Flexible mapping**: automapping by column name or explicit mappings with formats for dates, decimals, files (blobs/clobs), lowercase, uppercase and email
- **Insert, update or upsert**: with duplicate handling in the import data and in the database
- **Table creation**: scans the data and creates a suitable table (and SQLite, Derby or HSQL database), or uses a JSON structure file
- **Fast and robust**: batch updates with automatic fallback to single item mode, so only the erroneous items are skipped
- **Multiple files at once**: wildcards in the file name, each file imported into the table named like the file
- **Error handling**: logging of the import and of the erroneous data items in separate files
- **Secure connections**: TLS/SSL with your own TrustStore, which DbImport can also create for you
- **Extras**: blob import of single files, database connection test and SQL DDL migration/merge script generation (GUI)
- **Three ways to use it**: command line, console menu (also in headless environments) and GUI

## Supported data formats

| Format | File extensions | Notes |
|---|---|---|
| CSV | `.csv` | Configurable separator, string quote, escape character, null value text and headers |
| JSON | `.json` | Array of objects, optional JSON path (`-dp`) and JSON schema validation (`-sp`) |
| YAML | `.yaml`, `.yml` | List of objects, optional data path (`-dp`) |
| XML | `.xml` | Optional XPath (`-dp`) and XSD validation (`-sp`) |
| SQL | `.sql` | The statements of the script are executed one after another |
| Excel | `.xls` (≤ 2003), `.xlsx` (2007+) | Optional sheet name (`-dp`) |
| OpenDocument Spreadsheet | `.ods` | Optional sheet name (`-dp`) |
| vCard | `.vcf` | One data item per vCard |
| KeePass | `.kdbx` | Password via `-kdbxpassword` |

The format is detected by the file extension. If it cannot be detected, CSV is used. You can always set it explicitly with `-x`.

## Supported databases

| Vendor | `dbtype` | Notes |
|---|---|---|
| MySQL | `mysql` | TLS/SSL supported |
| MariaDB | `mariadb` | TLS/SSL supported |
| Oracle | `oracle` | SID, service name (`/<servicename>`) or TNS description, TLS/SSL supported |
| PostgreSQL | `postgresql` | |
| MS SQL Server | `mssql` | TLS/SSL supported |
| Firebird | `firebird` | |
| SQLite | `sqlite` | File based, no host and user needed |
| Derby | `derby` | File based, no host and user needed |
| HSQL | `hsql` | File based |
| Cassandra | `cassandra` | Restricted feature set (see [Import and duplicate modes](#import-and-duplicate-modes)) |

**JDBC drivers:** If the driver of a database vendor is not available, DbImport asks once for the driver jar file (in a file dialog or on the console). The location is stored in `~/.DbImport/.DbImport.config`.

## Requirements

- Java 17 or newer
- The JDBC driver of your database (see above)

## Getting started

```bash
# Open the GUI (also the default when started without parameters)
java -jar DbImport.jar

# Open the console menu (default without parameters in headless environments)
java -jar DbImport.jar menu

# Show the help
java -jar DbImport.jar help
```

### Examples

Import a CSV file into a PostgreSQL table (the password is asked interactively):

```bash
java -jar DbImport.jar postgresql localhost:5432 mydb myuser -table customer -import ./customer.csv
```

Upsert by key column `email`, with a comma as separator and a progress bar:

```bash
java -jar DbImport.jar -v -s ',' -i UPSERT -k email \
  mysql dbserver mydb myuser -table customer -import ./customer.csv
```

Create the table from the data and import a sheet of an Excel file into SQLite:

```bash
java -jar DbImport.jar -create -dp 'Sheet1' sqlite ./data.sqlite -table orders -import ./orders.xlsx
```

Import all CSV files of a directory, each into the table named like the file:

```bash
java -jar DbImport.jar -create postgresql localhost mydb myuser -table '*' -import './export/*.csv'
```

Import with an explicit mapping and formats:

```bash
java -jar DbImport.jar -m 'id="Customer No";birthday="Date of birth" dd.MM.yyyy;mail="E-Mail" email' \
  oracle dbserver /ORCLPDB myuser -table customer -import ./customer.csv
```

## Command line reference

```
java -jar DbImport.jar [optional parameters] dbtype hostname[:port] dbname username -table tablename -import importfilepathOrData [password]
```

### Mandatory parameters

| Parameter | Description |
|---|---|
| `dbtype` | `mysql` \| `mariadb` \| `oracle` \| `postgresql` \| `mssql` \| `firebird` \| `sqlite` \| `derby` \| `hsql` \| `cassandra` |
| `hostname[:port]` | Database host with optional port (not needed for SQLite and Derby) |
| `dbname` | Database name, or file path for SQLite and Derby (Oracle: SID, `/<servicename>` or TNS description) |
| `username` | Database user (not needed for SQLite and Derby) |
| `password` | Asked interactively, if not given (not needed for SQLite and Derby) |
| `-table tablename` | Table to import into (`*` for a table named like each file) |
| `-import importfilepathOrData` | File to import (wildcards `?` and `*` allowed in the file name, automapping only), optionally compressed, or the data itself as text (see `-data`) |

### Data and format

| Parameter | Description |
|---|---|
| `-data` | Declare `importfilepathOrData` explicitly as inline data (no file path) |
| `-x format` | Data format: `CSV` \| `JSON` \| `YAML` \| `XML` \| `SQL` \| `EXCEL` \| `ODS` \| `VCF` \| `KDBX` (default: by file extension, otherwise CSV) |
| `-dp datapath` | Data path within XML (XPath), JSON or YAML data, or the sheet name of Excel/ODS data |
| `-sp schemaFilePath` | Schema file to validate XML (XSD) or JSON data |
| `-e encoding` | Encoding of CSV, JSON and YAML data and of clob files (default: UTF-8) |
| `-s separator` | CSV separator character (default: `;`) |
| `-q quote` | CSV string quote character (default: `"`), `null` or `none` for no quote |
| `-qe escape` | CSV string quote escape character (default: `"`) |
| `-noheaders` | The first CSV line contains data instead of the column names |
| `-noescapesequences` | Do not interpret escape sequences like `\n` or `\t` in CSV string values |
| `-n 'NULL'` | Text for null values (CSV, XML, Excel and ODS, default: empty text) |
| `-a` | Allow underfilled lines (CSV) |
| `-r` | Allow lines with surplus empty trailing columns (CSV) |
| `-t` | Trim data values |
| `-zippassword 'password'` | Password of a zip file |
| `-kdbxpassword 'password'` | Password of a KeePass file |
| `-dbtz 'timezone'` | Time zone of the database (default: system time zone, e.g. `Europe/Berlin`) |
| `-idtz 'timezone'` | Time zone of the import data (default: system time zone) |
| `-dateFormat 'pattern'` | Fallback date format, overridden by mapping formats (Java format characters) |
| `-dateTimeFormat 'pattern'` | Fallback date time format, overridden by mapping formats (Java format characters) |

### Mapping

| Parameter | Description |
|---|---|
| `-m 'mapping'` | Column mappings, separated by semicolon or line break. Without a mapping, the columns are mapped by name (automapping). |
| `-mf mappingFile` | File containing the mapping entries of `-m` |

Mapping entry format: `dbcolumnname="data column name" <formatinfo>`

`<formatinfo>` is optional and may be:

- a decimal delimiter (default `.`)
- a date pattern (default `dd.MM.yyyy HH:mm:ss`)
- `file`: the value is a file path, whose content is imported (blob/clob)
- `lc` / `uc`: convert to lowercase/uppercase
- `email`: validate and normalize as email address

Example: `'db1="def 1" ,;db2="def 2" .;db3="def 3" dd.MM.yyyy HH:mm:ss;db4="def 4" file'`

### Import behaviour

| Parameter | Description |
|---|---|
| `-i importmode` | Import mode, see below (default: `INSERT`) |
| `-d duplicatemode` | Duplicate mode, see below (default: `UPDATE_ALL_JOIN`) |
| `-k 'keycolumns'` | Comma separated key columns for duplicate detection, e.g. `id` or `lower(email)` |
| `-u` | Don't update existing values with null values of the import data |
| `-insvalues 'values'` | Additional values for inserts, semicolon separated, e.g. `id=test_seq.NEXTVAL;flag='abc'` |
| `-updvalues 'values'` | Additional values for updates, semicolon separated, e.g. `changed=current_timestamp;flag='abc'` |
| `-create` | Scan the data and create a suitable table, if it does not exist (also creates SQLite, Derby and HSQL databases) |
| `-structure structureFilePath` | Database structure JSON file used with `-create` |
| `-c` | Complete commit only: rollback on any error (takes more time) |
| `-nonewindex` | Do not create helpful new indexes on the destination table |
| `-deactivatefk` | Deactivate foreign key constraints during the import |
| `-deactivatetriggers` | Deactivate triggers during the import |
| `-batchBlockSize size` | Number of items per batch update (default: 1000) |
| `-noSingleMode` | Do not repeat a failed batch item by item to find the erroneous items |

### Logging and output

| Parameter | Description |
|---|---|
| `-l` | Log the import information in a `.log` file (subdirectory `importlogs` next to the import file) |
| `-logerrors` | Write the erroneous data items to a separate file next to the import file |
| `-v` | Show progress and estimated time on the console |

### Connection security

| Parameter | Description |
|---|---|
| `-secure` | Use TLS/SSL for the database connection |
| `-truststore 'filepath'` | TrustStore (JKS) for encrypted connections |
| `-truststorepassword 'password'` | Optional password of the TrustStore |

### Global parameters

| Parameter | Description |
|---|---|
| `help` | Show the help (only as single parameter) |
| `version` | Show the installed version (only as single parameter) |
| `gui` | Open the GUI (only as first parameter, optionally followed by import parameters to prefill) |
| `menu` | Open the console menu (only as first parameter, optionally followed by import parameters to prefill) |
| `update [username [password]]` | Check for an online update and install it after confirmation |

## Import and duplicate modes

**Import modes** (`-i`):

| Mode | Description |
|---|---|
| `CLEARINSERT` | Deletes all existing data before the insert. If key columns are set, duplicates are prevented. |
| `INSERT` | *(Default)* Inserts all data. If key columns are set, only data not already existing is inserted. |
| `UPDATE` | Updates only existing data, needs key columns. |
| `UPSERT` | Updates existing and inserts new data, needs key columns. |

**Duplicate modes** (`-d`): `DROP` uses only the first of duplicate import data items, `JOIN` combines their values.

| Mode | Checks duplicates in | Database duplicates |
|---|---|---|
| `NO_CHECK` | — (insert only) | — |
| `CHECK_SOURCE_ONLY_DROP` / `_JOIN` | import data | — |
| `UPDATE_FIRST_DROP` / `_JOIN` | import data and database | only the first occurrence is updated |
| `UPDATE_ALL_DROP` / `_JOIN` | import data and database | all occurrences are updated (*default: `UPDATE_ALL_JOIN`*) |
| `MAKE_UNIQUE_DROP` / `_JOIN` | import data and database | duplicate occurrences are removed |

> **Cassandra** only supports the duplicate mode `UPDATE_ALL_JOIN`, updates with null values and commits without `-c`.

## Further tools

### Blob import

Imports a single file into a blob or clob column via an update statement with a `?` placeholder:

```
java -jar DbImport.jar importblob dbtype hostname[:port] dbname username -updatesql sqlUpdateStatementWithPlaceholder -blobfile filePath [password]
```

Example:

```bash
java -jar DbImport.jar importblob postgresql localhost mydb myuser \
  -updatesql "UPDATE documents SET content = ? WHERE id = 42" -blobfile ./contract.pdf
```

### Connection test

Connects to the database repeatedly and optionally executes a check statement:

```
java -jar DbImport.jar connectiontest dbtype hostname[:port] dbname username [-iter n] [-sleep n] [-check checksql] [password]
```

| Parameter | Description |
|---|---|
| `-iter n` | Number of checks (default: 1, 0 = unlimited) |
| `-sleep n` | Seconds to wait after each check (default: 1) |
| `-check checksql` | SQL statement to execute, or `vendor` for the vendor's default check statement |

### Create TrustStore

Creates a TrustStore (JKS) with the certificates of a database server:

```
java -jar DbImport.jar createtruststore hostname:port truststorefilePath [truststorepassword]
```

## Configuration files

DbImport stores its files in `~/.DbImport/`:

| File | Content |
|---|---|
| `.DbImport.config` | Application configuration, e.g. the locations of JDBC driver files |
| `.DbImport.secpref` | Encrypted connection preferences of the GUI and console menu |

