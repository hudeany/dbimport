package de.soderer.utilities;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;

public abstract class BasicReader implements Closeable {
	/** Default input encoding. */
	public static final String DEFAULT_ENCODING = "UTF-8";
	
	/** Input stream. */
	private InputStream inputStream;

	/** Input encoding. */
	private Charset encoding;

	/** Input reader. */
	private BufferedReader inputReader = null;
	
	private Character currentChar;
	private Character reuseChar = null;
	private long readCharacters = 0;

	public BasicReader(InputStream inputStream) throws Exception {
		this(inputStream, (String) null);
	}
	
	public BasicReader(InputStream inputStream, String encoding) throws Exception {
		this(inputStream, isBlank(encoding) ? Charset.forName(DEFAULT_ENCODING) : Charset.forName(encoding));
	}
	
	public BasicReader(InputStream inputStream, Charset encodingCharset) throws Exception {
		if (inputStream == null) {
			throw new Exception("Invalid empty inputStream");
		}
		this.inputStream = inputStream;
		this.encoding = encodingCharset == null ? Charset.forName(DEFAULT_ENCODING) : encodingCharset;
	}
	
	public long getReadCharacters() {
		return readCharacters;
	}
	
	public void reuseCurrentChar() {
		reuseChar = currentChar;
		readCharacters--;
	}

	protected Character readNextCharacter() throws IOException {
		if (inputReader == null) {
			if (inputStream == null) {
				throw new IllegalStateException("Reader is already closed");
			}
			inputReader = new BufferedReader(new InputStreamReader(inputStream, encoding));
		}
		
		if (reuseChar != null) {
			currentChar = reuseChar;
			reuseChar = null;
			readCharacters++;
			return currentChar;
		} else {
			int currentCharInt = inputReader.read();
			if (currentCharInt != -1) {
				// Check for UTF-8 BOM at data start
				if (readCharacters == 0 && encoding == Charset.forName("UTF-8") && currentCharInt == Utilities.BOM_UTF_8_CHAR) {
					return readNextCharacter();
				} else {
					currentChar = (char) currentCharInt;
					readCharacters++;
				}
			} else {
				currentChar = null;
			}
			
			return currentChar;
		}
	}

	protected Character readNextNonWhitespace() throws Exception {
		readNextCharacter();
		while (currentChar != null && Character.isWhitespace(currentChar) ) {
			readNextCharacter();
		}
		return currentChar;
	}

	protected String readUpToNext(boolean includeLimitChars, Character escapeCharacter, char... endChars) throws Exception {
		if (Utilities.anyCharsAreEqual(endChars)) {
			throw new Exception("Invalid limit characters");
		} else if (Utilities.contains(endChars, escapeCharacter)) {
			throw new Exception("Invalid escape characters");
		}
		
		StringBuilder returnValue = new StringBuilder();
		returnValue.append(currentChar);
		boolean escapeNextCharacter = false;
		while (true) {
			readNextCharacter();
			if (!escapeNextCharacter) {
				if (escapeCharacter != null && escapeCharacter == currentChar) {
					escapeNextCharacter = true;
				} else {
					for (char endChar : endChars) {
						if (endChar == currentChar) {
							if (includeLimitChars) {
								returnValue.append(currentChar);
							} else {
								reuseCurrentChar();
							}
							return returnValue.toString();
						}
					}
					returnValue.append(currentChar);
				}
			} else {
				if (escapeCharacter == currentChar) {
					returnValue.append(escapeCharacter);
				} else if ('\n' == currentChar) {
					returnValue.append('\n');
				} else if ('\r' == currentChar) {
					returnValue.append(currentChar);
					if ('\n' == readNextCharacter()) {
						returnValue.append(currentChar);
					} else {
						reuseCurrentChar();
					}
				} else {
					for (char endChar : endChars) {
						if (endChar == currentChar) {
							returnValue.append(endChar);
						}
					}
				}
				escapeNextCharacter = false;
			}
		}
	}

	protected String readQuotedText(char quoteChar, Character escapeCharacter) throws Exception {
		if (currentChar != quoteChar) {
			throw new Exception("Invalid start of double-quoted text");
		}
		
		String returnValue = readUpToNext(true, escapeCharacter, quoteChar);
		return returnValue.substring(1, returnValue.length() - 1);
	}

	/**
	 * Close this writer and its underlying stream.
	 */
	@Override
	public void close() {
		closeQuietly(inputReader);
		inputReader = null;
		inputStream = null;
	}

	/**
	 * Check if String value is null or contains only whitespace characters.
	 *
	 * @param value
	 *            the value
	 * @return true, if is blank
	 */
	private static boolean isBlank(String value) {
		return value == null || value.trim().length() == 0;
	}

	/**
	 * Check if String value is not null and has a length greater than 0.
	 *
	 * @param value
	 *            the value
	 * @return true, if is not empty
	 */
	protected static boolean isNotEmpty(String value) {
		return value != null && value.length() > 0;
	}

	/**
	 * Close a Closable item and ignore any Exception thrown by its close method.
	 *
	 * @param closeableItem
	 *            the closeable item
	 */
	private static void closeQuietly(Closeable closeableItem) {
		if (closeableItem != null) {
			try {
				closeableItem.close();
			} catch (IOException e) {
				// Do nothing
			}
		}
	}
}
