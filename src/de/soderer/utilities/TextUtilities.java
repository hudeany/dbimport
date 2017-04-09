package de.soderer.utilities;

import java.io.IOException;
import java.io.InputStream;
import java.io.LineNumberReader;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Text Utilities
 *
 * This class does no Logging via Log4J, because it is often used before its initialisation
 */
public class TextUtilities {
	/**
	 * A simple string for testing, which includes all ASCII characters
	 */
	public static final String ASCII_CHARACTERS_STRING = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 !\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~";

	/**
	 * A simple string for testing, which includes all german characters
	 */
	public static final String GERMAN_TEST_STRING = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 äöüßÄÖÜµ!?§@€$%&/\\<>(){}[]'\"´`^°¹²³*#.,;:=+-~_|½¼¬";

	/**
	 * Enum to represent a linebreak type of an text
	 */
	public enum LineBreakType {
		Unknown, Mixed, Unix, Mac, Windows;

		public static LineBreakType getLineBreakTypeByName(String lineBreakTypeName) {
			if ("WINDOWS".equalsIgnoreCase(lineBreakTypeName)) {
				return LineBreakType.Windows;
			} else if ("UNIX".equalsIgnoreCase(lineBreakTypeName)) {
				return LineBreakType.Unix;
			} else if ("MAC".equalsIgnoreCase(lineBreakTypeName)) {
				return LineBreakType.Mac;
			} else {
				return LineBreakType.Unknown;
			}
		}
	}

	/**
	 * Mac/Apple linebreak character
	 */
	public static String LinebreakMac = "\r";

	/**
	 * Unix/Linux linebreak character
	 */
	public static String LinebreakUnix = "\n";

	/**
	 * Windows linebreak characters
	 */
	public static String LinebreakWindows = "\r\n";

	/**
	 * Trim a string to an exact length with alignment right for display purposes If the string underruns the length, it will be filled up with blanks on the left side. If the string exceeds the
	 * length, it will be cut from the left side.
	 *
	 * @param inputString
	 * @param length
	 * @return
	 */
	public static String trimStringToLengthAlignedRight(String inputString, int length) {
		if (inputString.length() > length) {
			// Takes the last characters of length
			return inputString.subSequence(inputString.length() - length, inputString.length()).toString();
		} else {
			return String.format("%" + length + "s", inputString);
		}
	}

	/**
	 * Trim a string to a exact length with alignment left for display purposes. If the string underruns the length, it will be filled up with blanks on the right side. If the string exceeds the
	 * length, it will be cut from the right side.
	 *
	 * @param inputString
	 * @param length
	 * @return
	 */
	public static String trimStringToLengthAlignedLeft(String inputString, int length) {
		if (inputString.length() > length) {
			// Takes the first characters of length
			return inputString.subSequence(0, length).toString();
		} else {
			return String.format("%-" + length + "s", inputString);
		}
	}

	/**
	 * Build a string with repetitions. 0 repetitions returns an empty string.
	 *
	 * @param itemString
	 * @param repeatTimes
	 * @return
	 */
	public static String repeatString(String itemString, int repeatTimes) {
		return repeatString(itemString, repeatTimes, null);
	}

	/***
	 * Build a string with repetitions. 0 repetitions returns an empty string. In other cases there will be put a glue string between the reptitions, which can be left empty.
	 *
	 * @param itemString
	 *            string to be repeated
	 * @param separatorString
	 *            glue string
	 * @param repeatTimes
	 *            Number of repetitions
	 * @return
	 */
	public static String repeatString(String itemString, int repeatTimes, String separatorString) {
		StringBuilder returnStringBuilder = new StringBuilder();
		for (int i = 0; i < repeatTimes; i++) {
			if (separatorString != null && returnStringBuilder.length() > 0) {
				returnStringBuilder.append(separatorString);
			}
			returnStringBuilder.append(itemString);
		}
		return returnStringBuilder.toString();
	}

	/**
	 * Split a string into smaller strings of a maximum size
	 *
	 * @param text
	 * @param chunkSize
	 * @return
	 */
	public static List<String> chopToChunks(String text, int chunkSize) {
		List<String> returnList = new ArrayList<String>((text.length() + chunkSize - 1) / chunkSize);

		for (int start = 0; start < text.length(); start += chunkSize) {
			returnList.add(text.substring(start, Math.min(text.length(), start + chunkSize)));
		}

		return returnList;
	}

	/**
	 * Reduce a text to a maximum number of lines. The type of linebreaks in the result string will not be changed. If the text has less lines then maximum it will be returned unchanged.
	 *
	 * @param value
	 * @param maxLines
	 * @return
	 */
	public static String trimToMaxNumberOfLines(String value, int maxLines) {
		String normalizedValue = normalizeLineBreaks(value, LineBreakType.Unix);
		int count = 0;
		int nextLinebreak = 0;
		while (nextLinebreak != -1 && count < maxLines) {
			nextLinebreak = normalizedValue.indexOf(LinebreakUnix, nextLinebreak + 1);
			count++;
		}

		if (nextLinebreak != -1) {
			LineBreakType originalLineBreakType = detectLinebreakType(value);
			return normalizeLineBreaks(normalizedValue.substring(0, nextLinebreak + 1), originalLineBreakType) + "...";
		} else {
			return value;
		}
	}

	/**
	 * Detect the type of linebreaks in a text.
	 *
	 * @param value
	 * @return
	 */
	public static LineBreakType detectLinebreakType(String value) {
		TextPropertiesReader textPropertiesReader = new TextPropertiesReader(value);
		textPropertiesReader.readProperties();
		return textPropertiesReader.getLinebreakType();
	}

	/**
	 * Change all linebreaks of a text, no matter what type they are, to a given type
	 *
	 * @param value
	 * @param type
	 * @return
	 */
	public static String normalizeLineBreaks(String value, LineBreakType type) {
		String returnString = value.replace(LinebreakWindows, LinebreakUnix).replace(LinebreakMac, LinebreakUnix);
		if (type == LineBreakType.Mac) {
			return returnString.replace(LinebreakUnix, LinebreakMac);
		} else if (type == LineBreakType.Windows) {
			return returnString.replace(LinebreakUnix, LinebreakWindows);
		} else {
			return returnString;
		}
	}

	/**
	 * Get the text lines of a text as a list of strings
	 *
	 * @param dataString
	 * @return
	 * @throws IOException
	 */
	public static List<String> getLines(String dataString) throws IOException {
		List<String> list = new ArrayList<String>();
		LineNumberReader lineNumberReader = null;
		try {
			lineNumberReader = new LineNumberReader(new StringReader(dataString));
			String line;
			while ((line = lineNumberReader.readLine()) != null) {
				list.add(line);
			}

			return list;
		} finally {
			Utilities.closeQuietly(lineNumberReader);
		}
	}

	/**
	 * Get the number of lines in a text
	 *
	 * @param dataString
	 * @return
	 * @throws IOException
	 */
	public static int getLineCount(String dataString) throws IOException {
		if (dataString == null) {
			return 0;
		} else if ("".equals(dataString)) {
			return 1;
		} else {
			LineNumberReader lineNumberReader = null;
			try {
				lineNumberReader = new LineNumberReader(new StringReader(dataString));
				while (lineNumberReader.readLine() != null) {
					// do nothing
				}

				return lineNumberReader.getLineNumber();
			} finally {
				Utilities.closeQuietly(lineNumberReader);
			}
		}
	}

	/**
	 * Count the number of words by whitespaces in a text separated
	 *
	 * @param dataString
	 * @return
	 */
	public static int countWords(String dataString) {
		int counter = 0;
		boolean isWord = false;
		int endOfLine = dataString.length() - 1;

		for (int i = 0; i < dataString.length(); i++) {
			if (!Character.isWhitespace(dataString.charAt(i)) && i != endOfLine) {
				isWord = true;
			} else if (Character.isWhitespace(dataString.charAt(i)) && isWord) {
				counter++;
				isWord = false;
			} else if (!Character.isWhitespace(dataString.charAt(i)) && i == endOfLine) {
				counter++;
			}
		}
		return counter;
	}

	/**
	 * Get the start index of a line within a text
	 *
	 * @param dataString
	 * @param lineNumber
	 * @return
	 */
	public static int getStartIndexOfLine(String dataString, int lineNumber) {
		if (dataString == null || lineNumber < 0) {
			return -1;
		} else if (lineNumber == 1) {
			return 0;
		} else {
			int lineCount = 1;
			int position = 0;
			while (lineCount < lineNumber) {
				int nextLineBreakMac = dataString.indexOf(LinebreakMac, position);
				int nextLineBreakUnix = dataString.indexOf(LinebreakUnix, position);
				int nextLineBreakWindows = dataString.indexOf(LinebreakWindows, position);
				int nextPosition = -1;
				int lineBreakSize = 0;
				if (nextLineBreakMac >= 0 && (nextLineBreakUnix < 0 || nextLineBreakMac < nextLineBreakUnix) && (nextLineBreakWindows < 0 || nextLineBreakMac < nextLineBreakWindows)) {
					nextPosition = nextLineBreakMac;
					lineBreakSize = 1;
				} else if (nextLineBreakUnix >= 0 && (nextLineBreakWindows < 0 || nextLineBreakUnix < nextLineBreakWindows)) {
					nextPosition = nextLineBreakUnix;
					lineBreakSize = 1;
				} else if (nextLineBreakWindows >= 0) {
					nextPosition = nextLineBreakWindows;
					lineBreakSize = 2;
				}

				if (nextPosition >= 0) {
					position = nextPosition + lineBreakSize;
					if (position >= dataString.length()) {
						break;
					}
				} else {
					break;
				}
				lineCount++;
			}
			return position;
		}
	}

	/**
	 * Get the startindex of the line at a given position within the text
	 *
	 * @param dataString
	 * @param index
	 * @return
	 */
	public static int getStartIndexOfLineAtIndex(String dataString, int index) {
		if (dataString == null || index < 0) {
			return -1;
		} else if (index == 0) {
			return 0;
		} else {
			int nextLineBreakMac = dataString.lastIndexOf(LinebreakMac, index);
			int nextLineBreakUnix = dataString.lastIndexOf(LinebreakUnix, index);
			int nextLineBreakWindows = dataString.lastIndexOf(LinebreakWindows, index);

			if (nextLineBreakMac >= 0 && (nextLineBreakUnix < 0 || nextLineBreakMac < nextLineBreakUnix) && (nextLineBreakWindows < 0 || nextLineBreakMac < nextLineBreakWindows)) {
				return nextLineBreakMac + LinebreakMac.length();
			} else if (nextLineBreakUnix >= 0 && (nextLineBreakWindows < 0 || nextLineBreakUnix < nextLineBreakWindows)) {
				return nextLineBreakUnix + LinebreakUnix.length();
			} else if (nextLineBreakWindows >= 0) {
				return nextLineBreakWindows + LinebreakWindows.length();
			} else {
				return 0;
			}
		}
	}

	/**
	 * Get the end index of a line within a text
	 *
	 * @param dataString
	 * @param index
	 * @return
	 */
	public static int getEndIndexOfLineAtIndex(String dataString, int index) {
		if (dataString == null || index < 0) {
			return -1;
		} else if (index == 0) {
			return 0;
		} else {
			int nextLineBreakMac = dataString.indexOf(LinebreakMac, index);
			int nextLineBreakUnix = dataString.indexOf(LinebreakUnix, index);
			int nextLineBreakWindows = dataString.indexOf(LinebreakWindows, index);

			if (nextLineBreakMac >= 0 && (nextLineBreakUnix < 0 || nextLineBreakMac > nextLineBreakUnix) && (nextLineBreakWindows < 0 || nextLineBreakMac > nextLineBreakWindows)) {
				return nextLineBreakMac;
			} else if (nextLineBreakUnix >= 0 && (nextLineBreakWindows < 0 || nextLineBreakUnix - 1 > nextLineBreakWindows)) {
				return nextLineBreakUnix;
			} else if (nextLineBreakWindows >= 0) {
				return nextLineBreakWindows;
			} else {
				return dataString.length();
			}
		}
	}

	/**
	 * Searching the next position of a string or searchPattern beginning at a startIndex. Searched text can be a simple string or a regex pattern, switching this with the flag
	 * searchTextIsRegularExpression. Search can be done caseinsensitive or caseinsensitive by the flag searchCaseSensitive. Search can be done from end to start, backwards by the flag
	 * searchReversely.
	 *
	 *
	 * @param text
	 * @param startPosition
	 * @param searchTextOrPattern
	 * @param searchTextIsRegularExpression
	 * @param searchCaseSensitive
	 * @param searchReversely
	 * @return Tuple First = Startindex, Second = Length of found substring
	 */
	public static Tuple<Integer, Integer> searchNextPosition(String text, int startPosition, String searchTextOrPattern, boolean searchTextIsRegularExpression, boolean searchCaseSensitive,
			boolean searchReversely) {
		if (Utilities.isBlank(text) || startPosition < 0 || text.length() < startPosition || Utilities.isEmpty(searchTextOrPattern)) {
			return null;
		} else {
			String fullText = text;
			int nextPosition = -1;
			int length = -1;

			Pattern searchPattern;
			if (searchTextIsRegularExpression) {
				if (searchCaseSensitive) {
					searchPattern = Pattern.compile(searchTextOrPattern, Pattern.MULTILINE);
				} else {
					searchPattern = Pattern.compile(searchTextOrPattern, Pattern.CASE_INSENSITIVE | Pattern.MULTILINE);
				}
			} else {
				if (searchCaseSensitive) {
					searchPattern = Pattern.compile(searchTextOrPattern, Pattern.LITERAL | Pattern.MULTILINE);
				} else {
					searchPattern = Pattern.compile(searchTextOrPattern, Pattern.CASE_INSENSITIVE | Pattern.LITERAL | Pattern.MULTILINE);
				}
			}

			Matcher matcher = searchPattern.matcher(fullText);

			if (searchReversely) {
				while (matcher.find()) {
					if (matcher.start() < startPosition) {
						nextPosition = matcher.start();
						length = matcher.end() - nextPosition;
					} else {
						break;
					}
				}
				if (nextPosition < 0) {
					if (matcher.start() > 0) {
						nextPosition = matcher.start();
						length = matcher.end() - nextPosition;
					}
					while (matcher.find()) {
						nextPosition = matcher.start();
						length = matcher.end() - nextPosition;
					}
				}
			} else {
				if (matcher.find(startPosition)) {
					nextPosition = matcher.start();
					length = matcher.end() - nextPosition;
				}
				if (startPosition > 0 && nextPosition < 0) {
					if (matcher.find(0)) {
						nextPosition = matcher.start();
						length = matcher.end() - nextPosition;
					}
				}
			}

			if (nextPosition < 0) {
				return null;
			} else {
				return new Tuple<Integer, Integer>(nextPosition, length);
			}
		}
	}

	/**
	 * Count the occurences of a string or pattern within a text
	 *
	 * @param text
	 * @param searchTextOrPattern
	 * @param searchTextIsRegularExpression
	 * @param searchCaseSensitive
	 * @return
	 */
	public static int countOccurences(String text, String searchTextOrPattern, boolean searchTextIsRegularExpression, boolean searchCaseSensitive) {
		if (text != null && searchTextOrPattern != null) {
			String fullText = text;
			int occurences = 0;

			Pattern searchPattern;
			if (searchTextIsRegularExpression) {
				if (searchCaseSensitive) {
					searchPattern = Pattern.compile(searchTextOrPattern, Pattern.MULTILINE);
				} else {
					searchPattern = Pattern.compile(searchTextOrPattern, Pattern.CASE_INSENSITIVE | Pattern.MULTILINE);
				}
			} else {
				if (searchCaseSensitive) {
					searchPattern = Pattern.compile(searchTextOrPattern, Pattern.LITERAL | Pattern.MULTILINE);
				} else {
					searchPattern = Pattern.compile(searchTextOrPattern, Pattern.CASE_INSENSITIVE | Pattern.LITERAL | Pattern.MULTILINE);
				}
			}

			Matcher matcher = searchPattern.matcher(fullText);
			while (matcher.find()) {
				occurences++;
			}
			return occurences;
		} else {
			return -1;
		}
	}

	/**
	 * Get line number at a given text position
	 *
	 * @param dataString
	 * @param textPosition
	 * @return
	 */
	public static int getLineNumberOfTextposition(String dataString, int textPosition) {
		if (dataString == null) {
			return -1;
		} else {
			try {
				String textPart = dataString;
				if (dataString.length() > textPosition) {
					textPart = dataString.substring(0, textPosition);
				}
				int lineNumber = getLineCount(textPart);
				if (textPart.endsWith(LinebreakUnix) || textPart.endsWith(LinebreakMac)) {
					lineNumber++;
				}
				return lineNumber;
			} catch (IOException e) {
				e.printStackTrace();
				return -1;
			} catch (Exception e) {
				e.printStackTrace();
				return -1;
			}
		}
	}

	/**
	 * Get the number of the first line containing a string or pattern
	 *
	 * @param dataString
	 * @param searchTextOrPattern
	 * @param searchCaseSensitive
	 * @param searchTextIsRegularExpression
	 * @return
	 * @throws IOException
	 */
	public static int getNumberOfLineContainingText(String dataString, String searchTextOrPattern, boolean searchCaseSensitive, boolean searchTextIsRegularExpression) throws IOException {
		LineNumberReader lineNumberReader = null;
		try {
			Pattern searchPattern;
			if (searchTextIsRegularExpression) {
				if (searchCaseSensitive) {
					searchPattern = Pattern.compile(searchTextOrPattern, Pattern.MULTILINE);
				} else {
					searchPattern = Pattern.compile(searchTextOrPattern, Pattern.CASE_INSENSITIVE | Pattern.MULTILINE);
				}
			} else {
				if (searchCaseSensitive) {
					searchPattern = Pattern.compile(searchTextOrPattern, Pattern.LITERAL | Pattern.MULTILINE);
				} else {
					searchPattern = Pattern.compile(searchTextOrPattern, Pattern.CASE_INSENSITIVE | Pattern.LITERAL | Pattern.MULTILINE);
				}
			}

			lineNumberReader = new LineNumberReader(new StringReader(dataString));
			String lineContent;
			while ((lineContent = lineNumberReader.readLine()) != null) {
				Matcher matcher = searchPattern.matcher(lineContent);
				if (matcher.find()) {
					return lineNumberReader.getLineNumber();
				}
			}
			return -1;
		} finally {
			Utilities.closeQuietly(lineNumberReader);
		}
	}

	/**
	 * Insert some string before a given line into a text
	 *
	 * @param dataString
	 * @param insertText
	 * @param lineNumber
	 * @return
	 * @throws IOException
	 */
	public static String insertTextBeforeLine(String dataString, String insertText, int lineNumber) throws IOException {
		if (lineNumber > -1) {
			int startIndex = TextUtilities.getStartIndexOfLine(dataString, lineNumber);
			StringBuilder dataBuilder = new StringBuilder(dataString);
			dataBuilder.insert(startIndex, insertText);
			return dataBuilder.toString();
		} else {
			return dataString;
		}
	}

	/**
	 * Insert some string at the end of a given line into a text
	 *
	 * @param dataString
	 * @param insertText
	 * @param lineNumber
	 * @return
	 * @throws IOException
	 */
	public static String insertTextAfterLine(String dataString, String insertText, int lineNumber) throws IOException {
		if (lineNumber > -1) {
			int startIndex = TextUtilities.getStartIndexOfLine(dataString, lineNumber + 1);
			StringBuilder dataBuilder = new StringBuilder(dataString);
			dataBuilder.insert(startIndex, insertText);
			return dataBuilder.toString();
		} else {
			return dataString;
		}
	}

	/**
	 * Try to detect the encoding of a byte array xml data.
	 *
	 *
	 * @param data
	 * @return
	 * @throws UnsupportedEncodingException
	 */
	public static Tuple<String, Boolean> detectEncoding(byte[] data) throws UnsupportedEncodingException {
		if (data.length > 2 && data[0] == Utilities.BOM_UTF_16_BIG_ENDIAN[0] && data[1] == Utilities.BOM_UTF_16_BIG_ENDIAN[1]) {
			return new Tuple<String, Boolean>("UTF-16BE", true);
		} else if (data.length > 2 && data[0] == Utilities.BOM_UTF_16_LOW_ENDIAN[0] && data[1] == Utilities.BOM_UTF_16_LOW_ENDIAN[1]) {
			return new Tuple<String, Boolean>("UTF-16LE", true);
		} else if (data.length > 3 && data[0] == Utilities.BOM_UTF_8[0] && data[1] == Utilities.BOM_UTF_8[1] && data[2] == Utilities.BOM_UTF_8[2]) {
			return new Tuple<String, Boolean>("UTF-8", true);
		} else {
			// Detect Xml Encoding
			try {
				// Use first data part only to speed up
				String interimString = new String(data, 0, Math.min(data.length, 100), "UTF-8").toLowerCase();
				String reducedInterimString = interimString.replace("\u0000", "");
				int encodingStart = reducedInterimString.indexOf("encoding");
				if (encodingStart >= 0) {
					encodingStart = reducedInterimString.indexOf("=", encodingStart);
					if (encodingStart >= 0) {
						encodingStart++;
						while (reducedInterimString.charAt(encodingStart) == ' ') {
							encodingStart++;
						}
						if (reducedInterimString.charAt(encodingStart) == '"' || reducedInterimString.charAt(encodingStart) == '\'') {
							encodingStart++;
						}
						StringBuilder encodingString = new StringBuilder();
						while (Character.isLetter(reducedInterimString.charAt(encodingStart)) || Character.isDigit(reducedInterimString.charAt(encodingStart))
								|| reducedInterimString.charAt(encodingStart) == '-') {
							encodingString.append(reducedInterimString.charAt(encodingStart));
							encodingStart++;
						}
						Charset.forName(encodingString.toString());
						if (encodingString.toString().startsWith("utf-16") && data[0] == 0) {
							return new Tuple<String, Boolean>("UTF-16BE", false);
						} else if (encodingString.toString().startsWith("utf-16") && data[1] == 0) {
							return new Tuple<String, Boolean>("UTF-16LE", false);
						} else {
							return new Tuple<String, Boolean>(encodingString.toString().toUpperCase(), false);
						}
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}

			String interimString = new String(data, "UTF-8").toLowerCase();
			int zeroIndex = interimString.indexOf('\u0000');
			if (zeroIndex >= 0 && zeroIndex <= 100) {
				if (zeroIndex % 2 == 0) {
					return new Tuple<String, Boolean>("UTF-16BE", false);
				} else {
					return new Tuple<String, Boolean>("UTF-16LE", false);
				}
			}

			return null;
		}
	}

	/**
	 * Replace the leading indentation by double blanks of a text by some other character like tab
	 *
	 * @param data
	 * @param blankCount
	 * @param replacement
	 * @return
	 */
	public static String replaceIndentingBlanks(String data, int blankCount, String replacement) {
		String result = data;
		Pattern pattern = Pattern.compile("^(  )+", Pattern.MULTILINE);
		Matcher matcher = pattern.matcher(result);
		while (matcher.find()) {
			int replacementCount = (matcher.end() - matcher.start()) / blankCount;
			result = matcher.replaceFirst(Utilities.repeat(replacement, replacementCount));
			matcher = pattern.matcher(result);
		}
		return result;
	}

	/**
	 * Remove the first leading whitespace of a string
	 *
	 * @param data
	 * @return
	 */
	public static String removeFirstLeadingWhitespace(String data) {
		Pattern pattern = Pattern.compile("^[\\t ]", Pattern.MULTILINE);
		Matcher matcher = pattern.matcher(data);
		return matcher.replaceAll("");
	}

	/**
	 * Indent the lines of a string by one tab
	 *
	 * @param data
	 * @return
	 */
	public static String addLeadingTab(String data) {
		return addLeadingTabs(data, 1);
	}

	/**
	 * Indent the lines of a string by some tabs
	 *
	 * @param data
	 * @param numberOfTabs
	 * @return
	 */
	public static String addLeadingTabs(String data, int numberOfTabs) {
		Pattern pattern = Pattern.compile("^", Pattern.MULTILINE);
		Matcher matcher = pattern.matcher(data);
		return matcher.replaceAll(Utilities.repeat("\t", numberOfTabs));
	}

	/**
	 * Chop a string into pieces of maximum length
	 *
	 * @param value
	 * @param length
	 * @return
	 */
	public static List<String> truncate(String value, int length) {
		List<String> parts = new ArrayList<String>();
		int valueLength = value.length();
		for (int i = 0; i < valueLength; i += length) {
			parts.add(value.substring(i, Math.min(valueLength, i + length)));
		}
		return parts;
	}

	/**
	 * Trim all strings in an array of strings to a maximum length and add the cut off rest as new lines
	 *
	 * @param lines
	 * @param length
	 * @return
	 */
	public static List<String> truncate(String[] lines, int length) {
		List<String> linesTruncated = new ArrayList<String>();
		for (String line : lines) {
			linesTruncated.addAll(truncate(line, length));
		}
		return linesTruncated;
	}

	/**
	 * Trim all strings in a list of strings to a maximum length and add the cut off rest as new lines
	 *
	 * @param lines
	 * @param length
	 * @return
	 */
	public static List<String> truncate(List<String> lines, int length) {
		List<String> linesTruncated = new ArrayList<String>();
		for (String line : lines) {
			linesTruncated.addAll(truncate(line, length));
		}
		return linesTruncated;
	}

	/**
	 * Remove all leading and trailing whispaces, not only blanks like it is done by .trim()
	 *
	 * @param data
	 * @return
	 */
	public static String removeLeadingAndTrailingWhitespaces(String data) {
		Pattern pattern1 = Pattern.compile("^\\s*", Pattern.MULTILINE);
		Matcher matcher1 = pattern1.matcher(data);
		data = matcher1.replaceAll("");

		Pattern pattern2 = Pattern.compile("\\s*$", Pattern.MULTILINE);
		Matcher matcher2 = pattern2.matcher(data);
		data = matcher2.replaceAll("");

		return data;
	}

	/**
	 * Remove all trailing whispaces
	 *
	 * @param data
	 * @return
	 */
	public static String removeTrailingWhitespaces(String data) {
		Pattern pattern = Pattern.compile("\\s*$", Pattern.MULTILINE);
		Matcher matcher = pattern.matcher(data);
		data = matcher.replaceAll("");

		return data;
	}

	/**
	 * Scan an inputstream for start indexes of text lines
	 *
	 * @param input
	 * @return
	 * @throws IOException
	 */
	public static List<Long> scanLineStartIndexes(InputStream input) throws IOException {
		List<Long> returnList = new ArrayList<Long>();
		long position = 0;
		int bufferPrevious = 0;
		int bufferNext;
		returnList.add(0L);
		while ((bufferNext = input.read()) != -1) {
			if ((char) bufferNext == '\n') {
				returnList.add(position + 1);
			} else if ((char) bufferPrevious == '\r') {
				returnList.add(position);
			}

			bufferPrevious = bufferNext;
			position++;
		}

		if ((char) bufferPrevious == '\r') {
			returnList.add(position);
		}

		return returnList;
	}

	/**
	 * Scan an inputstream for start indexes of text lines. Skip the first characters up to startIndex.
	 *
	 * @param input
	 * @param startIndex
	 * @return
	 * @throws IOException
	 */
	public static List<Long> scanLineStartIndexes(InputStream input, long startIndex) throws IOException {
		List<Long> returnList = new ArrayList<Long>();
		long position = 0;
		int bufferPrevious = 0;
		int bufferNext;
		if (startIndex <= 0) {
			returnList.add(0L);
		} else {
			input.skip(startIndex);
			position = startIndex;
		}
		while ((bufferNext = input.read()) != -1) {
			if ((char) bufferNext == '\n') {
				returnList.add(position + 1);
			} else if ((char) bufferPrevious == '\r') {
				returnList.add(position);
			}

			bufferPrevious = bufferNext;
			position++;
		}

		if ((char) bufferPrevious == '\r') {
			returnList.add(position);
		}

		return returnList;
	}

	/**
	 * Scan an inputstream for positions of linebreaks.
	 *
	 * @param dataInputStream
	 * @return
	 * @throws IOException
	 */
	public static List<Long> scanLinebreakIndexes(InputStream dataInputStream) throws IOException {
		List<Long> returnList = new ArrayList<Long>();
		long position = 0;
		int bufferPrevious = 0;
		int bufferNext;
		while ((bufferNext = dataInputStream.read()) != -1) {
			if ((char) bufferNext == '\n' && (char) bufferPrevious != '\r') {
				returnList.add(position);
			} else if ((char) bufferNext == '\r') {
				returnList.add(position);
			}

			bufferPrevious = bufferNext;
			position++;
		}
		return returnList;
	}

	/**
	 * Break all lines after a maximum number of characters.
	 *
	 * @param dataString
	 * @param maximumLength
	 * @return
	 */
	public static String breakLinesAfterMaximumLength(String dataString, int maximumLength) {
		return Utilities.join(dataString.split("(?<=\\G.{" + maximumLength + "})"), "\n");
	}

	/**
	 * Scan a string for the characters used in it
	 *
	 * @param data
	 * @return
	 */
	public static List<Character> getUsedChars(String data) {
		List<Character> usedCharsList = new ArrayList<Character>();
		for (char item : data.toCharArray()) {
			if (!usedCharsList.contains(item)) {
				usedCharsList.add(item);
			}
		}
		return usedCharsList;
	}

	/**
	 * Check if a string ends with a stringpart caseinsensitive
	 *
	 * @param str
	 * @param suffix
	 * @return
	 */
	public static boolean endsWithIgnoreCase(String str, String suffix) {
		if (str == null || suffix == null) {
			return (str == null && suffix == null);
		} else if (suffix.length() > str.length()) {
			return false;
		} else {
			int strOffset = str.length() - suffix.length();
			return str.regionMatches(true, strOffset, suffix, 0, suffix.length());
		}
	}

	/**
	 * Check if a string starts with a stringpart caseinsensitive
	 *
	 * @param str
	 * @param prefix
	 * @return
	 */
	public static boolean startsWithIgnoreCase(String str, String prefix) {
		if (str == null || prefix == null) {
			return (str == null && prefix == null);
		} else if (prefix.length() > str.length()) {
			return false;
		} else {
			return str.regionMatches(true, 0, prefix, 0, prefix.length());
		}
	}

	/**
	 * Trim a string to a maximum number of characters and add a sign if it was cut off.
	 *
	 * @param inputString
	 * @param laenge
	 * @param interLeave
	 * @return
	 */
	public static String trimStringToMaximumLength(String inputString, int laenge, String interLeave) {
		if (inputString == null || inputString.length() <= laenge) {
			return inputString;
		} else {
			int takeFirstLength = (laenge - interLeave.length()) / 2;
			int takeLastLength = laenge - interLeave.length() - takeFirstLength;
			return inputString.substring(0, takeFirstLength) + interLeave + inputString.substring(inputString.length() - takeLastLength);
		}
	}

	/**
	 * Check if a string contains a stringpart caseinsensitive
	 *
	 * @param str
	 * @param substr
	 * @return
	 */
	public static boolean containsIgnoreCase(String str, String substr) {
		if (str == substr) {
			return true;
		} else if (str == null) {
			return false;
		} else if (substr == null) {
			return true;
		} else if (substr.length() > str.length()) {
			return false;
		} else {
			return str.regionMatches(true, 0, substr, 0, str.length());
		}
	}

	public static String mapToString(Map<? extends Object, ? extends Object> map) {
		StringBuilder returnValue = new StringBuilder();
		for (Entry<? extends Object, ? extends Object> entry : map.entrySet()) {
			if (returnValue.length() > 0) {
				returnValue.append("\n");
			}
			Object key = entry.getKey();
			if (key == null) {
				key = "";
			}
			Object value = entry.getValue();
			if (value == null) {
				value = "";
			}

			returnValue.append(key.toString() + ": " + value.toString());
		}
		return returnValue.toString();
	}

	public static String mapToStringWithSortedKeys(Map<? extends String, ? extends Object> map) {
		StringBuilder returnValue = new StringBuilder();
		for (String key : Utilities.asSortedList(map.keySet())) {
			Object value = map.get(key);
			if (returnValue.length() > 0) {
				returnValue.append("\n");
			}
			if (key == null) {
				key = "";
			}
			if (value == null) {
				value = "";
			}

			returnValue.append(key.toString() + ": " + value.toString());
		}
		return returnValue.toString();
	}

	public static int getColumnNumberFromColumnChars(String columnChars) {
		int value = 0;
		for (char columnChar : columnChars.toLowerCase().toCharArray()) {
			int columnIndex = (byte) columnChar - (byte) 'a' + 1;
			value = value * 26 + columnIndex;
		}
		return value;
	}
}