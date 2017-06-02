//package csvParser;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Maximilian Valenza
 *
 */
public class CSVParserImpl implements CSVParser {
	private static final char DEFAULT_SEPARATOR = ';';
	private static final char DEFAULT_QUOTE = '"';
	private static final int DEFAULT_SEPARATOR_MAX = 10;

	/*public CSVParserImpl() {
		super();
	}

	/* (non-Javadoc)
	 * @see csvParser.CSVParser#parseLine(java.lang.String)
	 */
	//@Override
	public List<String> parseLine(String cvsLine) {
		return parseLine(cvsLine, DEFAULT_SEPARATOR, DEFAULT_QUOTE);
	}

	/* (non-Javadoc)
	 * @see csvParser.CSVParser#parseLine(java.lang.String, char)
	 */
	//@Override
	public List<String> parseLine(String cvsLine, char separators) {
		return parseLine(cvsLine, separators, DEFAULT_QUOTE);
	}

	/* (non-Javadoc)
	 * @see csvParser.CSVParser#parseLine(java.lang.String, char, char)
	 */
	//@Override
	public List<String> parseLine(String cvsLine, char separators, char customQuote) {
		List<String> result = new ArrayList<>();

		if (cvsLine == null) {
			return result;
		}

		if (customQuote == ' ') {
			customQuote = DEFAULT_QUOTE;
		}

		if (separators == ' ') {
			separators = DEFAULT_SEPARATOR;
		}

		StringBuffer currentValue = new StringBuffer();
		boolean inQuotes = false;
		boolean startCollectChar = false;
		boolean doubleQuotesInColumn = false;

		char[] csvLineAsCharArray = cvsLine.toCharArray();

		for (char currentChar : csvLineAsCharArray) {
			int separatorCount = 0;

			if (inQuotes) {
				startCollectChar = true;
				if (currentChar == customQuote) {
					inQuotes = false;
					doubleQuotesInColumn = false;
				} else {
					// Allow " in quotes
					if (currentChar == '\"') {
						if (!doubleQuotesInColumn) {
							currentValue.append(currentChar + currentChar);
							doubleQuotesInColumn = true;
						}
					} else {
						currentValue.append(currentChar);
					}

				}
			} else {
				if (currentChar == customQuote) {

					inQuotes = true;

					if (csvLineAsCharArray[0] != '"' && customQuote == '\"') {
						currentValue.append('"');
					}

					if (startCollectChar) {
						currentValue.append('"');
					}

				} else if (currentChar == separators) {
					separatorCount++;

					result.add(currentValue.toString());

					currentValue = new StringBuffer();
					startCollectChar = false;

				} else if (currentChar == '\r') {
					// Ignore carriage-return
					continue;
				} else if (currentChar == '\n') {
					// Ignore newlines
					continue;
				} else if (currentChar == '\n' && separatorCount >= DEFAULT_SEPARATOR_MAX) {
					// Done
					break;
				} else {
					currentValue.append(currentChar);
				}
			}

		}

		result.add(currentValue.toString());

		return result;
	}
}
