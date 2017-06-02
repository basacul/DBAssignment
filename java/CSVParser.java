/**
 *
 */
//package csvParser;

import java.util.List;

/**
 * @author Maximilian Valenza
 *
 */
public interface CSVParser {
	List<String> parseLine(String cvsLine);

	List<String> parseLine(String cvsLine, char separators);

	List<String> parseLine(String cvsLine, char separators, char customQuote);
}
