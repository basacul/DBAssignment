import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.nio.file.FileSystems;
import java.nio.file.Paths;
import java.util.List;
import java.util.Scanner;
import java.util.Stack;
import java.sql.*;
import java.util.*;

import java.sql.Connection;
import java.sql.DriverManager;


public class DataSanitizer {
	private static final String FILESYSTEM_SEPARATOR = FileSystems.getDefault().getSeparator();
	private static final String OUTPUT_FILE_PREFIX = "sanitized_";
	private static CSVParser csvParser;


	public static void main(String[] args) throws Exception {
		// Input/Output files
		File csvFile = Paths.get(args[0]).toFile();
		File outFile = new File(csvFile.getParentFile() + FILESYSTEM_SEPARATOR + OUTPUT_FILE_PREFIX + csvFile.getName());

		// Filereader/-writer
		Scanner scanner = new Scanner(csvFile);
		FileWriter fileWriter = new FileWriter(outFile);
		BufferedWriter buffWriter = new BufferedWriter(fileWriter);

		//for jdbc
		Connection c = null;
	  Statement s = null;

		//for retrieving the hashtags
		String plainText;
		ArrayList<String> hashtags = new ArrayList<String>();

		ArrayList<String> links = new ArrayList<String>();

		//for the id in Tweet and much easier to compute and retrieve
		int i = 0;

		try{
			Class.forName("org.postgresql.Driver");

			c = DriverManager.getConnection(
				"jdbc:postgresql://[::1]:5432/Election",
 				"postgres", "postgres");

				s = c.createStatement();

				System.out.println("success");
		}catch(Exception e){
			System.out.println("Unlucky?" + e.getMessage());
		}


		// Skip first line (handle,text,is_retweet,...
		if (scanner.hasNext()) {
			scanner.nextLine();
		}

		//otherwise go through each line and retrieve the necessary values to populate each
		//table in our model
		while (scanner.hasNext()) {
			List<String> line = getCSVParser().parseLine(scanner.nextLine());

			//is the line ok? If not, skip this one and start with the next line
			if (!verifyLine(line)) {
				continue;
			}


			plainText = line.get(1);

			//hashtag retrieved go to next iteration
			boolean breakLoop = false;

			//retrieve the hash and dynamically update length condition in outer for loop
			for(int w = 0; w < plainText.length(); w++){
				if(plainText.charAt(w) == '#'){
					for(int e = w; e < plainText.length() && !breakLoop; e++){
						if(plainText.charAt(e) == ' ' || plainText.charAt(e) == ','){
							hashtags.add(plainText.substring(w, e));
							plainText = plainText.substring(0,w) + plainText.substring(e+1, plainText.length() - 1);
							breakLoop = true;
						}
					}
				}
				breakLoop = false;
			}


			//link retrieved go to next iteration
			breakLoop = false;
			int diff = 0;

			//retrieve the links and dynamically update length condition in outer for loop
			for(int w = 0; w < plainText.length(); w++){
				diff = w + 5;
				if(diff < plainText.length() && plainText.substring(w,diff).equalsIgnoreCase("http")){
					for(int e = w; e < plainText.length() && !breakLoop; e++){
						if(plainText.charAt(e) == ' ' || plainText.charAt(e) == ','){
							links.add(plainText.substring(w, e));
							System.out.println(plainText.substring(w, e));
							plainText = plainText.substring(0,w) + plainText.substring(e, plainText.length() - 1);
							breakLoop = true;
						}
					}
				}
				breakLoop = false;
			}

			//replaces the T in the time string with an empty space " "
			line.set(4, (line.get(4).substring(0,10) + " " + line.get(4).substring(11, line.get(4).length() - 1)));

			//populate the tables in the database Election
			try{

    			s.executeUpdate("INSERT INTO Tweet (timestamp, favouriteCount, sourceUrl, quoteStatus, truncated, retweetCount, handle) VALUES ('" + line.get(4) + "', '" + line.get(8) + "', '" + line.get(9) + "', " + line.get(6) + ", " + line.get(10) + ", " + line.get(7) + ", '" + line.get(0) + "')");
					s.executeUpdate("INSERT INTO Content (Tweet_id)" + "VALUES ('" + i + "')" );
    			s.executeUpdate("INSERT INTO Text (Content_id, text) " + "VALUES ('" + i + "', '" + plainText + "')");

					for(int q = 0; q < hashtags.size(); q++){
						s.executeUpdate("INSERT INTO Hashtag (Content_id, hashtag) VALUES ('" + i + "' , '" + hashtags.get(q) + "')");
					}

					for(int q = 0; q < links.size(); q++){
						s.executeUpdate("INSERT INTO Link (Content_id, link) " + "VALUES ('" + i + "', '" + links.get(q) + "')");
					}

					s.executeUpdate("INSERT INTO TargetHandle (Content_id, targetHandle) VALUES ('"  + i + "', '" + line.get(0) + "')");
    			s.executeUpdate("INSERT INTO Retweet (id, Tweet_id, originalHandle) " + "VALUES (" + i + ", SELECT id FROM Tweet WHERE id=" + i + ", '" + line.get(3) + "')");
    			s.executeUpdate("INSERT INTO Reply (Tweet_id, replyHandle) " + "VALUES ( SELECT id FROM Tweet WHERE id=" + i + ", '" + line.get(5) + "')");

			}catch(Exception e){
				//System.out.println("Still unlucky?" + e.getMessage());
			}

			//increments the id, which will be set automatically for each Tweet entry
			//but i is used to decrease computation through jdbc
			++i;

			//clear arraylist hashtags
			hashtags.clear();
			links.clear();

			// Append "("
			line.set(0, "(" + line.get(0));
			// Quote "text" to prevent comma inside it from interfering with SQL
			line.set(1, "\"" + line.get(1) + "\"");
			// Prepend ")"
			line.set(line.size()-1, line.get(line.size()-1) + ")");


			@SuppressWarnings({ "rawtypes", "unchecked" })
			SQLPrinter<List<String>> sqlPrinter = new SQLPrinter();
			sqlPrinter.setUnformattedStringList(line);
			String sqlValue = sqlPrinter.toString();

			if (!sqlValue.isEmpty()) {
				buffWriter.write(sqlValue + "\n");
			}
		}

		//close connections to the database
		try{
			s.close();
			c.close();
		}catch(Exception e){
			System.out.println(e.getMessage());
			System.out.println("Failure");
		}

		// Close I/O
		scanner.close();
		buffWriter.close();
		fileWriter.close();
	}

	private static boolean verifyLine(List<String> line) {
		boolean lineIsCorrect = true;

		// Throw away all lines not containing exactly CSVParserImpl.DEFAULT_SEPARATOR_MAX amount of separators and an even amount of "
		if (line.size() != 11 || !lineTextIsBalanced(line)) {
			lineIsCorrect = false;
		}

		return lineIsCorrect;
	}

	private static boolean lineTextIsBalanced(List<String> line) {
		boolean isBalanced = true;
		String lineText = line.get(1);
		final Stack<Character> stack = new Stack<Character>();

		for (int p = 0; p < lineText.length(); p++) {
			if ('\"' == (lineText.charAt(p))) {
				stack.push(lineText.charAt(p));
			}
		}
		if ((stack.size() % 2) != 0) {
			isBalanced = false;
		}

		return isBalanced;
	}

	private static CSVParser getCSVParser() {
		CSVParser parser;

		if (csvParser == null) {
			parser = new CSVParserImpl();
		} else {
			parser = csvParser;
		}

		return parser;
	}
}
