import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.nio.file.FileSystems;
import java.nio.file.Paths;
import java.util.List;
import java.util.Scanner;
import java.util.Stack;
import java.util.ArrayList;
import java.sql.*;
//import java.sql.Connection;
//import java.sql.DriverManager;


public class DataSanitizer {
	private static final String FILESYSTEM_SEPARATOR = FileSystems.getDefault().getSeparator();
	private static final String OUTPUT_FILE_PREFIX = "sanitized_";
	private static CSVParser csvParser;
	
	//Primary key for the Tweet and as Foreign Key for all others
	private static int i = 0;
	//Primary key for the Hashtag table
	private static int hashtagID = 0;
	//Primary key for the Link table
	private static int linkID = 0;

	/*tested the number of errors thrown along the population of the database election
	* which was used to test and count the number of erroneous entries along the
	* sanitization process or as a simple feedback tool for further investigation
	*/
	private static int error = 0;

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

		try{
			Class.forName("org.postgresql.Driver");

			c = DriverManager.getConnection(
				"jdbc:postgresql://[::1]:5432/Election",
 				"admin", "admin");

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

			//cleans further the data and populates the tables in the database election line by line
			populateDB(line, s);

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
		
		System.out.println("Number of corrupted lines : " + error );
		
		// Close I/O
		scanner.close();
		buffWriter.close();
		fileWriter.close();
	}

	private static void populateDB(List<String> line, Statement s){
		//for retrieving the hashtags and links and working on plainText
		String plainText = line.get(1);
		ArrayList<String> hashtags = new ArrayList<String>();
		ArrayList<String> links = new ArrayList<String>();
		//if hashtag was retrieved go to next iteration
		boolean breakLoop = false;

		for(int c = 0;c < plainText.length();c++){
			if(plainText.charAt(c) == '\''){
				plainText = plainText.substring(0,c)+' '+plainText.substring(c+1);
			}
		}
		//retrieve the hash and dynamically update length condition in outer for loop
		for(int w = 0; w < plainText.length(); w++){
			if(plainText.charAt(w) == '#'){
				for(int e = w; e < plainText.length() && !breakLoop; e++){
					if(plainText.charAt(e) == ' ' || plainText.charAt(e) == ','  || plainText.charAt(e) == '\'' || e +1 == plainText.length()){
						// System.out.println("hashtags");
						hashtags.add(plainText.substring(w, e));
						w = e;
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
			diff = w + 3;
			if(diff < plainText.length() && plainText.charAt(w) == 'h' &&
				plainText.charAt(w+1) == 't' &&
				plainText.charAt(w+2) == 't' &&
				plainText.charAt(w+3) == 'p'){
				for(int e = w; e < plainText.length() && !breakLoop; e++){
					if(plainText.charAt(e) == ' ' || plainText.charAt(e) == ','  || plainText.charAt(e) == '\'' || e +1 == plainText.length()){
						links.add(plainText.substring(w, e));
						w = e;
						breakLoop = true;
					}
				}
			}
			breakLoop = false;
		}
	
	  //Time string is cleaned by replacing 'T' with ' '	
		line.set(4, (line.get(4).substring(0,10) + " " + line.get(4).substring(11, line.get(4).length() - 1)));
	
		//populate the tables in the database Election
		try{
  		s.executeUpdate("INSERT INTO Tweet (id, datetime, favouriteCount, sourceUrl, quoteStatus, truncated, retweetCount, handle) VALUES ('" + i + "','" + line.get(4) + "', '" + line.get(8) + "', '" + line.get(9) + "', " + line.get(6) + ", " + line.get(10) + ", " + line.get(7) + ", '" + line.get(0) + "')");
			s.executeUpdate("INSERT INTO Content (id, idTweet)" + "VALUES ('" + i + "', '" + i + "')" );
  		s.executeUpdate("INSERT INTO Text (id, idContent, plaintext) " + "VALUES ('" + i + "','" + i + "', '" + plainText + "')");

			for(int q = 0; q < hashtags.size(); q++){
				s.executeUpdate("INSERT INTO Hashtag (id, idContent, hashtag) VALUES ('" + (hashtagID++) + "' , '" + i + "' , '" + hashtags.get(q) + "')");
			}

			for(int q = 0; q < links.size(); q++){
				s.executeUpdate("INSERT INTO Link (id, idContent, link) " + "VALUES ('" + (linkID++) + "' , '" + i + "', '" + links.get(q) + "')");
			}

			s.executeUpdate("INSERT INTO TargetHandle (id, idContent, targetHandle) VALUES ('"  + i + "','"  + i + "', '" + line.get(0) + "')");
  		s.executeUpdate("INSERT INTO Retweet (id, idTweet, originalHandle) " + "VALUES ( '"  + i + "','" + i + "', '" + line.get(3) + "')");
  		s.executeUpdate("INSERT INTO Reply (idTweet, replyHandle) " + "VALUES ('" + i + "', '" + line.get(5) + "')");

		}catch(Exception e){
			System.out.println("Still unlucky?" + e.getMessage());
			++error;
		}

		//increments the id, which will be set automatically for each Tweet entry
		//but i is used to decrease computation through jdbc
		++i;

		//clear arraylist hashtags
		hashtags.clear();
		links.clear();
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