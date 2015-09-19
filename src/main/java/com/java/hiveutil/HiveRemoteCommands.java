package com.java.hiveutil;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.sql.SQLException;

/**
 * HiveRemoteCommands enables querying a remote Hive DB (using JDBC).
 * Multiple semicolon-separated queries are supported (enabling set operations, changing database etc.). 
 */
public class HiveRemoteCommands {

	public final static String USAGE = "Usage: HiveRemoteCommands <semicolon-separated-commands> [<output-file-path>]";
	private final static String ERROR_PREFIX = "ERROR: ";

	//TODO Move to external configuration
	private final static String DB_URL = "jdbc:hive2://lab-hdp01:10000/default";
	private final static String USER = "mapr";
	private final static String PASS = "mapr";

	public static void usage(){
		System.out.println(USAGE);
	}
	
	public static void main(String args[]) throws Exception {
		/* Usage */
		if ((args.length < 1) || (args.length > 2)) {
			usage();
			System.exit(1);
		}

		String queriesString = args[0];
		String queries = queriesString.trim();
		/* Checks if filename is passed as argument and if not output to STDOUT */	 

		Writer writer = null;

		try {
			if(args.length == 1) 
			{
				writer = new OutputStreamWriter(System.out);
			} else {
				//Opening a file handler
				String filename = args[1];
				writer = new BufferedWriter(new FileWriter(filename));
			}
			IResultSetHandler resultSetHandler = new WriterResultSetHandler(writer,true);
			HiveUtil hiveUtil = new HiveUtil(DB_URL, USER, PASS);
			hiveUtil.execute(queries,resultSetHandler);
		} catch(SQLException e) {
			errorExit(e);
		} finally {
			if (writer != null) {
				writer.close();
			}
		}
	}

	protected static void errorExit(String message) {
		System.err.println(ERROR_PREFIX + message);
		System.exit(1);
	}

	protected static void errorExit(Throwable th) {
		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);
		th.printStackTrace(pw);	
		errorExit(sw.toString());
	}
}
