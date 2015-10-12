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
//      private final static String DB_URL = "jdbc:hive2://hiveserver2-nydc1-research.nydc1.outbrain.com:10000/default";
//	private final static String DB_URL = "jdbc:hive2://192.168.31.138:10000/default";
	private final static String USER = "hive";
	private final static String PASS = "hive";

	public static void usage(){
		System.out.println(USAGE);
	}
	
	public static void main(String args[]) throws Exception {
		/* Usage */
		if ((args.length < 2) || (args.length > 3)) {
			usage();
			System.exit(1);
		}

		String hostname = args[0];
         
        String DB_URL = "jdbc:hive2://"+hostname+":10000/default";
        System.out.println(DB_URL);

		String queriesString = args[1];
		String queries = queriesString.trim();
		/* Checks if filename is passed as argument and if not output to STDOUT */	 

		Writer writer = null;

		try {
			if(args.length == 2) 
			{
				writer = new OutputStreamWriter(System.out);
			} else {
				//Opening a file handler
				String filename = args[2];
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
