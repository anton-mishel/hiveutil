package com.java.hiveutil;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.*;

import static org.apache.commons.lang3.StringUtils.isBlank;

/**
 * HiveRemoteCommands enables querying a remote Hive DB (using JDBC).
 * Multiple semicolon-separated queries are supported (enabling set operations, changing database etc.).
 */
public class HiveUtil {

    private final static Logger logger = LogManager.getLogger();

    private final static String DRIVER_CLASS = "org.apache.hive.jdbc.HiveDriver";
    private final static int FETCH_SIZE = 100000;
    private String dbUrl;
    private String user;
    private String pass;

    static {
        try {
            Class.forName(DRIVER_CLASS);
        } catch (ClassNotFoundException e) {
            logger.error("Hive driver class not found", e);
            System.exit(1);
        }
    }

    public HiveUtil(String dbUrl, String user, String pass) {
        this.dbUrl = dbUrl;
        this.user = user;
        this.pass = pass;
    }

    public void execute(String commands) throws SQLException {
        execute(commands, null);
    }
    /**
     * Executes the given commands (semicolon-separated-commands string).
     * If a command returns a ResultSet, the resultSet is handled by the resultSetHandler.
     * Note: all resultSets are handled by the same resultSetHandler.
     */
    public void execute(String commands, IResultSetHandler resultSetHandler) throws SQLException {
        if (isBlank(commands)) {
            logger.info("Commands string is empty. skipping.");
            return;
        }

        try (Connection con = DriverManager.getConnection(dbUrl, user, pass); Statement stmt = con.createStatement()) {
            //Opening connection to Hive
            stmt.setFetchSize(FETCH_SIZE);
            String[] queriesArray = commands.split(";");
            for (String queryString : queriesArray) {
                String query = queryString.trim();
                //using "execute" (and not executeQuery) to enable commands which do not return a result set like CREATE TABLE for example
                // (executeQuery throws an exception if no ResultSet is returned in such cases).
                logger.debug("# Executing query: " + query);
                boolean resultsSetReturned = stmt.execute(query);
                if (resultsSetReturned) {
                    ResultSet rs = stmt.getResultSet();
                    if (resultSetHandler != null) {
                        resultSetHandler.handle(rs);
                    }
                }
            }
        }
    }
}
