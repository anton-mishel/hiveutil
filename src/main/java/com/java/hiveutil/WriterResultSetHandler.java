package com.java.hiveutil;

import java.io.IOException;
import java.io.Writer;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class WriterResultSetHandler implements IResultSetHandler{

	private Writer writer;
	private boolean withHeader;
	
	Logger logger = LogManager.getLogger();
	public WriterResultSetHandler (Writer writer, boolean withHeader){
		this.writer = writer;
		this.withHeader = withHeader;
	}
	
	@Override
	public void handle(ResultSet rs) throws SQLException{
		ResultSetMetaData rsmd = rs.getMetaData();
		int numberOfColumns = rsmd.getColumnCount();
		String lineSeparator = System.getProperty("line.separator");
		try {
			if (withHeader){
				StringBuilder header = new StringBuilder();
				for (int i = 1; i <= numberOfColumns; i++) {
					if (i > 1) {
						header.append("\t");
					}
					String columnName = rsmd.getColumnName(i);
					header.append(columnName);
				}
				writer.write(header.toString());
				writer.write(lineSeparator);
			}
			while (rs.next()) {
				StringBuilder row = new StringBuilder();
				for (int i = 1; i <= numberOfColumns; i++) {
					if (i > 1) {
						row.append("\t");
					}
					String columnValue = rs.getString(i);
					if (columnValue != null) {
							row.append(columnValue);							
					}
				}
				writer.write(row.toString());
				writer.write(lineSeparator);  
			}
		} catch (IOException e) {
			logger.error("Failed writing to output stream",e);
			throw new RuntimeException("WriterResultSetHandler problem",e);
		}
			
	}

		

}
