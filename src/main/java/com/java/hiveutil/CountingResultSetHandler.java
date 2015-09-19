package com.java.hiveutil;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class CountingResultSetHandler implements IResultSetHandler{

	private int numberOfRows = 0;
	Logger logger = LogManager.getLogger();
	
	@Override
	public void handle(ResultSet rs) throws SQLException{
			while (rs.next()) {
				numberOfRows++;
			}			
	}

	public int getNumberOfRows() {
		return numberOfRows;
	}

}
