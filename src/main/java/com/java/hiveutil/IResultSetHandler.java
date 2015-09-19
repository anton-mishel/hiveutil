package com.java.hiveutil;

import java.sql.ResultSet;
import java.sql.SQLException;

public interface IResultSetHandler {
	public void handle(ResultSet rs)  throws SQLException;

}
