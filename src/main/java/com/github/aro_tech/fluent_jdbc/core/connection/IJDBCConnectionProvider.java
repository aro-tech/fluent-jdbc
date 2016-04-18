package com.github.aro_tech.fluent_jdbc.core.connection;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Interface for a SQL connection provider
 * @author aro_tech
 *
 */
public interface IJDBCConnectionProvider {

	/**
	 * @return A JDBC Connection
	 * @throws SQLException
	 */
	public abstract Connection getConnection() throws SQLException;

}