/**
 * 
 */
package com.github.aro_tech.fluent_jdbc.core.connection;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Provides a JDBC connection to a PostgreSQL database
 * @author aro_tech
 * 
 */
public class PostgreSQLConnectionProvider implements IJDBCConnectionProvider {
	private final String host;
	private final int port;
	private final String databaseName;
	private final String dbLogin;
	private final String dbPassword;
	
	
	/**
	 * 
	 * Constructor
	 * @param host
	 * @param databaseName
	 * @param dbLogin
	 * @param dbPassword
	 */
	public PostgreSQLConnectionProvider(String host,
			String databaseName, String dbLogin, String dbPassword) {
		super();		
		this.host = host;
		this.port = 5432;
		this.databaseName = databaseName;
		this.dbLogin = dbLogin;
		this.dbPassword = dbPassword;
	}
	
	/**
	 * 
	 * Constructor
	 * @param host
	 * @param port
	 * @param databaseName
	 * @param dbLogin
	 * @param dbPassword
	 */
	public PostgreSQLConnectionProvider(String host, int port,
			String databaseName, String dbLogin, String dbPassword) {
		super();
		this.host = host;
		this.port = port;
		this.databaseName = databaseName;
		this.dbLogin = dbLogin;
		this.dbPassword = dbPassword;
	}

	/* (non-Javadoc)
	 * @see com.aip.textspace.infra.storage.postgres.IJDBCConnectionProvider#getConnection()
	 */
	@Override
	public Connection getConnection() throws SQLException {
		try {
			Class.forName("org.postgresql.Driver");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		return java.sql.DriverManager.getConnection("jdbc:postgresql://" + host + ":" + port + "/" + databaseName, dbLogin, dbPassword);
	}
}
