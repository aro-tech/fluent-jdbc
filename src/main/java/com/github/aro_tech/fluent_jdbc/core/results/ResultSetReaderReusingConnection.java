/**
 * 
 */
package com.github.aro_tech.fluent_jdbc.core.results;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Functional interface to allow a lambda to handle a query result and reuse the
 * connection to refine the end result
 * 
 * @author aro_tech
 *
 */
@FunctionalInterface
public interface ResultSetReaderReusingConnection<T> {

	/**
	 * Convert a JDBC ResultSet to an object of some template type T
	 * 
	 * @param rs
	 *            results from query
	 * @param c
	 *            Open connection to re-use based on results
	 * @return
	 */
	public T handleResults(ResultSet rs, Connection c) throws SQLException;

}
