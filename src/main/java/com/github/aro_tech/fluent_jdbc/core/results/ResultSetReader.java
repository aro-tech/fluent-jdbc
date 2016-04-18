package com.github.aro_tech.fluent_jdbc.core.results;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Functional interface to extract results from a JDBC query using a lambda
 * @author aro_tech
 *
 */
@FunctionalInterface
public interface ResultSetReader<T> {

	/**
	 * Convert a JDBC ResultSet to an object of some template type T
	 * @param r
	 * @return
	 */
	public T handleResults(ResultSet r)throws SQLException;
}
