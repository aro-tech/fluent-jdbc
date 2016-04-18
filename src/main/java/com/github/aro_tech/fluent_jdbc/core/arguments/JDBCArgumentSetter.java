/**
 * 
 */
package com.github.aro_tech.fluent_jdbc.core.arguments;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Functional interface to set arguments for a statement using a lambda
 * @author aro_tech
 *
 */
@FunctionalInterface
public interface JDBCArgumentSetter {
	void setArguments(PreparedStatement stmnt) throws SQLException;
}
