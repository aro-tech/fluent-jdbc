/**
 * 
 */
package com.github.aro_tech.fluent_jdbc.core.logging;

/**
 * Logger implementation based on slf4j to be used for JDBC/SQL-related messages
 * 
 * @author aro_tech
 *
 */
public class SQLLogger extends Slf4JLogger implements ILogger {

	/**
	 * Constructor
	 */
	public SQLLogger() {
		super("SQL");
	}

}
