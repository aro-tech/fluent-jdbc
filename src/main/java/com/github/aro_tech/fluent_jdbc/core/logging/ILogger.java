package com.github.aro_tech.fluent_jdbc.core.logging;

/**
 * Interface for synchronous debug and error logging (using templates and values
 * for performance reasons) 
 * 
 * @author aro_tech
 *
 */
public interface ILogger {
	/**
	 * Conditionally logged message
	 * @param template message template using "{}" to insert parameters
	 * @param parameters optional parameters to insert
	 */
	void debug(String template, Object... parameters);
	
	/**
	 * Conditionally logged message
	 * @param template message template using "{}" to insert parameters
	 * @param parameters optional parameters to insert
	 */
	void info(String template, Object... parameters);
	
	/**
	 * Conditionally logged message
	 * @param template message template using "{}" to insert parameters
	 * @param parameters optional parameters to insert
	 */
	void warn(String template, Object... parameters);

	
	/**
	 * Conditionally logged message with error
	 * @param message to log
	 * @param throwable Error to log
	 */
	void warn(String message, Throwable throwable);

	/**
	 * Conditionally logged message with error
	 * @param message to log
	 * @param throwable Error to log
	 */
	void error(String message, Throwable throwable);
	
	/**
	 * Conditionally logged message
	 * @param template message template using "{}" to insert parameters
	 * @param parameters optional parameters to insert
	 */
	void error(String template, Object... parameters);

}
