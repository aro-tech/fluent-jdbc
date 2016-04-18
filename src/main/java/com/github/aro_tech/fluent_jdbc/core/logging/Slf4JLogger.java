package com.github.aro_tech.fluent_jdbc.core.logging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ILogger implementation using SLF4J
 * @author aro_tech
 *
 */
public class Slf4JLogger implements ILogger {

	private final Logger logger; 

	public Slf4JLogger(String logName) {
		super();
		this.logger = LoggerFactory.getLogger(logName);
	}

	@Override
	public void debug(String template, Object... parameters) {
		logger.debug(template, parameters);
	}

	@Override
	public void info(String template, Object... parameters) {
		logger.info(template, parameters);
	}

	@Override
	public void warn(String template, Object... parameters) {
		logger.warn(template, parameters);
	}

	@Override
	public void warn(String message, Throwable throwable) {
		logger.warn(message, throwable);
	}

	@Override
	public void error(String message, Throwable throwable) {
		logger.error(message, throwable);
	}

	@Override
	public void error(String template, Object... parameters) {
		logger.warn(template, parameters);
	}
}