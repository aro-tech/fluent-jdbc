package com.github.aro_tech.fluent_jdbc.core.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.github.aro_tech.fluent_jdbc.core.arguments.JDBCArgumentSetter;
import com.github.aro_tech.fluent_jdbc.core.connection.IJDBCConnectionProvider;
import com.github.aro_tech.fluent_jdbc.core.logging.ILogger;
import com.github.aro_tech.fluent_jdbc.core.results.ResultSetReader;
import com.github.aro_tech.fluent_jdbc.core.results.ResultSetReaderReusingConnection;

/**
 * Allows JDBC query set-up and handling via lambdas
 * 
 * @author aro_tech
 *
 */
public class SQLExecutor<T> {
	private final ILogger logger;
	private final List<SQLException> caughtErrors = new ArrayList<SQLException>();

	/**
	 * Static factory
	 * 
	 * @param clazz
	 * @param logger
	 * @return
	 */
	public static <T> SQLExecutor<T> withLogger(Class<T> clazz, ILogger logger) {
		return new SQLExecutor<T>(logger);
	}

	/**
	 * 
	 * Constructor
	 * 
	 * @param logger
	 */
	public SQLExecutor(ILogger logger) {
		super();
		this.logger = logger;
	}

	/**
	 * Execute SQL using lambdas
	 * 
	 * @param connectionProvider
	 * @param sql
	 * @param argumentSetter
	 * @param resultHandler
	 * @return
	 */
	public T execute(IJDBCConnectionProvider connectionProvider, String sql,
			JDBCArgumentSetter argumentSetter, ResultSetReader<T> resultHandler) {
		try (Connection c = connectionProvider.getConnection()) {
			return execute(sql, argumentSetter, resultHandler, c);
		} catch (SQLException e) {
			return handleError(sql, e);
		}
	}

	/**
	 * 
	 * @param connectionProvider
	 * @param sql
	 * @param argumentSetter
	 * @param resultHandler
	 * @return
	 */
	public T executeWithSubrequests(IJDBCConnectionProvider connectionProvider,
			String sql, JDBCArgumentSetter argumentSetter,
			ResultSetReaderReusingConnection<T> resultHandler) {
		try (Connection c = connectionProvider.getConnection()) {
			return listToSingleValue(executeImpl(sql, null, resultHandler, c,
					new ArrayList<T>(), argumentSetter));
		} catch (SQLException e) {
			return handleError(sql, e);
		}
	}

	private T listToSingleValue(final List<T> resultsList) {
		if (resultsList.isEmpty()) {
			return null;
		}
		return resultsList.get(0);
	}

	/**
	 * Query execution using an existing connection Useful for running nested
	 * SQL requests based on query results
	 * 
	 * @param sql
	 * @param argumentSetter
	 * @param resultHandler
	 * @param c
	 * @throws SQLException
	 */
	public T execute(String sql, JDBCArgumentSetter argumentSetter,
			ResultSetReader<T> resultHandler, Connection c)
			throws SQLException {
		return listToSingleValue(executeImpl(sql, resultHandler, null, c,
				new ArrayList<T>(), argumentSetter));
	}

	/**
	 * Call a request multiple times with different arguments
	 * 
	 * @param connectionProvider
	 * @param sql
	 * @param resultHandler
	 * @param argumentSetters
	 * @return
	 */
	public List<T> executeMultiple(IJDBCConnectionProvider connectionProvider,
			String sql, ResultSetReader<T> resultHandler,
			JDBCArgumentSetter... argumentSetters) {
		final ArrayList<T> resultsListOutParam = new ArrayList<T>();
		try (Connection c = connectionProvider.getConnection()) {
			return executeImpl(sql, resultHandler, null, c,
					resultsListOutParam, argumentSetters);
		} catch (SQLException e) {
			handleError(sql, e);
			return resultsListOutParam;
		}
	}

	private List<T> executeImpl(String sql, ResultSetReader<T> resultHandler,
			ResultSetReaderReusingConnection<T> resultHandlerReusingC,
			Connection c, List<T> resultsListOutParam,
			JDBCArgumentSetter... argumentSetters) throws SQLException {
		logger.info("About to execute SQL request: \n{}", sql);
		PreparedStatement stmnt = c.prepareStatement(sql);
		JDBCArgumentSetter[] argumentSettersToUse = safeArgumentSetters(argumentSetters);
		int counter = 1;
		for (JDBCArgumentSetter argumentSetter : argumentSettersToUse) {
			try {
				argumentSetter.setArguments(stmnt);
				if (stmnt.execute()) {
					ResultSet rs = stmnt.getResultSet();
					final T results = getResults(resultHandler,
							resultHandlerReusingC, c, rs);
					if (null != results) {
						logger.debug("Query returned results: \n{}", results);
					} else {
						logger.debug("No results.");
					}
					resultsListOutParam.add(results);
				} else {
					logger.debug(
							"Query executed with no result sets but with update count {}",
							stmnt.getUpdateCount());
				}
				counter++;
			} catch (SQLException e) {
				if (!caughtErrors.isEmpty()) {
					caughtErrors.add(e);
					throw caughtErrors.remove(0); // don't postpone error
													// handling if more than
					// one error
				} else if (null == resultsListOutParam
						|| resultsListOutParam.size() < 1) {
					throw e;
				} else {
					caughtErrors.add(e);
					logger.warn(
							"In multiple SQL executions, error at counter={}",
							counter);
				}
			}
		}
		if (!caughtErrors.isEmpty()) {
			throw caughtErrors.remove(0); // after getting as many results as
											// possible, handle
			// the one error which arose
		}
		return resultsListOutParam;
	}

	private T getResults(ResultSetReader<T> resultHandler,
			ResultSetReaderReusingConnection<T> resultHandlerReusingC,
			Connection c, ResultSet rs) throws SQLException {
		return null != resultHandler ? resultHandler.handleResults(rs)
				: null != resultHandlerReusingC ? resultHandlerReusingC
						.handleResults(rs, c) : null;
	}

	private JDBCArgumentSetter[] safeArgumentSetters(
			JDBCArgumentSetter... argumentSetters) {
		JDBCArgumentSetter[] argumentSettersToUse = argumentSetters;
		if (null == argumentSetters || argumentSetters.length < 1) {
			argumentSettersToUse = new JDBCArgumentSetter[1];

		}
		for (int i = 0; i < argumentSettersToUse.length; i++) {
			if (null == argumentSettersToUse[i]) {
				argumentSettersToUse[i] = s -> {
				};
			}
		}
		return argumentSettersToUse;
	}

	/**
	 * Error handling
	 * 
	 * @param sql
	 * @param e
	 * @return null (or a default object can be returned if overridden)
	 */
	protected T handleError(String sql, SQLException e) {
		logger.error("Exception for SQL request: " + sql, e);
		this.caughtErrors.add(e);
		return null;
	}

	/**
	 * @return List of SQLExceptions caught during execution. Never null, but
	 *         hopefully empty.
	 */
	public List<SQLException> getCaughtErrors() {
		return caughtErrors;
	}

}
