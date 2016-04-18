/**
 * 
 */
package com.github.aro_tech.fluent_jdbc.core;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.github.aro_tech.fluent_jdbc.core.arguments.ArgumentSetterBuilder;
import com.github.aro_tech.fluent_jdbc.core.arguments.JDBCArgumentSetter;
import com.github.aro_tech.fluent_jdbc.core.connection.IJDBCConnectionProvider;
import com.github.aro_tech.fluent_jdbc.core.impl.SQLExecutor;
import com.github.aro_tech.fluent_jdbc.core.logging.ILogger;
import com.github.aro_tech.fluent_jdbc.core.logging.SQLLogger;
import com.github.aro_tech.fluent_jdbc.core.results.ResultSetReader;
import com.github.aro_tech.fluent_jdbc.core.results.ResultSetReaderReusingConnection;

/**
 * Fluid JDBC request executor
 * 
 * @author aro_tech
 *
 */
public class RequestBuilder<T> {
	private final ILogger logger;
	private final ArgumentSetterBuilder currentArgSetter;
	private final Class<T> returnType;
	private final List<JDBCArgumentSetter> setters;
	private final ResultSetReader<T> reader;
	private final ResultSetReaderReusingConnection<T> readerForFollowUp;
	private final IJDBCConnectionProvider connectionProvider;
	private final Connection jdbcConnection;
	private final String sql;
	private final List<SQLException> errorsCaught;

	private RequestBuilder(ILogger logger,
			ArgumentSetterBuilder currentArgSetter, Class<T> returnType,
			List<JDBCArgumentSetter> setters, ResultSetReader<T> handler,
			ResultSetReaderReusingConnection<T> handlerForFollowUp,
			IJDBCConnectionProvider connectionProvider,
			Connection jdbcConnection, String sql,
			List<SQLException> errorCatcher) {
		super();
		if (null != logger) {
			this.logger = logger;
		} else {
			this.logger = new SQLLogger();
		}

		this.errorsCaught = errorCatcher;

		this.sql = sql;

		this.currentArgSetter = currentArgSetter;

		this.returnType = returnType;

		if (null == setters) {
			this.setters = new ArrayList<JDBCArgumentSetter>();
		} else {
			this.setters = setters;
		}

		this.reader = handler;
		this.readerForFollowUp = handlerForFollowUp;
		this.connectionProvider = connectionProvider;
		this.jdbcConnection = jdbcConnection;
	}

	/**
	 * Factory method
	 * 
	 * @param returnTypeClass
	 *            Type of object returned by the request
	 * @return RequestBuilder for the given return type
	 */
	public static <T> RequestBuilder<T> returning(final Class<T> returnTypeClass) {
		return new RequestBuilder<T>(null, null, returnTypeClass, null, null,
				null, null, null, null, null);
	}

	/**
	 * Factory method for lists
	 * 
	 * @param returnTypeClass
	 *            Type of object returned by the request
	 * @return RequestBuilder for the given return type
	 */
	@SuppressWarnings("unchecked")
	public static <U> RequestBuilder<List<U>> returningListOf(
			final Class<U> returnTypeListElementClass) {
		return new RequestBuilder<List<U>>(null, null,
				(Class<List<U>>) new ArrayList<U>().getClass().getSuperclass(),
				null, null, null, null, null, null, null);
	}

	/**
	 * Factory method for sets
	 * 
	 * @param returnTypeClass
	 *            Type of object returned by the request
	 * @return RequestBuilder for the given return type
	 */
	@SuppressWarnings("unchecked")
	public static <U> RequestBuilder<Set<U>> returningSetOf(
			final Class<U> returnTypeListElementClass) {
		return new RequestBuilder<Set<U>>(null, null,
				(Class<Set<U>>) new HashSet<U>().getClass().getSuperclass(),
				null, null, null, null, null, null, null);
	}
	
	/**
	 * Factory method
	 * 
	 * @return RequestBuilder which returns nothing (e.g. for update, delete,
	 *         insert)
	 */
	public static RequestBuilder<Void> returningVoid() {
		return returning(Void.class);
	}

	/**
	 * Factory method - generates default ResultSetReader returning the first
	 * column as a Long
	 * 
	 * @return RequestBuilder which returns a Long (e.g. for a count)
	 */
	public static RequestBuilder<Long> returningLong() {
		return returning(Long.class).withResultReader(rs -> {
			if (rs.next()) {
				return rs.getLong(1);
			}
			return null;
		});
	}

	/**
	 * Factory method - generates default ResultSetReader returning the first
	 * column as a Integer
	 * 
	 * @return RequestBuilder which returns a Integer (e.g. for a count)
	 */
	public static RequestBuilder<Integer> returningInteger() {
		return returning(Integer.class).withResultReader(rs -> {
			if (rs.next()) {
				return rs.getInt(1);
			}
			return null;
		});
	}

	/**
	 * Factory method - generates default ResultSetReader returning the first
	 * column as a String
	 * 
	 * @return RequestBuilder which returns a String (e.g. for a count)
	 */
	public static RequestBuilder<String> returningString() {
		return returning(String.class).withResultReader(rs -> {
			if (rs.next()) {
				return rs.getString(1);
			}
			return null;
		});
	}

	/**
	 * Specify the logger to use
	 * 
	 * @param loggerToUse
	 * @return builder
	 */
	public RequestBuilder<T> withLogger(ILogger loggerToUse) {
		return new RequestBuilder<T>(loggerToUse, currentArgSetter, returnType,
				setters, reader, readerForFollowUp, connectionProvider,
				jdbcConnection, sql, errorsCaught);
	}

	/**
	 * Specify the SQL request
	 * 
	 * @param sql
	 */
	public RequestBuilder<T> withSQL(String sql) {
		return new RequestBuilder<T>(logger, currentArgSetter, returnType,
				setters, reader, readerForFollowUp, connectionProvider,
				jdbcConnection, sql, errorsCaught);
	}

	/**
	 * Specify the connection provider
	 * 
	 * @param provider
	 *            Object which provides a JDBC connection
	 */
	public RequestBuilder<T> withConnectionProvider(
			IJDBCConnectionProvider provider) {
		return new RequestBuilder<T>(logger, currentArgSetter, returnType,
				setters, reader, readerForFollowUp, provider, jdbcConnection,
				sql, errorsCaught);
	}

	/**
	 * Specify the connection
	 * 
	 * @param connection
	 *            a JDBC connection
	 */
	public RequestBuilder<T> withConnection(Connection connection) {
		return new RequestBuilder<T>(logger, currentArgSetter, returnType,
				setters, reader, readerForFollowUp, connectionProvider,
				connection, sql, errorsCaught);
	}

	/**
	 * Specify one or more argument setters
	 * 
	 * @param setters
	 *            one or more argument setters (0 is possible, but useless)
	 * @return builder
	 */
	public RequestBuilder<T> withArgumentSetters(JDBCArgumentSetter... setters) {

		return new RequestBuilder<T>(logger, currentArgSetter, returnType,
				Arrays.asList(setters), reader, readerForFollowUp,
				connectionProvider, jdbcConnection, sql, errorsCaught);
	}

	/**
	 * Specify the reader which converts the ResultSet into the return type
	 * object(s)
	 * 
	 * @param reader
	 *            lambda or instance of ResultSetReader
	 * @return builder
	 */
	public RequestBuilder<T> withResultReader(ResultSetReader<T> reader) {
		return new RequestBuilder<T>(logger, currentArgSetter, returnType,
				setters, reader, null, connectionProvider, jdbcConnection, sql,
				errorsCaught);
	}

	/**
	 * Specify the reader which converts the ResultSet into the return type
	 * object(s)
	 * 
	 * @param reader
	 *            lambda or instance of ResultSetReaderReusingConnection. This
	 *            reader receives, in a addition to the ResultSet, the active
	 *            SQL connection, allowing follow-up queries to refine the
	 *            results
	 * @return builder
	 */
	public RequestBuilder<T> withResultReader(
			ResultSetReaderReusingConnection<T> handler) {
		return new RequestBuilder<T>(logger, currentArgSetter, returnType,
				setters, null, handler, connectionProvider, jdbcConnection,
				sql, errorsCaught);
	}

	/**
	 * Add a SQL parameter (corresponding to a "?" in the SQL
	 * 
	 * @param param
	 *            parameter to add
	 * @return builder
	 */
	public RequestBuilder<T> addParam(boolean param) {
		ArgumentSetterBuilder currentArgSetterToUse = createArgSetterBuilderIfNeeded();
		return new RequestBuilder<T>(logger, currentArgSetterToUse.add(param),
				returnType, setters, reader, readerForFollowUp,
				connectionProvider, jdbcConnection, sql, errorsCaught);
	}

	/**
	 * Add a SQL parameter (corresponding to a "?" in the SQL
	 * 
	 * @param param
	 *            parameter to add
	 * @return builder
	 */
	public RequestBuilder<T> addParam(double param) {
		ArgumentSetterBuilder currentArgSetterToUse = createArgSetterBuilderIfNeeded();
		return new RequestBuilder<T>(logger, currentArgSetterToUse.add(param),
				returnType, setters, reader, readerForFollowUp,
				connectionProvider, jdbcConnection, sql, errorsCaught);
	}

	/**
	 * Add a SQL parameter (corresponding to a "?" in the SQL
	 * 
	 * @param param
	 *            parameter to add
	 * @return builder
	 */
	public RequestBuilder<T> addParam(float param) {
		ArgumentSetterBuilder currentArgSetterToUse = createArgSetterBuilderIfNeeded();
		return new RequestBuilder<T>(logger, currentArgSetterToUse.add(param),
				returnType, setters, reader, readerForFollowUp,
				connectionProvider, jdbcConnection, sql, errorsCaught);
	}

	/**
	 * Add a SQL parameter (corresponding to a "?" in the SQL
	 * 
	 * @param param
	 *            parameter to add
	 * @return builder
	 */
	public RequestBuilder<T> addParam(int param) {
		ArgumentSetterBuilder currentArgSetterToUse = createArgSetterBuilderIfNeeded();
		return new RequestBuilder<T>(logger, currentArgSetterToUse.add(param),
				returnType, setters, reader, readerForFollowUp,
				connectionProvider, jdbcConnection, sql, errorsCaught);
	}

	/**
	 * Add a SQL parameter (corresponding to a "?" in the SQL
	 * 
	 * @param param
	 *            parameter to add
	 * @return builder
	 */
	public RequestBuilder<T> addParam(LocalDateTime param) {
		ArgumentSetterBuilder currentArgSetterToUse = createArgSetterBuilderIfNeeded();
		return new RequestBuilder<T>(logger, currentArgSetterToUse.add(param),
				returnType, setters, reader, readerForFollowUp,
				connectionProvider, jdbcConnection, sql, errorsCaught);
	}

	/**
	 * Add a SQL parameter (corresponding to a "?" in the SQL
	 * 
	 * @param param
	 *            parameter to add
	 * @return builder
	 */
	public RequestBuilder<T> addParam(long param) {
		ArgumentSetterBuilder currentArgSetterToUse = createArgSetterBuilderIfNeeded();
		return new RequestBuilder<T>(logger, currentArgSetterToUse.add(param),
				returnType, setters, reader, readerForFollowUp,
				connectionProvider, jdbcConnection, sql, errorsCaught);
	}

	/**
	 * Add a SQL parameter (corresponding to a "?" in the SQL
	 * 
	 * @param param
	 *            parameter to add
	 * @return builder
	 */
	public RequestBuilder<T> addParam(String param) {
		ArgumentSetterBuilder currentArgSetterToUse = createArgSetterBuilderIfNeeded();
		return new RequestBuilder<T>(logger, currentArgSetterToUse.add(param),
				returnType, setters, reader, readerForFollowUp,
				connectionProvider, jdbcConnection, sql, errorsCaught);
	}

	/**
	 * Add a SQL parameter (corresponding to a "?" in the SQL
	 * 
	 * @param param
	 *            parameter to add
	 * @return builder
	 */
	public RequestBuilder<T> addParam(Timestamp param) {
		ArgumentSetterBuilder currentArgSetterToUse = createArgSetterBuilderIfNeeded();
		return new RequestBuilder<T>(logger, currentArgSetterToUse.add(param),
				returnType, setters, reader, readerForFollowUp,
				connectionProvider, jdbcConnection, sql, errorsCaught);
	}

	/**
	 * Add a SQL parameter (corresponding to a "?" in the SQL
	 * 
	 * @param param
	 *            parameter to add
	 * @param Constant
	 *            from java.sql.Types
	 * @return builder
	 */
	public RequestBuilder<T> addParam(Object param, int sqlType) {
		ArgumentSetterBuilder currentArgSetterToUse = createArgSetterBuilderIfNeeded();
		return new RequestBuilder<T>(logger, currentArgSetterToUse.add(param,
				sqlType), returnType, setters, reader, readerForFollowUp,
				connectionProvider, jdbcConnection, sql, errorsCaught);
	}

	/**
	 * Add a SQL parameter (corresponding to a "?" in the SQL This method is
	 * specific to PostgreSQL, which has support for the JSON data type
	 * 
	 * @param param
	 *            parameter to add (a JSON String)
	 * @return builder
	 */
	public RequestBuilder<T> addPGJSONParam(String param) {
		ArgumentSetterBuilder currentArgSetterToUse = createArgSetterBuilderIfNeeded();
		return new RequestBuilder<T>(logger,
				currentArgSetterToUse.addPGjson(param), returnType, setters,
				reader, readerForFollowUp, connectionProvider, jdbcConnection,
				sql, errorsCaught);
	}

	private ArgumentSetterBuilder createArgSetterBuilderIfNeeded() {
		ArgumentSetterBuilder currentArgSetterToUse = currentArgSetter;
		if (null == currentArgSetterToUse) {
			currentArgSetterToUse = ArgumentSetterBuilder.withLogging(logger);
		}
		return currentArgSetterToUse;
	}

	/**
	 * Execute the request based on the supplied info
	 * 
	 * @return The result of the request
	 */
	public T execute() {
		SQLExecutor<T> exec = new SQLExecutor<T>(logger);
		finalizeParameters();

		if (null == this.connectionProvider) {
			if (null != this.jdbcConnection) {
				return executeSubrequest(exec);
			}
			logger.error("No connection provided for request sql={}", sql);
			return null;
		} else {
			if (null == this.readerForFollowUp) {
				return singleExecuteWithProviderAndNoSubrequests(exec);
			} else {
				return singleExecuteWithSubrequests(exec);
			}
		}
	}

	private T singleExecuteWithSubrequests(SQLExecutor<T> exec) {
		if (this.setters.size() < 1) {
			T returnVal = exec.executeWithSubrequests(connectionProvider, sql,
					null, this.readerForFollowUp);
			copyOutCaughtErrors(exec);
			return returnVal;
		} else if (this.setters.size() < 2) {
			T returnVal = exec.executeWithSubrequests(connectionProvider, sql,
					this.setters.get(0), this.readerForFollowUp);
			copyOutCaughtErrors(exec);
			return returnVal;
		}
		logger.error(
				"Request not executed. RequestBuilder configuration not implemented (multiple param sets in subrequest). : sql={}",
				sql);
		return null;
	}

	private T singleExecuteWithProviderAndNoSubrequests(SQLExecutor<T> exec) {
		if (this.setters.size() < 1) {
			T returnVal = exec.execute(connectionProvider, sql, null,
					this.reader);
			copyOutCaughtErrors(exec);
			return returnVal;
		} else if (this.setters.size() < 2) {
			T returnVal = exec.execute(connectionProvider, sql,
					this.setters.get(0), this.reader);
			copyOutCaughtErrors(exec);
			return returnVal;
		} else {
			logger.error(
					"Call to exec() should be replaced by call to execMultiple(). sql={}",
					sql);
			return null;
		}
	}

	private void copyOutCaughtErrors(SQLExecutor<T> exec) {
		if (null != this.errorsCaught) {
			this.errorsCaught.addAll(exec.getCaughtErrors());
		}
	}

	private T executeSubrequest(SQLExecutor<T> exec) {
		try {
			T returnVal = exec.execute(sql, this.setters.size() < 1 ? null
					: this.setters.get(0), reader, jdbcConnection);
			copyOutCaughtErrors(exec);
			return returnVal;
		} catch (SQLException e) {
			logger.error("SQL error. sql=" + sql, e);
			return null;
		}
	}

	/**
	 * Execute the request for each set of parameters provided
	 * 
	 * @return list of results
	 */
	public List<T> executeMultiple() {
		SQLExecutor<T> exec = new SQLExecutor<T>(logger);
		finalizeParameters();
		List<T> returnValues = exec.executeMultiple(connectionProvider, sql,
				reader, setters.toArray(new JDBCArgumentSetter[0]));
		copyOutCaughtErrors(exec);
		return returnValues;
	}

	private void finalizeParameters() {
		if (null != this.currentArgSetter) {
			this.setters.add(this.currentArgSetter.build());
		}
	}

	/**
	 * Add a list to collect any exceptions caught
	 * 
	 * @param caughtErrors
	 * @return builder
	 */
	public RequestBuilder<T> withErrorCollector(List<SQLException> caughtErrors) {
		return new RequestBuilder<T>(logger, currentArgSetter, returnType,
				setters, reader, readerForFollowUp, connectionProvider,
				jdbcConnection, sql, caughtErrors);
	}
}
