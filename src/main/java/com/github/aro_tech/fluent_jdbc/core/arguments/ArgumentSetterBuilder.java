/**
 * 
 */
package com.github.aro_tech.fluent_jdbc.core.arguments;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import org.postgresql.util.PGobject;

import com.github.aro_tech.fluent_jdbc.core.logging.ILogger;

/**
 * Fluent interface to build an argument setter lambda
 * 
 * @author aro_tech
 *
 */
public class ArgumentSetterBuilder {
	private final ILogger logger;
	private final List<SingleArgumentSetter> setters;
	private final StringBuilder logTextBuilder;

	@FunctionalInterface
	private static interface SingleArgumentSetter {
		void setOneArgument(PreparedStatement stmnt, int index)
				throws SQLException;
	}

	/**
	 * Constructor
	 * 
	 * @param logger
	 */
	private ArgumentSetterBuilder(ILogger logger) {
		super();
		this.logger = logger;
		this.setters = new ArrayList<SingleArgumentSetter>();
		this.logTextBuilder = new StringBuilder();
	}

	/**
	 * Static factory
	 * 
	 * @param logger
	 *            For debug statements with arguments
	 * @return
	 */
	public static ArgumentSetterBuilder withLogging(ILogger logger) {
		return new ArgumentSetterBuilder(logger);
	}

	/**
	 * Add an integer argument
	 * 
	 * @param arg
	 *            SQL parameter value
	 * @return Updated builder object (a new copy - previous builder state is
	 *         preserved in case branching of states in needed)
	 */
	public ArgumentSetterBuilder add(int arg) {
		return generateNextBuilder("" + arg, (stmnt, index) -> {
			stmnt.setInt(index, arg);
		}, "(int)");
	}

	private ArgumentSetterBuilder generateNextBuilder(String argAsStringForLog,
			final SingleArgumentSetter singleArgSetter, String typeForLog) {
		ArgumentSetterBuilder next = new ArgumentSetterBuilder(logger);
		next.setters.addAll(this.setters);
		next.setters.add(singleArgSetter);
		if (null != logger) {
			appendLogText(argAsStringForLog, next, typeForLog);
		}
		return next;
	}

	private void appendLogText(String argAsString, ArgumentSetterBuilder next,
			final String typeText) {
		next.logTextBuilder.append(logTextBuilder.toString())
				.append(next.setters.size()).append(") ").append(typeText)
				.append(" ").append(argAsString).append(" ");
	}

	/**
	 * Add a double precision number argument
	 * 
	 * @param arg
	 *            SQL parameter value
	 * @return Updated builder object (a new copy - previous builder state is
	 *         preserved in case branching of states in needed)
	 */
	public ArgumentSetterBuilder add(double arg) {
		return generateNextBuilder("" + arg, (stmnt, index) -> {
			stmnt.setDouble(index, arg);
		}, "(double)");
	}

	/**
	 * Add a floating-point number argument
	 * 
	 * @param arg
	 *            SQL parameter value
	 * @return Updated builder object (a new copy - previous builder state is
	 *         preserved in case branching of states in needed)
	 */
	public ArgumentSetterBuilder add(float arg) {
		return generateNextBuilder("" + arg, (stmnt, index) -> {
			stmnt.setFloat(index, arg);
		}, "(float)");
	}

	public ArgumentSetterBuilder add(long arg) {
		return generateNextBuilder("" + arg, (stmnt, index) -> {
			stmnt.setLong(index, arg);
		}, "(long)");
	}

	public ArgumentSetterBuilder add(String arg) {
		return generateNextBuilder(null != arg ? "'" + arg + "'" : "null", (
				stmnt, index) -> {
			stmnt.setString(index, arg);
		}, "(String)");
	}

	/**
	 * Add a date/time argument
	 * 
	 * @param arg
	 *            SQL parameter value
	 * @return Updated builder object (a new copy - previous builder state is
	 *         preserved in case branching of states in needed)
	 */
	public ArgumentSetterBuilder add(LocalDateTime arg) {
		return generateNextBuilder("" + arg, (stmnt, index) -> {
			final Timestamp ts = null != arg ? Timestamp.valueOf(arg) : null;
			stmnt.setTimestamp(index, ts);
		}, "(LocalDateTime)");
	}

	/**
	 * Add a date/time argument
	 * 
	 * @param arg
	 *            SQL parameter value
	 * @return Updated builder object (a new copy - previous builder state is
	 *         preserved in case branching of states in needed)
	 */
	public ArgumentSetterBuilder add(Timestamp arg) {
		return generateNextBuilder("" + arg, (stmnt, index) -> {
			stmnt.setTimestamp(index, arg);
		}, "(Timestamp)");
	}

	/**
	 * Add a SQL argument of the specified type
	 * 
	 * @param arg
	 *            SQL parameter value
	 * @param sqlType
	 *            constant from java.sql.Types
	 * @return Updated builder object (a new copy - previous builder state is
	 *         preserved in case branching of states in needed)
	 */
	public ArgumentSetterBuilder add(Object arg, int sqlType) {
		return generateNextBuilder("" + arg, objectOrNullSetter(arg, sqlType),
				"(Object of type" + sqlType + ")");
	}

	private SingleArgumentSetter objectOrNullSetter(Object arg, int sqlType) {
		final SingleArgumentSetter singleArgSetter;
		if (null == arg) {
			singleArgSetter = (stmnt, index) -> {
				stmnt.setNull(index, sqlType);
			};
		} else {
			singleArgSetter = (stmnt, index) -> {
				stmnt.setObject(index, arg, sqlType);
			};
		}
		return singleArgSetter;
	}

	private SingleArgumentSetter postgresJSONSetter(String json) {
		final SingleArgumentSetter singleArgSetter;
		if (null == json) {
			singleArgSetter = (stmnt, index) -> {
				stmnt.setNull(index, Types.JAVA_OBJECT);
			};
		} else {
			singleArgSetter = (stmnt, index) -> {
				PGobject pg = new PGobject();
				pg.setType("json");
				pg.setValue(json);
				stmnt.setObject(index, pg);
			};
		}
		return singleArgSetter;
	}

	
	/**
	 * @return Constructed function that sets JDBC parameters for a given
	 *         PreparedStatement
	 */
	public JDBCArgumentSetter build() {
		return stmnt -> {
			logger.debug("Setting SQL parameters: {}",
					this.logTextBuilder.toString());
			int index = 1;
			for (SingleArgumentSetter cur : setters) {
				cur.setOneArgument(stmnt, index++);
			}
		};
	}

	/**
	 * Add a boolean argument
	 * 
	 * @param arg
	 *            SQL parameter value
	 * @return Updated builder object (a new copy - previous builder state is
	 *         preserved in case branching of states in needed)
	 */
	public ArgumentSetterBuilder add(boolean arg) {
		return generateNextBuilder("" + arg, (stmnt, index) -> {
			stmnt.setBoolean(index, arg);
		}, "(boolean)");
	}

	public ArgumentSetterBuilder addPGjson(String json) {
		ArgumentSetterBuilder next = new ArgumentSetterBuilder(logger);
		next.setters.addAll(this.setters);
		next.setters.add(postgresJSONSetter(json));
		if (null != logger) {
			appendLogText("" + json, next, "(json)");
		}
		return next;
	}
}
