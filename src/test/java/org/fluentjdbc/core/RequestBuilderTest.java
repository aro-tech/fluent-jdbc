/**
 * 
 */
package org.fluentjdbc.core;

import java.sql.Connection;
import java.time.LocalDateTime;

import org.junit.Before;
import org.junit.Test;

import com.github.aro_tech.fluent_jdbc.core.RequestBuilder;
import com.github.aro_tech.fluent_jdbc.core.arguments.ArgumentSetterBuilder;
import com.github.aro_tech.fluent_jdbc.core.arguments.JDBCArgumentSetter;
import com.github.aro_tech.fluent_jdbc.core.connection.IJDBCConnectionProvider;
import com.github.aro_tech.fluent_jdbc.core.connection.PostgreSQLConnectionProvider;
import com.github.aro_tech.fluent_jdbc.core.logging.ILogger;
import com.github.aro_tech.fluent_jdbc.core.logging.SQLLogger;
import com.github.aro_tech.tdd_mixins.AssertJ;

/**
 * @author aro_tech
 *
 */
public class RequestBuilderTest implements AssertJ {
	
	public static final String TEST_DB_IP = "192.168.99.100";


	private static final int INSERT_COUNT = 20;
	/*
	 * CREATE TABLE jdbc_test ( id bigint PRIMARY KEY, test_varchar character
	 * varying(100), test_bool boolean, test_text text, test_json json,
	 * test_float float, test_double double precision, test_smallint smallint )
	 */

	private IJDBCConnectionProvider connectionProvider = new PostgreSQLConnectionProvider(
			TEST_DB_IP, "fluentjdbc", "postgres", "postgres");
	private ILogger logger;

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
		try (Connection c = connectionProvider.getConnection()) {
			RequestBuilder.returningVoid()
					.withConnectionProvider(connectionProvider)
					.withSQL("DELETE FROM jdbc_test").executeMultiple();
		} catch (Exception e) {
			e.printStackTrace();
		}
		//logger = Mockito.mock(ILogger.class);
		logger = new SQLLogger();
	}


	/**
	 * Test method for
	 * {@link com.github.aro_tech.fluent_jdbc.core.RequestBuilder#executeMultiple()}.
	 */
	@Test
	public void can_do_multi_insert() {
		JDBCArgumentSetter[] setters = new JDBCArgumentSetter[INSERT_COUNT];
		StringBuilder longText = new StringBuilder();
		for (int i = 0; i < setters.length; i++) {
			for (int j = 0; j < 100; j++) {
				longText.append(j).append(" ");
			}
			setters[i] = ArgumentSetterBuilder.withLogging(logger)
					.add((long) i + 10000).add("Hello " + (i + 1))
					.add(Boolean.TRUE).add(longText.toString())
					.addPGjson("{\"a\": 1, \"b\": null}").add(3.14f)
					.add(3.14159d).add(1).add(LocalDateTime.now().minusDays(i))
					.build();
		}

		RequestBuilder
				.returningVoid()
				.withConnectionProvider(connectionProvider)
				.withLogger(logger)
				.withSQL(
						"INSERT INTO jdbc_test (id,test_varchar,test_bool,test_text,test_json,test_float,test_double,test_smallint,test_timestamp) "
								+ "VALUES (?,?,?,?,?,?,?,?,?)")
				.withArgumentSetters(setters).executeMultiple();

		int count = RequestBuilder.returningInteger()
				.withConnectionProvider(connectionProvider).withLogger(logger)
				.withSQL("SELECT COUNT(*) FROM jdbc_test").execute();
		
		assertThat(count).isEqualTo(INSERT_COUNT);

		/*
		 * DROP TABLE jdbc_test;
		 * 
		 * CREATE TABLE jdbc_test ( id bigint PRIMARY KEY, test_varchar
		 * character varying(100), test_bool boolean, test_text text, test_json
		 * json, test_float float, test_double double precision, test_smallint
		 * smallint, test_timestamp timestamp );
		 */
	}

	// TODO: scenario w/ transaction and rollback ?
}
