package org.fluentjdbc.core;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import com.github.aro_tech.extended_mockito.ExtendedMockito;
import com.github.aro_tech.fluent_jdbc.core.arguments.ArgumentSetterBuilder;
import com.github.aro_tech.fluent_jdbc.core.arguments.JDBCArgumentSetter;
import com.github.aro_tech.fluent_jdbc.core.logging.ILogger;

public class ArgumentSetterBuilderTest implements ExtendedMockito {

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
	}

	@Test
	public void should_build_with_some_nulls() throws SQLException {
		ILogger logger = mock(ILogger.class);
		String nullStr = null;
		LocalDateTime nullDate = null;
		final LocalDateTime now = LocalDateTime.now();
		JDBCArgumentSetter setter = ArgumentSetterBuilder.withLogging(logger)
				.add(47).add(nullStr).add(nullDate)
				.add(now).add(Double.valueOf(1.12))
				.add(111L).add("Hello").build();

		PreparedStatement stmnt = mock(PreparedStatement.class);
		setter.setArguments(stmnt);

		verify(logger)
				.debug("Setting SQL parameters: {}",
						"1) (int) 47 2) (String) null 3) (LocalDateTime) null "
								+ "4) (LocalDateTime) "
								+ now.toString()
								+ " "
								+ "5) (double) 1.12 6) (long) 111 7) (String) 'Hello' ");

		verify(stmnt).setInt(1, 47);
		verify(stmnt).setString(2, null);
		verify(stmnt).setTimestamp(3, null);
		verify(stmnt).setTimestamp(eq(4),
				eq(Timestamp.valueOf(now)));
		verify(stmnt).setDouble(5, 1.12);
		verify(stmnt).setLong(6, 111L);
		verify(stmnt).setString(7, "Hello");
	}

}
