package com.firebolt.jdbc.statement;

import com.firebolt.jdbc.connection.FireboltConnection;
import com.firebolt.jdbc.connection.settings.FireboltProperties;
import com.firebolt.jdbc.exception.FireboltException;
import com.firebolt.jdbc.resultset.FireboltResultSet;
import com.firebolt.jdbc.service.FireboltStatementService;
import lombok.SneakyThrows;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.Closeable;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.sql.Wrapper;
import java.util.HashMap;
import java.util.Optional;
import java.util.stream.Stream;

import static java.sql.Statement.CLOSE_CURRENT_RESULT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class FireboltStatementTest {

	@Captor
	ArgumentCaptor<StatementInfoWrapper> queryInfoWrapperArgumentCaptor;
	@Mock
	private FireboltStatementService fireboltStatementService;
	@Mock
	private FireboltConnection fireboltConnection;

	private static FireboltStatement statement;

	private static Stream<Arguments> unsupported() {
		return Stream.of(
				Arguments.of("clearWarnings", (Executable) () -> statement.clearWarnings()),
				Arguments.of("setCursorName", (Executable) () -> statement.setCursorName("my_cursor")),
				Arguments.of("addBatch", (Executable) () -> statement.addBatch("insert into people (id, name) values (?, ?))")),
				Arguments.of("clearBatch", (Executable) () -> statement.clearBatch()),
				Arguments.of("executeBatch", (Executable) () -> statement.executeBatch()),
				Arguments.of("getGeneratedKeys", (Executable) () -> statement.getGeneratedKeys()),
				Arguments.of("executeUpdate(auto generated keys)", (Executable) () -> statement.executeUpdate("insert", Statement.NO_GENERATED_KEYS)),
				Arguments.of("executeUpdate(column indexes)", (Executable) () -> statement.executeUpdate("insert", new int[0])),
				Arguments.of("executeUpdate(column names)", (Executable) () -> statement.executeUpdate("insert", new String[0])),
				Arguments.of("execute(auto generated keys)", (Executable) () -> statement.execute("insert", Statement.NO_GENERATED_KEYS)),
				Arguments.of("execute(column indexes)", (Executable) () -> statement.execute("insert", new int[0])),
				Arguments.of("execute(column names)", (Executable) () -> statement.execute("insert", new String[0])),
				Arguments.of("setPoolable(true)", (Executable) () -> statement.setPoolable(true))
		);
	}

	@ParameterizedTest(name = "{0}")
	@MethodSource("unsupported")
	void shouldThrowSQLFeatureNotSupportedException(String name, Executable function) {
		statement = FireboltStatement.builder().statementService(fireboltStatementService)
				.sessionProperties(mock(FireboltProperties.class)).connection(mock(FireboltConnection.class)).build();
		assertThrows(SQLFeatureNotSupportedException.class, function);
	}

	@Test
	void shouldExtractAdditionalPropertiesAndNotExecuteQueryWhenSetParamIsUsed() throws SQLException {
		FireboltConnection connection = mock(FireboltConnection.class);
		FireboltProperties fireboltProperties = FireboltProperties.builder().additionalProperties(new HashMap<>())
				.build();
		FireboltStatement fireboltStatement = FireboltStatement.builder().statementService(fireboltStatementService)
				.sessionProperties(fireboltProperties).connection(connection).build();

		fireboltStatement.execute("set custom_1 = 1");
		verifyNoMoreInteractions(fireboltStatementService);
		verify(connection).addProperty(new ImmutablePair<>("custom_1", "1"));
	}

	@SneakyThrows
	@Test
	void shouldAbortStatementOnCancel() {
		FireboltProperties fireboltProperties = FireboltProperties.builder().database("db")
				.additionalProperties(new HashMap<>()).build();

		FireboltStatement fireboltStatement = FireboltStatement.builder().statementService(fireboltStatementService)
				.sessionProperties(fireboltProperties).connection(fireboltConnection).build();

		Field runningStatementField = FireboltStatement.class.getDeclaredField("runningStatementId");
		runningStatementField.setAccessible(true);
		runningStatementField.set(fireboltStatement, "1234");
		fireboltStatement.cancel();
		verify(fireboltStatementService).abortStatement(any(), eq(fireboltProperties));
	}

	@Test
	void shouldCloseInputStreamOnClose() throws SQLException {
		ResultSet rs = mock(ResultSet.class);
		FireboltProperties fireboltProperties = FireboltProperties.builder().additionalProperties(new HashMap<>())
				.build();
		FireboltConnection connection = mock(FireboltConnection.class);
		FireboltStatement fireboltStatement = FireboltStatement.builder().statementService(fireboltStatementService)
				.sessionProperties(fireboltProperties).connection(connection).build();

		when(fireboltStatementService.execute(any(), any(), anyInt(), anyInt(), anyBoolean(), anyBoolean(), any()))
				.thenReturn(Optional.empty());
		when(fireboltStatementService.execute(any(), any(), anyInt(), anyInt(), anyBoolean(), anyBoolean(), any()))
				.thenReturn(Optional.of(rs));
		fireboltStatement.executeQuery("show database");
		fireboltStatement.close();
		verify(rs).close();
		verify(connection).removeClosedStatement(fireboltStatement);

		// validate that recurrent close does not create cascading closing calls.
		fireboltStatement.close();
		verifyNoMoreInteractions(connection);
	}

	@Test
	void shouldThrowAnExceptionWhenExecutingQueryOnANonQueryStatement() {
		FireboltConnection connection = mock(FireboltConnection.class);
		FireboltProperties fireboltProperties = FireboltProperties.builder().additionalProperties(new HashMap<>())
				.build();

		FireboltStatement fireboltStatement = FireboltStatement.builder().statementService(fireboltStatementService)
				.sessionProperties(fireboltProperties).connection(connection).build();

		assertThrows(FireboltException.class, () -> fireboltStatement.executeQuery("set custom_1 = 1"));
	}

	@Test
	void shouldExecuteIfUpdateStatementWouldNotReturnAResultSet() throws SQLException {
		FireboltProperties fireboltProperties = FireboltProperties.builder().additionalProperties(new HashMap<>())
				.build();

		try (FireboltStatement fireboltStatement = FireboltStatement.builder()
				.statementService(fireboltStatementService).connection(fireboltConnection)
				.sessionProperties(fireboltProperties).build()) {
			when(fireboltStatementService.execute(any(), any(), anyInt(), anyInt(), anyBoolean(), anyBoolean(), any()))
					.thenReturn(Optional.empty());
			assertEquals(0, fireboltStatement.executeUpdate("INSERT INTO cars(sales, name) VALUES (500, 'Ford')"));
			verify(fireboltStatementService).execute(queryInfoWrapperArgumentCaptor.capture(), eq(fireboltProperties),
					anyInt(), anyInt(), anyBoolean(), anyBoolean(), any());
			assertEquals("INSERT INTO cars(sales, name) VALUES (500, 'Ford')",
					queryInfoWrapperArgumentCaptor.getValue().getSql());
			assertEquals(0, fireboltStatement.getUpdateCount());
		}

	}

	@Test
	void shouldCloseCurrentAndGetMoreResultsForMultiStatementQuery() throws SQLException {
		ResultSet rs = mock(FireboltResultSet.class);
		ResultSet rs2 = mock(FireboltResultSet.class);
		FireboltConnection connection = mock(FireboltConnection.class);
		FireboltProperties fireboltProperties = FireboltProperties.builder().additionalProperties(new HashMap<>())
				.build();
		FireboltStatement fireboltStatement = FireboltStatement.builder().statementService(fireboltStatementService)
				.sessionProperties(fireboltProperties).connection(connection).build();

		when(fireboltStatementService.execute(any(), any(), anyInt(), anyInt(), anyBoolean(), anyBoolean(), any()))
				.thenReturn(Optional.of(rs)).thenReturn(Optional.of(rs2));
		fireboltStatement.execute("SELECT 1; SELECT 2;");
		assertEquals(rs, fireboltStatement.getResultSet());
		assertEquals(-1, fireboltStatement.getUpdateCount());
		fireboltStatement.getMoreResults();
		verify(rs).close();
		assertEquals(rs2, fireboltStatement.getResultSet());
		rs = fireboltStatement.getResultSet();
		fireboltStatement.getMoreResults();
		verify(rs).close();
		assertEquals(-1, fireboltStatement.getUpdateCount());
		assertNull(fireboltStatement.getResultSet());
	}

	@Test
	void shouldCloseCurrentAndGetMoreResultWhenCallingGetMoreResultsWithCloseCurrentFlag() throws SQLException {
		FireboltConnection connection = mock(FireboltConnection.class);
		FireboltProperties fireboltProperties = FireboltProperties.builder().additionalProperties(new HashMap<>())
				.build();
		FireboltStatement fireboltStatement = FireboltStatement.builder().statementService(fireboltStatementService)
				.sessionProperties(fireboltProperties).connection(connection).build();
		when(fireboltStatementService.execute(any(), any(), anyInt(), anyInt(), anyBoolean(), anyBoolean(), any()))
				.thenReturn(Optional.of(mock(FireboltResultSet.class)));
		fireboltStatement.execute("SELECT 1; SELECT 2;");
		ResultSet resultSet = fireboltStatement.getResultSet();
		fireboltStatement.getMoreResults(CLOSE_CURRENT_RESULT);
		verify(resultSet).close();
	}

	@Test
	void shouldKeepCurrentAndGetMoreResultWhenCallingGetMoreResultsWithKeepCurrentResultFlag() throws SQLException {
		FireboltConnection connection = mock(FireboltConnection.class);
		FireboltProperties fireboltProperties = FireboltProperties.builder().additionalProperties(new HashMap<>())
				.build();
		FireboltStatement fireboltStatement = FireboltStatement.builder().statementService(fireboltStatementService)
				.sessionProperties(fireboltProperties).connection(connection).build();
		when(fireboltStatementService.execute(any(), any(), anyInt(), anyInt(), anyBoolean(), anyBoolean(), any()))
				.thenReturn(Optional.of(mock(ResultSet.class)));

		fireboltStatement.execute("SELECT 1; SELECT 2;");
		ResultSet resultSet = fireboltStatement.getResultSet();
		fireboltStatement.getMoreResults(Statement.KEEP_CURRENT_RESULT);
		verify(resultSet, never()).close();
	}

	@Test
	void shouldCloseUnclosedAndGetMoreResultWhenCallingGetMoreResultsWithCloseAllResultFlag() throws SQLException {
		ResultSet rs = mock(FireboltResultSet.class);
		ResultSet rs2 = mock(FireboltResultSet.class);
		ResultSet rs3 = mock(FireboltResultSet.class);
		FireboltConnection connection = mock(FireboltConnection.class);
		FireboltProperties fireboltProperties = FireboltProperties.builder().additionalProperties(new HashMap<>())
				.build();
		FireboltStatement fireboltStatement = FireboltStatement.builder().statementService(fireboltStatementService)
				.sessionProperties(fireboltProperties).connection(connection).build();

		when(fireboltStatementService.execute(any(), any(), anyInt(), anyInt(), anyBoolean(), anyBoolean(), any()))
				.thenReturn(Optional.of(rs)).thenReturn(Optional.of(rs2)).thenReturn(Optional.of(rs3));

		fireboltStatement.execute("SELECT 1; SELECT 2; SELECT 3;");
		ResultSet firstRs = fireboltStatement.getResultSet();
		fireboltStatement.getMoreResults(Statement.KEEP_CURRENT_RESULT);
		verify(firstRs, never()).close();
		ResultSet secondRs = fireboltStatement.getResultSet();
		fireboltStatement.getMoreResults(Statement.KEEP_CURRENT_RESULT);
		verify(secondRs, never()).close();
		fireboltStatement.getMoreResults(Statement.CLOSE_ALL_RESULTS);
		verify(firstRs).close();
		verify(secondRs).close();

	}

	@Test
	void maxRows() throws SQLException {
		try (Statement statement  = new FireboltStatement(fireboltStatementService, null, mock(FireboltConnection.class))) {
			assertEquals(0, statement.getMaxRows()); // zero means there is no limit
			statement.setMaxRows(123);
			assertEquals(123, statement.getMaxRows());
		}
	}

	@Test
	void negativeMaxRows() throws SQLException {
		try (Statement statement  = new FireboltStatement(fireboltStatementService, null, mock(FireboltConnection.class))) {
			assertThrows(SQLException.class, () -> statement.setMaxRows(-1));
		}
	}

	@Test
	void statementParameters() throws SQLException {
		try (Statement statement  = new FireboltStatement(fireboltStatementService, null, mock(FireboltConnection.class))) {
			assertEquals(ResultSet.FETCH_FORWARD, statement.getFetchDirection());
			assertEquals(ResultSet.CONCUR_READ_ONLY, statement.getResultSetConcurrency());
			assertEquals(ResultSet.TYPE_FORWARD_ONLY, statement.getResultSetType());
			assertEquals(ResultSet.HOLD_CURSORS_OVER_COMMIT, statement.getResultSetHoldability());
			assertNull(statement.getWarnings());
		}
	}

	@ParameterizedTest
	@ValueSource(classes = {Statement.class, FireboltStatement.class, Wrapper.class, AutoCloseable.class})
	void successfulUnwrap(Class<?> clazz) throws SQLException {
		Statement statement  = new FireboltStatement(fireboltStatementService, null, mock(FireboltConnection.class));
		assertSame(statement, statement.unwrap(clazz));
	}

	@ParameterizedTest
	@ValueSource(classes = {Connection.class, String.class, Closeable.class})
	void failingUnwrap(Class<?> clazz) {
		Statement statement  = new FireboltStatement(fireboltStatementService, null, mock(FireboltConnection.class));
		assertThrows(SQLException.class, () -> statement.unwrap(clazz));
	}

	@ParameterizedTest
	@ValueSource(classes = {Statement.class, FireboltStatement.class, Wrapper.class, AutoCloseable.class})
	void isWrapperFor(Class<?> clazz) throws SQLException {
		Statement statement  = new FireboltStatement(fireboltStatementService, null, mock(FireboltConnection.class));
		assertTrue(statement.isWrapperFor(clazz));
	}

	@ParameterizedTest
	@ValueSource(classes = {Connection.class, String.class, Closeable.class})
	void isNotWrapperFor(Class<?> clazz) throws SQLException {
		Statement statement  = new FireboltStatement(fireboltStatementService, null, mock(FireboltConnection.class));
		assertFalse(statement.isWrapperFor(clazz));
	}

	@Test
	void queryTimeout() throws SQLException {
		Statement statement  = new FireboltStatement(fireboltStatementService, null, mock(FireboltConnection.class));
		assertEquals(0, statement.getQueryTimeout());
		statement.setQueryTimeout(12345);
		assertEquals(12345, statement.getQueryTimeout());
	}

	@Test
	void closeOnCompletion() throws SQLException {
		Statement statement  = new FireboltStatement(fireboltStatementService, null, mock(FireboltConnection.class));
		assertFalse(statement.isCloseOnCompletion());
		statement.closeOnCompletion();
		assertTrue(statement.isCloseOnCompletion());
	}

	@Test
	void isPoolable() throws SQLException {
		Statement statement  = new FireboltStatement(fireboltStatementService, null, mock(FireboltConnection.class));
		assertFalse(statement.isPoolable());
		assertThrows(SQLFeatureNotSupportedException.class, () -> statement.setPoolable(true));
		assertFalse(statement.isPoolable());
		assertThrows(SQLFeatureNotSupportedException.class, () -> statement.setPoolable(false));
		assertFalse(statement.isPoolable());
	}

	@Test
	void fetchSize() throws SQLException {
		Statement statement  = new FireboltStatement(fireboltStatementService, null, mock(FireboltConnection.class));
		assertEquals(0, statement.getFetchSize());
		statement.setFetchSize(123); // ignore
		assertEquals(0, statement.getFetchSize());
		assertThrows(SQLException.class, () -> statement.setFetchSize(-1));
	}

	@Test
	void getConnection() throws SQLException {
		FireboltConnection connection = mock(FireboltConnection.class);
		Statement statement  = new FireboltStatement(fireboltStatementService, null, connection);
		assertSame(connection, statement.getConnection());
	}
}
