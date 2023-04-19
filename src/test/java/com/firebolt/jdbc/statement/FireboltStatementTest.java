package com.firebolt.jdbc.statement;

import static java.sql.Statement.CLOSE_CURRENT_RESULT;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import java.lang.reflect.Field;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Optional;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.firebolt.jdbc.connection.FireboltConnection;
import com.firebolt.jdbc.connection.settings.FireboltProperties;
import com.firebolt.jdbc.exception.FireboltException;
import com.firebolt.jdbc.resultset.FireboltResultSet;
import com.firebolt.jdbc.service.FireboltStatementService;

import lombok.SneakyThrows;

@ExtendWith(MockitoExtension.class)
class FireboltStatementTest {

	@Captor
	ArgumentCaptor<FireboltProperties> fireboltPropertiesArgumentCaptor;
	@Captor
	ArgumentCaptor<StatementInfoWrapper> queryInfoWrapperArgumentCaptor;
	@Captor
	ArgumentCaptor<Boolean> booleanArgumentCaptor;
	@Mock
	private FireboltStatementService fireboltStatementService;
	@Mock
	private FireboltConnection fireboltConnection;

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

		when(fireboltStatementService.execute(any(), any(), anyInt(), anyInt(), anyBoolean(), any()))
				.thenReturn(Optional.empty());
		when(fireboltStatementService.execute(any(), any(), anyInt(), anyInt(), anyBoolean(), any()))
				.thenReturn(Optional.of(rs));
		fireboltStatement.executeQuery("show database");
		fireboltStatement.close();
		verify(rs).close();
		verify(connection).removeClosedStatement(fireboltStatement);
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
			when(fireboltStatementService.execute(any(), any(), anyInt(), anyInt(), anyBoolean(), any()))
					.thenReturn(Optional.empty());
			assertEquals(0, fireboltStatement.executeUpdate("INSERT INTO cars(sales, name) VALUES (500, 'Ford')"));
			verify(fireboltStatementService).execute(queryInfoWrapperArgumentCaptor.capture(), eq(fireboltProperties),
					anyInt(), anyInt(), anyBoolean(), any());
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

		when(fireboltStatementService.execute(any(), any(), anyInt(), anyInt(), anyBoolean(), any()))
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
		when(fireboltStatementService.execute(any(), any(), anyInt(), anyInt(), anyBoolean(), any()))
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
		when(fireboltStatementService.execute(any(), any(), anyInt(), anyInt(), anyBoolean(), any()))
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

		when(fireboltStatementService.execute(any(), any(), anyInt(), anyInt(), anyBoolean(), any()))
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
}
