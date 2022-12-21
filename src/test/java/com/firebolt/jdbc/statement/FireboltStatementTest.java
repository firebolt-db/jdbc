package com.firebolt.jdbc.statement;

import static java.sql.Statement.CLOSE_CURRENT_RESULT;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.*;
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
	void shouldExecuteQueryAndCreateResultSet() throws SQLException {
		try (MockedConstruction<FireboltResultSet> mocked = Mockito.mockConstruction(FireboltResultSet.class)) {
			FireboltProperties fireboltProperties = FireboltProperties.builder().additionalProperties(new HashMap<>())
					.build();
			FireboltStatement fireboltStatement = FireboltStatement.builder().statementService(fireboltStatementService)
					.sessionProperties(fireboltProperties).build();

			when(fireboltStatementService.execute(any(), any(), anyInt(), anyInt(), anyBoolean()))
					.thenReturn(mock(InputStream.class));
			fireboltStatement.executeQuery("show database");
			assertTrue(fireboltProperties.getAdditionalProperties().isEmpty());
			verify(fireboltStatementService).execute(queryInfoWrapperArgumentCaptor.capture(), eq(fireboltProperties),
					anyInt(), anyInt(), anyBoolean());
			assertEquals(1, mocked.constructed().size());
			assertEquals(-1, fireboltStatement.getUpdateCount());
			assertEquals("show database", queryInfoWrapperArgumentCaptor.getValue().getSql());
			assertEquals(StatementType.QUERY, queryInfoWrapperArgumentCaptor.getValue().getType());
		}
	}

	@Test
	void shouldExtractAdditionalProperties() throws SQLException {
		try (MockedConstruction<FireboltResultSet> mockedResultSet = Mockito
				.mockConstruction(FireboltResultSet.class)) {
			FireboltConnection connection = mock(FireboltConnection.class);
			FireboltProperties fireboltProperties = FireboltProperties.builder().additionalProperties(new HashMap<>())
					.build();

			FireboltStatement fireboltStatement = FireboltStatement.builder().statementService(fireboltStatementService)
					.sessionProperties(fireboltProperties).connection(connection).build();

			fireboltStatement.execute("set custom_1 = 1");
			verifyNoMoreInteractions(fireboltStatementService);
			verify(connection).addProperty(new ImmutablePair<>("custom_1", "1"));
			assertEquals(0, mockedResultSet.constructed().size());
		}
	}

	@SneakyThrows
	@Test
	void shouldCancelByQueryWhenAggressiveCancelIsEnabled() throws SQLException, IOException, NoSuchFieldException {
		try (MockedConstruction<FireboltResultSet> mockedResultSet = Mockito
				.mockConstruction(FireboltResultSet.class)) {

			FireboltProperties fireboltProperties = FireboltProperties.builder().database("db").aggressiveCancel(true)
					.additionalProperties(new HashMap<>()).build();

			FireboltStatement fireboltStatement = FireboltStatement.builder().statementService(fireboltStatementService)
					.sessionProperties(fireboltProperties).connection(fireboltConnection).build();

			Field runningStatementField = FireboltStatement.class.getDeclaredField("runningStatementId");
			runningStatementField.setAccessible(true);
			runningStatementField.set(fireboltStatement, "1234");
			fireboltStatement.cancel();
			verify(fireboltStatementService).execute(queryInfoWrapperArgumentCaptor.capture(),
					fireboltPropertiesArgumentCaptor.capture(), anyInt(), anyInt(), booleanArgumentCaptor.capture());
			assertEquals("KILL QUERY ON CLUSTER sql_cluster WHERE initial_query_id='1234'",
					queryInfoWrapperArgumentCaptor.getValue().getSql());
			assertFalse(booleanArgumentCaptor.getValue());
		}
	}

	@SneakyThrows
	@Test
	void shouldCancelByApiCallWhenAggressiveCancelIsDisabled() throws SQLException, IOException {
		try (MockedConstruction<FireboltResultSet> mockedResultSet = Mockito
				.mockConstruction(FireboltResultSet.class)) {

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
	}

	@Test
	void shouldCloseInputStreamOnClose() throws SQLException, IOException {
		try (MockedConstruction<FireboltResultSet> mockedResultSet = Mockito
				.mockConstruction(FireboltResultSet.class)) {
			FireboltProperties fireboltProperties = FireboltProperties.builder().additionalProperties(new HashMap<>())
					.build();
			FireboltConnection connection = mock(FireboltConnection.class);
			FireboltStatement fireboltStatement = FireboltStatement.builder().statementService(fireboltStatementService)
					.sessionProperties(fireboltProperties).connection(connection).build();

			when(fireboltStatementService.execute(any(), any(), anyInt(), anyInt(), anyBoolean()))
					.thenReturn(mock(InputStream.class));

			fireboltStatement.executeQuery("show database");
			fireboltStatement.close();
			verify(mockedResultSet.constructed().get(0)).close();
			verify(connection).removeClosedStatement(fireboltStatement);
		}
	}

	@Test
	void shouldThrowAnExceptionWhenExecutingQueryOnANonQueryStatement() {
		try (MockedConstruction<FireboltResultSet> mockedResultSet = Mockito
				.mockConstruction(FireboltResultSet.class)) {
			FireboltConnection connection = mock(FireboltConnection.class);
			FireboltProperties fireboltProperties = FireboltProperties.builder().additionalProperties(new HashMap<>())
					.build();

			FireboltStatement fireboltStatement = FireboltStatement.builder().statementService(fireboltStatementService)
					.sessionProperties(fireboltProperties).connection(connection).build();

			assertThrows(FireboltException.class, () -> fireboltStatement.executeQuery("set custom_1 = 1"));
		}
	}

	@Test
	void shouldExecuteIfUpdateStatementWouldNotReturnAResultSet() throws SQLException {
		FireboltProperties fireboltProperties = FireboltProperties.builder().additionalProperties(new HashMap<>())
				.build();

		try (FireboltStatement fireboltStatement = FireboltStatement.builder()
				.statementService(fireboltStatementService).connection(fireboltConnection)
				.sessionProperties(fireboltProperties).build()) {
			when(fireboltStatementService.execute(any(), any(), anyInt(), anyInt(), anyBoolean()))
					.thenReturn(mock(InputStream.class));
			assertEquals(0, fireboltStatement.executeUpdate("INSERT INTO cars(sales, name) VALUES (500, 'Ford')"));
			verify(fireboltStatementService).execute(queryInfoWrapperArgumentCaptor.capture(), eq(fireboltProperties),
					anyInt(), anyInt(), anyBoolean());
			assertEquals("INSERT INTO cars(sales, name) VALUES (500, 'Ford')",
					queryInfoWrapperArgumentCaptor.getValue().getSql());
		}

	}

	@Test
	void shouldCloseCurrentAndGetMoreResultsForMultiStatementQuery() throws SQLException {
		try (MockedConstruction<FireboltResultSet> mockedResultSet = Mockito
				.mockConstruction(FireboltResultSet.class)) {
			FireboltConnection connection = mock(FireboltConnection.class);
			FireboltProperties fireboltProperties = FireboltProperties.builder().additionalProperties(new HashMap<>())
					.build();
			FireboltStatement fireboltStatement = FireboltStatement.builder().statementService(fireboltStatementService)
					.sessionProperties(fireboltProperties).connection(connection).build();
			fireboltStatement.execute("SELECT 1; SELECT 2;");
			ResultSet rs = fireboltStatement.getResultSet();
			assertEquals(mockedResultSet.constructed().get(0), fireboltStatement.getResultSet());
			fireboltStatement.getMoreResults();
			verify(rs).close();
			assertEquals(mockedResultSet.constructed().get(1), fireboltStatement.getResultSet());
			rs = fireboltStatement.getResultSet();
			fireboltStatement.getMoreResults();
			verify(rs).close();
			assertNull(fireboltStatement.getResultSet());
		}
	}

	@Test
	void shouldCloseCurrentAndGetMoreResultWhenCallingGetMoreResultsWithCloseCurrentFlag() throws SQLException {
		try (MockedConstruction<FireboltResultSet> mockedResultSet = Mockito
				.mockConstruction(FireboltResultSet.class)) {
			FireboltConnection connection = mock(FireboltConnection.class);
			FireboltProperties fireboltProperties = FireboltProperties.builder().additionalProperties(new HashMap<>())
					.build();
			FireboltStatement fireboltStatement = FireboltStatement.builder().statementService(fireboltStatementService)
					.sessionProperties(fireboltProperties).connection(connection).build();
			fireboltStatement.execute("SELECT 1; SELECT 2;");
			ResultSet resultSet = fireboltStatement.getResultSet();
			fireboltStatement.getMoreResults(CLOSE_CURRENT_RESULT);
			verify(resultSet).close();
		}
	}

	@Test
	void shouldKeepCurrentAndGetMoreResultWhenCallingGetMoreResultsWithKeepCurrentResultFlag() throws SQLException {
		try (MockedConstruction<FireboltResultSet> mockedResultSet = Mockito
				.mockConstruction(FireboltResultSet.class)) {
			FireboltConnection connection = mock(FireboltConnection.class);
			FireboltProperties fireboltProperties = FireboltProperties.builder().additionalProperties(new HashMap<>())
					.build();
			FireboltStatement fireboltStatement = FireboltStatement.builder().statementService(fireboltStatementService)
					.sessionProperties(fireboltProperties).connection(connection).build();
			fireboltStatement.execute("SELECT 1; SELECT 2;");
			ResultSet resultSet = fireboltStatement.getResultSet();
			fireboltStatement.getMoreResults(Statement.KEEP_CURRENT_RESULT);
			verify(resultSet, never()).close();
		}
	}

	@Test
	void shouldCloseUnclosedAndGetMoreResultWhenCallingGetMoreResultsWithCloseAllResultFlag() throws SQLException {
		try (MockedConstruction<FireboltResultSet> mockedResultSet = Mockito
				.mockConstruction(FireboltResultSet.class)) {
			FireboltConnection connection = mock(FireboltConnection.class);
			FireboltProperties fireboltProperties = FireboltProperties.builder().additionalProperties(new HashMap<>())
					.build();
			FireboltStatement fireboltStatement = FireboltStatement.builder().statementService(fireboltStatementService)
					.sessionProperties(fireboltProperties).connection(connection).build();
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
}
