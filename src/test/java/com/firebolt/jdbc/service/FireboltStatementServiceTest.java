package com.firebolt.jdbc.service;

import com.firebolt.jdbc.client.query.StatementClient;
import com.firebolt.jdbc.client.query.StatementClientImpl;
import com.firebolt.jdbc.connection.FireboltConnection;
import com.firebolt.jdbc.connection.settings.FireboltProperties;
import com.firebolt.jdbc.exception.FireboltException;
import com.firebolt.jdbc.resultset.FireboltResultSet;
import com.firebolt.jdbc.statement.FireboltStatement;
import com.firebolt.jdbc.statement.StatementInfoWrapper;
import com.firebolt.jdbc.statement.StatementUtil;
import okhttp3.OkHttpClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Optional;

import static java.util.Optional.empty;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class FireboltStatementServiceTest {
	private static final int QUERY_TIMEOUT_SECONDS = 10;
	private static final boolean IS_ASYNC = true;
	private static final boolean IS_SYNC = false;
	private final FireboltProperties emptyFireboltProperties = FireboltProperties.builder().build();

	@Mock
	private StatementClient statementClient;

	@Test
	void shouldExecuteQueryAndCreateResultSet() throws SQLException {
		try (MockedConstruction<FireboltResultSet> mocked = Mockito.mockConstruction(FireboltResultSet.class)) {

			StatementInfoWrapper statementInfoWrapper = StatementUtil.parseToStatementInfoWrappers("SELECT 1").get(0);
			FireboltProperties fireboltProperties = fireboltProperties("firebolt1", false);
			FireboltStatementService fireboltStatementService = new FireboltStatementService(statementClient);
			FireboltStatement statement = mock(FireboltStatement.class);
			when(statement.getQueryTimeout()).thenReturn(QUERY_TIMEOUT_SECONDS);
			fireboltStatementService.execute(statementInfoWrapper, fireboltProperties, statement);
			verify(statementClient).executeSqlStatement(statementInfoWrapper, fireboltProperties, QUERY_TIMEOUT_SECONDS, IS_SYNC);
			assertEquals(1, mocked.constructed().size());
		}
	}

	@Test
	void shouldExecuteQueryWithLocalHostFormatParameters() throws SQLException {
		try (MockedConstruction<FireboltResultSet> mocked = Mockito.mockConstruction(FireboltResultSet.class)) {
			StatementInfoWrapper statementInfoWrapper = StatementUtil.parseToStatementInfoWrappers("SELECT 1").get(0);
			FireboltProperties fireboltProperties = fireboltProperties("localhost", false);
			FireboltStatementService fireboltStatementService = new FireboltStatementService(statementClient);
			FireboltStatement statement = mock(FireboltStatement.class);
			when(statement.getQueryTimeout()).thenReturn(-1);
			fireboltStatementService.execute(statementInfoWrapper, fireboltProperties, statement);
			assertEquals(1, mocked.constructed().size());
			verify(statementClient).executeSqlStatement(statementInfoWrapper, fireboltProperties, -1, IS_SYNC);
		}
	}

	@Test
	void shouldCancelQueryWithAllRequiredParams() throws SQLException {
		FireboltProperties fireboltProperties = fireboltProperties("firebolt1", false);
		FireboltStatementService fireboltStatementService = new FireboltStatementService(statementClient);
		fireboltStatementService.abortStatement("123", fireboltProperties);
		verify(statementClient).abortStatement("123", fireboltProperties);
	}

	@Test
	void shouldThrowExceptionWhenTryingToCancelQueryWithASystemEngine() {
		FireboltProperties fireboltProperties = fireboltProperties("firebolt1", true);
		StatementClient client = new StatementClientImpl(new OkHttpClient(), Mockito.mock(FireboltConnection.class), null, null);
		FireboltStatementService fireboltStatementService = new FireboltStatementService(client);
		assertThrows(FireboltException.class, () -> fireboltStatementService.abortStatement("123", fireboltProperties));
		verifyNoInteractions(statementClient);
	}

	@Test
	void shouldExecuteQueryWithParametersForSystemEngine() throws SQLException {
		try (MockedConstruction<FireboltResultSet> mocked = Mockito.mockConstruction(FireboltResultSet.class)) {
			StatementInfoWrapper statementInfoWrapper = StatementUtil.parseToStatementInfoWrappers("SELECT 1").get(0);
			FireboltProperties fireboltProperties = fireboltProperties("firebolt1", true);
			FireboltStatementService fireboltStatementService = new FireboltStatementService(statementClient);
			FireboltStatement statement = mock(FireboltStatement.class);
			when(statement.getQueryTimeout()).thenReturn(QUERY_TIMEOUT_SECONDS);
			fireboltStatementService.execute(statementInfoWrapper, fireboltProperties, statement);
			assertEquals(1, mocked.constructed().size());
			verify(statementClient).executeSqlStatement(statementInfoWrapper, fireboltProperties, QUERY_TIMEOUT_SECONDS, IS_SYNC);
		}
	}

	@Test
	void shouldBeEmptyWithNonQueryStatement() throws SQLException {
		StatementInfoWrapper statementInfoWrapper = StatementUtil
				.parseToStatementInfoWrappers("INSERT INTO ltv SELECT * FROM ltv_external").get(0);
		FireboltProperties fireboltProperties = fireboltProperties("localhost", false);

		FireboltStatementService fireboltStatementService = new FireboltStatementService(statementClient);
		FireboltStatement statement = mock(FireboltStatement.class);
		when(statement.getQueryTimeout()).thenReturn(-1);
		assertEquals(empty(), fireboltStatementService.execute(statementInfoWrapper, fireboltProperties, statement));
		verify(statementClient).executeSqlStatement(statementInfoWrapper, fireboltProperties, -1, IS_SYNC);
	}

	@Test
	void shouldExecuteQueryAsync() throws SQLException {
		StatementInfoWrapper statementInfoWrapper = StatementUtil
				.parseToStatementInfoWrappers("INSERT INTO ltv SELECT * FROM ltv_external").get(0);
		FireboltProperties fireboltProperties = fireboltProperties("localhost", false);

		FireboltStatementService fireboltStatementService = new FireboltStatementService(statementClient);
		FireboltStatement statement = mock(FireboltStatement.class);
		when(statement.getQueryTimeout()).thenReturn(QUERY_TIMEOUT_SECONDS);
		String jsonResult = "{\"token\":\"123\"}";
		when(statementClient.executeSqlStatement(statementInfoWrapper, fireboltProperties, QUERY_TIMEOUT_SECONDS, IS_ASYNC)).thenReturn(getMockInputStream(jsonResult));
		String asyncToken = fireboltStatementService.executeAsyncStatement(statementInfoWrapper, fireboltProperties, statement);
		assertEquals("123", asyncToken);
		verify(statementClient).executeSqlStatement(statementInfoWrapper, fireboltProperties, QUERY_TIMEOUT_SECONDS, IS_ASYNC);
	}

	@Test
	void shouldThrowExceptionWhenResponseIsEmpty() throws SQLException {
		StatementInfoWrapper statementInfoWrapper = StatementUtil
				.parseToStatementInfoWrappers("INSERT INTO ltv SELECT * FROM ltv_external").get(0);
		FireboltProperties fireboltProperties = fireboltProperties("localhost", false);

		FireboltStatementService fireboltStatementService = new FireboltStatementService(statementClient);
		FireboltStatement statement = mock(FireboltStatement.class);
		when(statement.getQueryTimeout()).thenReturn(QUERY_TIMEOUT_SECONDS);
		when(statementClient.executeSqlStatement(statementInfoWrapper, fireboltProperties, QUERY_TIMEOUT_SECONDS, IS_ASYNC)).thenReturn(getMockInputStream(""));
		FireboltException exception = assertThrows(FireboltException.class, () -> fireboltStatementService.executeAsyncStatement(statementInfoWrapper, fireboltProperties, statement));
		assertTrue(exception.getMessage().contains("Cannot read response from DB"));
		verify(statementClient).executeSqlStatement(statementInfoWrapper, fireboltProperties, QUERY_TIMEOUT_SECONDS, IS_ASYNC);
	}

	@Test
	void shouldThrowExceptionWhenResponseIsNull() throws SQLException {
		StatementInfoWrapper statementInfoWrapper = StatementUtil
				.parseToStatementInfoWrappers("INSERT INTO ltv SELECT * FROM ltv_external").get(0);
		FireboltProperties fireboltProperties = fireboltProperties("localhost", false);

		FireboltStatementService fireboltStatementService = new FireboltStatementService(statementClient);
		FireboltStatement statement = mock(FireboltStatement.class);
		when(statement.getQueryTimeout()).thenReturn(QUERY_TIMEOUT_SECONDS);

		//InputStream should never be null
		when(statementClient.executeSqlStatement(statementInfoWrapper, fireboltProperties, QUERY_TIMEOUT_SECONDS, IS_ASYNC)).thenReturn(null);
		FireboltException exception = assertThrows(FireboltException.class, () -> fireboltStatementService.executeAsyncStatement(statementInfoWrapper, fireboltProperties, statement));

		//Exception message when unexpected exception happens: InputStream is null
		assertTrue(exception.getMessage().contains("Error while reading query response"));
		verify(statementClient).executeSqlStatement(statementInfoWrapper, fireboltProperties, QUERY_TIMEOUT_SECONDS, IS_ASYNC);
	}

	@Test
	void abortStatementHttpRequest() throws SQLException {
		FireboltStatementService fireboltStatementService = new FireboltStatementService(statementClient);
		fireboltStatementService.abortStatement("id", emptyFireboltProperties);
		verify(statementClient).abortStatement("id", emptyFireboltProperties);
	}

	@ParameterizedTest
	@ValueSource(booleans = {true, false})
	void isStatementRunning(boolean running) {
		FireboltStatementService fireboltStatementService = new FireboltStatementService(statementClient);
		when(statementClient.isStatementRunning("id")).thenReturn(running);
		assertEquals(running, fireboltStatementService.isStatementRunning("id"));
	}

	@Test
	void shouldExecuteQueryWithFilesAndCreateResultSet() throws SQLException {
		StatementInfoWrapper statementInfoWrapper = StatementUtil.parseToStatementInfoWrappers("SELECT 1 FROM read_parquet('upload://file1')").get(0);
		FireboltProperties fireboltProperties = fireboltProperties("firebolt1", false);
		FireboltStatementService fireboltStatementService = new FireboltStatementService(statementClient);
		FireboltStatement statement = mock(FireboltStatement.class);
		when(statement.getQueryTimeout()).thenReturn(QUERY_TIMEOUT_SECONDS);
		Map<String, byte[]> files = Map.of("file1", "test content".getBytes());
		when(statementClient.executeSqlStatementWithFiles(statementInfoWrapper, fireboltProperties, QUERY_TIMEOUT_SECONDS, IS_SYNC, files))
				.thenReturn(getMockInputStream("result"));
		Optional<ResultSet> result = fireboltStatementService.executeWithFiles(statementInfoWrapper, fireboltProperties, statement, files);
		assertTrue(result.isPresent());
        assertInstanceOf(FireboltResultSet.class, result.get());
		verify(statementClient).executeSqlStatementWithFiles(statementInfoWrapper, fireboltProperties, QUERY_TIMEOUT_SECONDS, IS_SYNC, files);
	}

	@Test
	void shouldExecuteNonQueryWithFilesAndReturnEmpty() throws SQLException {
		StatementInfoWrapper statementInfoWrapper = StatementUtil
				.parseToStatementInfoWrappers("INSERT INTO ltv SELECT * FROM read_parquet('upload://file1')").get(0);
		FireboltProperties fireboltProperties = fireboltProperties("localhost", false);
		FireboltStatementService fireboltStatementService = new FireboltStatementService(statementClient);
		FireboltStatement statement = mock(FireboltStatement.class);
		when(statement.getQueryTimeout()).thenReturn(-1);
		Map<String, byte[]> files = Map.of("file1", "test content".getBytes());
		when(statementClient.executeSqlStatementWithFiles(statementInfoWrapper, fireboltProperties, -1, IS_SYNC, files))
				.thenReturn(getMockInputStream("result"));
		assertEquals(empty(), fireboltStatementService.executeWithFiles(statementInfoWrapper, fireboltProperties, statement, files));
		verify(statementClient).executeSqlStatementWithFiles(statementInfoWrapper, fireboltProperties, -1, IS_SYNC, files);
	}

	@Test
	void shouldExecuteQueryWithFilesForSystemEngine() throws SQLException {
		try (MockedConstruction<FireboltResultSet> mocked = Mockito.mockConstruction(FireboltResultSet.class)) {
			StatementInfoWrapper statementInfoWrapper = StatementUtil.parseToStatementInfoWrappers("SELECT 1 FROM read_parquet('upload://file1')").get(0);
			FireboltProperties fireboltProperties = fireboltProperties("firebolt1", true);
			FireboltStatementService fireboltStatementService = new FireboltStatementService(statementClient);
			FireboltStatement statement = mock(FireboltStatement.class);
			when(statement.getQueryTimeout()).thenReturn(QUERY_TIMEOUT_SECONDS);
			Map<String, byte[]> files = Map.of("file1", "test content".getBytes());
			when(statementClient.executeSqlStatementWithFiles(statementInfoWrapper, fireboltProperties, QUERY_TIMEOUT_SECONDS, IS_SYNC, files))
					.thenReturn(getMockInputStream("result"));
			Optional<ResultSet> result = fireboltStatementService.executeWithFiles(statementInfoWrapper, fireboltProperties, statement, files);
			assertTrue(result.isPresent());
			verify(statementClient).executeSqlStatementWithFiles(statementInfoWrapper, fireboltProperties, QUERY_TIMEOUT_SECONDS, IS_SYNC, files);
			assertEquals(1, mocked.constructed().size());
		}
	}

	@Test
	void shouldExecuteAsyncStatementWithFiles() throws SQLException {
		StatementInfoWrapper statementInfoWrapper = StatementUtil
				.parseToStatementInfoWrappers("INSERT INTO ltv SELECT * FROM read_parquet('upload://file1')").get(0);
		FireboltProperties fireboltProperties = fireboltProperties("localhost", false);
		FireboltStatementService fireboltStatementService = new FireboltStatementService(statementClient);
		FireboltStatement statement = mock(FireboltStatement.class);
		when(statement.getQueryTimeout()).thenReturn(QUERY_TIMEOUT_SECONDS);
		Map<String, byte[]> files = Map.of("file1", "test content".getBytes());
		String jsonResult = "{\"token\":\"async-token-123\"}";
		when(statementClient.executeSqlStatementWithFiles(statementInfoWrapper, fireboltProperties, QUERY_TIMEOUT_SECONDS, IS_ASYNC, files))
				.thenReturn(getMockInputStream(jsonResult));
		String asyncToken = fireboltStatementService.executeAsyncStatementWithFiles(statementInfoWrapper, fireboltProperties, statement, files);
		assertEquals("async-token-123", asyncToken);
		verify(statementClient).executeSqlStatementWithFiles(statementInfoWrapper, fireboltProperties, QUERY_TIMEOUT_SECONDS, IS_ASYNC, files);
	}

	private FireboltProperties fireboltProperties(String host, boolean systemEngine) {
		return FireboltProperties.builder().database("db").host(host).ssl(true).compress(false).bufferSize(65536).systemEngine(systemEngine).build();
	}

	private InputStream getMockInputStream(String jsonResult) {
		return new ByteArrayInputStream(jsonResult.getBytes());
	}
}
