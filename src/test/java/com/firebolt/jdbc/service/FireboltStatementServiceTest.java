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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.SQLException;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class FireboltStatementServiceTest {
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
			when(statement.getQueryTimeout()).thenReturn(10);
			fireboltStatementService.execute(statementInfoWrapper, fireboltProperties, true, statement);
			verify(statementClient).executeSqlStatement(statementInfoWrapper, fireboltProperties, false, 10, true);
			Assertions.assertEquals(1, mocked.constructed().size());
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
			fireboltStatementService.execute(statementInfoWrapper, fireboltProperties, true, statement);
			Assertions.assertEquals(1, mocked.constructed().size());
			verify(statementClient).executeSqlStatement(statementInfoWrapper, fireboltProperties, false, -1, true);
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
			when(statement.getQueryTimeout()).thenReturn(10);
			fireboltStatementService.execute(statementInfoWrapper, fireboltProperties, true, statement);
			Assertions.assertEquals(1, mocked.constructed().size());
			verify(statementClient).executeSqlStatement(statementInfoWrapper, fireboltProperties, true, 10, true);
		}
	}

	@Test
	void shouldIncludeNonStandardSqlQueryParamForNonStandardSql() throws SQLException {
		try (MockedConstruction<FireboltResultSet> mocked = Mockito.mockConstruction(FireboltResultSet.class)) {

			StatementInfoWrapper statementInfoWrapper = StatementUtil.parseToStatementInfoWrappers("SELECT 1").get(0);
			FireboltProperties fireboltProperties = fireboltProperties("localhost", true);

			FireboltStatementService fireboltStatementService = new FireboltStatementService(statementClient);
			FireboltStatement statement = mock(FireboltStatement.class);
			when(statement.getQueryTimeout()).thenReturn(-1);
			fireboltStatementService.execute(statementInfoWrapper, fireboltProperties, false, statement);
			Assertions.assertEquals(1, mocked.constructed().size());
			verify(statementClient).executeSqlStatement(statementInfoWrapper, fireboltProperties, true, -1, false);
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
		Assertions.assertEquals(Optional.empty(), fireboltStatementService.execute(statementInfoWrapper,
				fireboltProperties, true, statement));
		verify(statementClient).executeSqlStatement(statementInfoWrapper, fireboltProperties, false, -1, true);
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
		Assertions.assertEquals(running, fireboltStatementService.isStatementRunning("id"));
	}

	private FireboltProperties fireboltProperties(String host, boolean systemEngine) {
		return FireboltProperties.builder().database("db").host(host).ssl(true).compress(false).systemEngine(systemEngine).build();
	}
}
