package com.firebolt.jdbc.service;

import java.sql.SQLException;
import java.util.Optional;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import com.firebolt.jdbc.client.query.StatementClient;
import com.firebolt.jdbc.connection.settings.FireboltProperties;
import com.firebolt.jdbc.exception.FireboltException;
import com.firebolt.jdbc.resultset.FireboltResultSet;
import com.firebolt.jdbc.statement.FireboltStatement;
import com.firebolt.jdbc.statement.StatementInfoWrapper;
import com.firebolt.jdbc.statement.StatementUtil;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class FireboltStatementServiceTest {

	@Mock
	private StatementClient statementClient;

	@Test
	void shouldExecuteQueryAndCreateResultSet() throws SQLException {
		try (MockedConstruction<FireboltResultSet> mocked = Mockito.mockConstruction(FireboltResultSet.class)) {

			StatementInfoWrapper statementInfoWrapper = StatementUtil.parseToStatementInfoWrappers("SELECT 1").get(0);
			FireboltProperties fireboltProperties = FireboltProperties.builder().database("db").host("firebolt1")
					.ssl(true).compress(false).build();
			FireboltStatementService fireboltStatementService = new FireboltStatementService(statementClient);

			fireboltStatementService.execute(statementInfoWrapper, fireboltProperties, 10, -1, true, false,
					mock(FireboltStatement.class));
			verify(statementClient).executeSqlStatement(statementInfoWrapper, fireboltProperties, false, 10, true);
			Assertions.assertEquals(1, mocked.constructed().size());
		}
	}

	@Test
	void shouldExecuteQueryWithLocalHostFormatParameters() throws SQLException {
		try (MockedConstruction<FireboltResultSet> mocked = Mockito.mockConstruction(FireboltResultSet.class)) {
			StatementInfoWrapper statementInfoWrapper = StatementUtil.parseToStatementInfoWrappers("SELECT 1").get(0);
			FireboltProperties fireboltProperties = FireboltProperties.builder().database("db").host("localhost")
					.ssl(true).compress(false).build();
			FireboltStatementService fireboltStatementService = new FireboltStatementService(statementClient);
			fireboltStatementService.execute(statementInfoWrapper, fireboltProperties, -1, 10, true, false,
					mock(FireboltStatement.class));
			Assertions.assertEquals(1, mocked.constructed().size());
			verify(statementClient).executeSqlStatement(statementInfoWrapper, fireboltProperties, false, -1, true);
		}
	}

	@Test
	void shouldCancelQueryWithAllRequiredParams() throws FireboltException {
		FireboltProperties fireboltProperties = FireboltProperties.builder().database("db").host("http://firebolt1")
				.ssl(true).compress(false).build();

		FireboltStatementService fireboltStatementService = new FireboltStatementService(statementClient);
		fireboltStatementService.abortStatement("123", fireboltProperties);
		verify(statementClient).abortStatement("123", fireboltProperties);
	}

	@Test
	void shouldThrowExceptionWhenTryingToCancelQueryWithASystemEngine() throws FireboltException {
		FireboltProperties fireboltProperties = FireboltProperties.builder().database("db").host("http://firebolt1")
				.ssl(true).compress(false).systemEngine(true).build();

		FireboltStatementService fireboltStatementService = new FireboltStatementService(statementClient);
		assertThrows(FireboltException.class, () -> fireboltStatementService.abortStatement("123", fireboltProperties));
		verifyNoInteractions(statementClient);
	}

	@Test
	void shouldExecuteQueryWithParametersForSystemEngine() throws SQLException {
		try (MockedConstruction<FireboltResultSet> mocked = Mockito.mockConstruction(FireboltResultSet.class)) {
			StatementInfoWrapper statementInfoWrapper = StatementUtil.parseToStatementInfoWrappers("SELECT 1").get(0);
			FireboltProperties fireboltProperties = FireboltProperties.builder().database("db").host("firebolt1")
					.ssl(true).compress(false).build();
			FireboltStatementService fireboltStatementService = new FireboltStatementService(statementClient);
			fireboltStatementService.execute(statementInfoWrapper, fireboltProperties, 10, 10, true, true,
					mock(FireboltStatement.class));
			Assertions.assertEquals(1, mocked.constructed().size());
			verify(statementClient).executeSqlStatement(statementInfoWrapper, fireboltProperties, true, 10, true);
		}
	}

	@Test
	void shouldIncludeNonStandardSqlQueryParamForNonStandardSql() throws SQLException {
		try (MockedConstruction<FireboltResultSet> mocked = Mockito.mockConstruction(FireboltResultSet.class)) {

			StatementInfoWrapper statementInfoWrapper = StatementUtil.parseToStatementInfoWrappers("SELECT 1").get(0);
			FireboltProperties fireboltProperties = FireboltProperties.builder().database("db").host("localhost")
					.ssl(true).compress(false).build();

			FireboltStatementService fireboltStatementService = new FireboltStatementService(statementClient);
			fireboltStatementService.execute(statementInfoWrapper, fireboltProperties, -1, 0, false, true,
					mock(FireboltStatement.class));
			Assertions.assertEquals(1, mocked.constructed().size());
			verify(statementClient).executeSqlStatement(statementInfoWrapper, fireboltProperties, true, -1, false);
		}
	}

	@Test
	void shouldBeEmptyWithNonQueryStatement() throws SQLException {
		StatementInfoWrapper statementInfoWrapper = StatementUtil
				.parseToStatementInfoWrappers("INSERT INTO ltv SELECT * FROM ltv_external").get(0);
		FireboltProperties fireboltProperties = FireboltProperties.builder().database("db").host("localhost").ssl(true)
				.compress(false).build();

		FireboltStatementService fireboltStatementService = new FireboltStatementService(statementClient);
		Assertions.assertEquals(Optional.empty(), fireboltStatementService.execute(statementInfoWrapper,
				fireboltProperties, -1, 10, true, false, mock(FireboltStatement.class)));
		verify(statementClient).executeSqlStatement(statementInfoWrapper, fireboltProperties, false, -1, true);
	}

	@Test
	void abortStatementHttpRequest() throws FireboltException {
		FireboltStatementService fireboltStatementService = new FireboltStatementService(statementClient);
		fireboltStatementService.abortStatementHttpRequest("id");
		verify(statementClient).abortRunningHttpRequest("id");
	}

	@ParameterizedTest
	@ValueSource(booleans = {true, false})
	void isStatementRunning(boolean running) {
		FireboltStatementService fireboltStatementService = new FireboltStatementService(statementClient);
		when(statementClient.isStatementRunning("id")).thenReturn(running);
		Assertions.assertEquals(running, fireboltStatementService.isStatementRunning("id"));
	}
}
