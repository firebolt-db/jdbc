package com.firebolt.jdbc.service;

import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.firebolt.jdbc.client.query.StatementClient;
import com.firebolt.jdbc.connection.settings.FireboltProperties;
import com.firebolt.jdbc.exception.FireboltException;
import com.firebolt.jdbc.statement.StatementInfoWrapper;
import com.firebolt.jdbc.statement.StatementUtil;

@ExtendWith(MockitoExtension.class)
class FireboltStatementServiceTest {

	@Mock
	private StatementClient statementClient;

	@Test
	void shouldExecuteQueryWithAllRequiredParameters() throws FireboltException {
		StatementInfoWrapper statementInfoWrapper = StatementUtil.parseToStatementInfoWrappers("SELECT 1").get(0);
		FireboltProperties fireboltProperties = FireboltProperties.builder().database("db").host("firebolt1").ssl(true)
				.compress(true).build();
		FireboltStatementService fireboltStatementService = new FireboltStatementService(statementClient, false);
		fireboltStatementService.execute(statementInfoWrapper, fireboltProperties, 10, -1, true);
		verify(statementClient).postSqlStatement(statementInfoWrapper, fireboltProperties,
				false, 10, -1, true);
	}

	@Test
	void shouldExecuteQueryWithLocalHostFormatParameters() throws FireboltException {
		StatementInfoWrapper statementInfoWrapper = StatementUtil.parseToStatementInfoWrappers("SELECT 1").get(0);
		FireboltProperties fireboltProperties = FireboltProperties.builder().database("db").host("localhost").ssl(true)
				.compress(true).build();

		FireboltStatementService fireboltStatementService = new FireboltStatementService(statementClient, false);
		fireboltStatementService.execute(statementInfoWrapper, fireboltProperties, -1, 10, true);
		verify(statementClient).postSqlStatement(statementInfoWrapper, fireboltProperties, false, -1, 10, true);

	}

	@Test
	void shouldCancelQueryWithAllRequiredParams() throws FireboltException {
		FireboltProperties fireboltProperties = FireboltProperties.builder().database("db").host("http://firebolt1")
				.ssl(true).compress(true).build();

		FireboltStatementService fireboltStatementService = new FireboltStatementService(statementClient, false);
		fireboltStatementService.abortStatement("123", fireboltProperties);
		verify(statementClient).abortStatement("123", fireboltProperties);
	}

	@Test
	void shouldThrowExceptionWhenTryingToCancelQueryWithASystemEngine() throws FireboltException {
		FireboltProperties fireboltProperties = FireboltProperties.builder().database("db").host("http://firebolt1")
				.ssl(true).compress(true).build();

		FireboltStatementService fireboltStatementService = new FireboltStatementService(statementClient, true);
		assertThrows(FireboltException.class, () -> fireboltStatementService.abortStatement("123", fireboltProperties));
		verifyNoInteractions(statementClient);
	}

	@Test
	void shouldExecuteQueryWithParametersForSystemEngine() throws FireboltException {
		StatementInfoWrapper statementInfoWrapper = StatementUtil.parseToStatementInfoWrappers("SELECT 1").get(0);
		FireboltProperties fireboltProperties = FireboltProperties.builder().database("db").host("firebolt1").ssl(true)
				.compress(true).build();
		FireboltStatementService fireboltStatementService = new FireboltStatementService(statementClient, true);
		fireboltStatementService.execute(statementInfoWrapper, fireboltProperties, 10, 10, true);
		verify(statementClient).postSqlStatement(statementInfoWrapper, fireboltProperties, true, 10, 10, true);
	}

	@Test
	void shouldIncludeNonStandardSqlQueryParamForNonStandardSql() throws FireboltException {
		StatementInfoWrapper statementInfoWrapper = StatementUtil.parseToStatementInfoWrappers("SELECT 1").get(0);
		FireboltProperties fireboltProperties = FireboltProperties.builder().database("db").host("localhost").ssl(true)
				.compress(true).build();

		FireboltStatementService fireboltStatementService = new FireboltStatementService(statementClient, true);
		fireboltStatementService.execute(statementInfoWrapper, fireboltProperties, -1, 0, false);
		verify(statementClient).postSqlStatement(statementInfoWrapper, fireboltProperties, true, -1, 0, false);
	}
}
