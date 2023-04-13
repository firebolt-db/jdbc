package com.firebolt.jdbc.service;

import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.*;

import java.sql.SQLException;
import java.util.Optional;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
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
			FireboltStatementService fireboltStatementService = new FireboltStatementService(statementClient, false);

			fireboltStatementService.execute(statementInfoWrapper, fireboltProperties, 10, -1, true,
					mock(FireboltStatement.class));
			verify(statementClient).executeSqlStatement(statementInfoWrapper, fireboltProperties, false, 10, -1, true);
			Assertions.assertEquals(1, mocked.constructed().size());
		}
	}

	@Test
	void shouldExecuteQueryWithLocalHostFormatParameters() throws SQLException {
		try (MockedConstruction<FireboltResultSet> mocked = Mockito.mockConstruction(FireboltResultSet.class)) {
			StatementInfoWrapper statementInfoWrapper = StatementUtil.parseToStatementInfoWrappers("SELECT 1").get(0);
			FireboltProperties fireboltProperties = FireboltProperties.builder().database("db").host("localhost")
					.ssl(true).compress(false).build();
			FireboltStatementService fireboltStatementService = new FireboltStatementService(statementClient, false);
			fireboltStatementService.execute(statementInfoWrapper, fireboltProperties, -1, 10, true,
					mock(FireboltStatement.class));
			Assertions.assertEquals(1, mocked.constructed().size());
			verify(statementClient).executeSqlStatement(statementInfoWrapper, fireboltProperties, false, -1, 10, true);
		}
	}

	@Test
	void shouldCancelQueryWithAllRequiredParams() throws FireboltException {
		FireboltProperties fireboltProperties = FireboltProperties.builder().database("db").host("http://firebolt1")
				.ssl(true).compress(false).build();

		FireboltStatementService fireboltStatementService = new FireboltStatementService(statementClient, false);
		fireboltStatementService.abortStatement("123", fireboltProperties);
		verify(statementClient).abortStatement("123", fireboltProperties);
	}

	@Test
	void shouldThrowExceptionWhenTryingToCancelQueryWithASystemEngine() throws FireboltException {
		FireboltProperties fireboltProperties = FireboltProperties.builder().database("db").host("http://firebolt1")
				.ssl(true).compress(false).build();

		FireboltStatementService fireboltStatementService = new FireboltStatementService(statementClient, true);
		assertThrows(FireboltException.class, () -> fireboltStatementService.abortStatement("123", fireboltProperties));
		verifyNoInteractions(statementClient);
	}

	@Test
	void shouldExecuteQueryWithParametersForSystemEngine() throws SQLException {
		try (MockedConstruction<FireboltResultSet> mocked = Mockito.mockConstruction(FireboltResultSet.class)) {
			StatementInfoWrapper statementInfoWrapper = StatementUtil.parseToStatementInfoWrappers("SELECT 1").get(0);
			FireboltProperties fireboltProperties = FireboltProperties.builder().database("db").host("firebolt1")
					.ssl(true).compress(false).build();
			FireboltStatementService fireboltStatementService = new FireboltStatementService(statementClient, true);
			fireboltStatementService.execute(statementInfoWrapper, fireboltProperties, 10, 10, true,
					mock(FireboltStatement.class));
			Assertions.assertEquals(1, mocked.constructed().size());
			verify(statementClient).executeSqlStatement(statementInfoWrapper, fireboltProperties, true, 10, 10, true);
		}
	}

	@Test
	void shouldIncludeNonStandardSqlQueryParamForNonStandardSql() throws SQLException {
		try (MockedConstruction<FireboltResultSet> mocked = Mockito.mockConstruction(FireboltResultSet.class)) {

			StatementInfoWrapper statementInfoWrapper = StatementUtil.parseToStatementInfoWrappers("SELECT 1").get(0);
			FireboltProperties fireboltProperties = FireboltProperties.builder().database("db").host("localhost")
					.ssl(true).compress(false).build();

			FireboltStatementService fireboltStatementService = new FireboltStatementService(statementClient, true);
			fireboltStatementService.execute(statementInfoWrapper, fireboltProperties, -1, 0, false,
					mock(FireboltStatement.class));
			Assertions.assertEquals(1, mocked.constructed().size());
			verify(statementClient).executeSqlStatement(statementInfoWrapper, fireboltProperties, true, -1, 0, false);
		}
	}

	@Test
	void shouldBeEmptyWithNonQueryStatement() throws SQLException {
		StatementInfoWrapper statementInfoWrapper = StatementUtil
				.parseToStatementInfoWrappers("INSERT INTO ltv SELECT * FROM ltv_external").get(0);
		FireboltProperties fireboltProperties = FireboltProperties.builder().database("db").host("localhost").ssl(true)
				.compress(false).build();

		FireboltStatementService fireboltStatementService = new FireboltStatementService(statementClient, false);
		Assertions.assertEquals(Optional.empty(), fireboltStatementService.execute(statementInfoWrapper,
				fireboltProperties, -1, 10, true, mock(FireboltStatement.class)));
		verify(statementClient).executeSqlStatement(statementInfoWrapper, fireboltProperties, false, -1, 10, true);
	}

}
