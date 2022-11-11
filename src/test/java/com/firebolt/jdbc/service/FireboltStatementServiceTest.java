package com.firebolt.jdbc.service;

import com.firebolt.jdbc.client.query.StatementClient;
import com.firebolt.jdbc.connection.settings.FireboltProperties;
import com.firebolt.jdbc.exception.FireboltException;
import com.firebolt.jdbc.statement.StatementInfoWrapper;
import com.firebolt.jdbc.statement.StatementUtil;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Map;

import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

@ExtendWith(MockitoExtension.class)
class FireboltStatementServiceTest {

    @Mock
    private StatementClient statementClient;

    @Test
    void shouldExecuteQueryWithAllRequiredParameters() throws FireboltException {
        StatementInfoWrapper statementInfoWrapper = StatementUtil.parseToStatementInfoWrappers("SELECT 1").get(0);
        FireboltProperties fireboltProperties = FireboltProperties.builder().database("db").host("firebolt1").ssl(true)
                .compress(true).build();
        Map<String, String> statementParams = ImmutableMap.of("param_1", "value_1");
        FireboltStatementService fireboltStatementService = new FireboltStatementService(statementClient, false);
        fireboltStatementService.execute(statementInfoWrapper, fireboltProperties, statementParams);
        verify(statementClient).postSqlStatement(statementInfoWrapper, fireboltProperties,
                ImmutableMap.of("database", "db", "output_format", "TabSeparatedWithNamesAndTypes", "query_id", statementInfoWrapper.getId(),
                        "compress", "1", "param_1", "value_1"));
    }

    @Test
    void shouldExecuteQueryWithLocalHostFormatParameters() throws FireboltException {
        StatementInfoWrapper statementInfoWrapper = StatementUtil.parseToStatementInfoWrappers("SELECT 1").get(0);
        FireboltProperties fireboltProperties = FireboltProperties.builder().database("db").host("localhost").ssl(true)
                .compress(true).build();
        Map<String, String> statementParams = ImmutableMap.of("param_1", "value_1");

        FireboltStatementService fireboltStatementService = new FireboltStatementService(statementClient, false);
        fireboltStatementService.execute(statementInfoWrapper, fireboltProperties, statementParams);
        verify(statementClient).postSqlStatement(statementInfoWrapper, fireboltProperties,
                ImmutableMap.of("database", "db", "default_format", "TabSeparatedWithNamesAndTypes", "query_id", statementInfoWrapper.getId(),
                        "compress", "1", "param_1", "value_1"));
    }

    @Test
    void shouldCancelQueryWithAllRequiredParams() throws FireboltException {
        FireboltProperties fireboltProperties = FireboltProperties.builder().database("db").host("http://firebolt1")
                .ssl(true).compress(true).build();

        FireboltStatementService fireboltStatementService = new FireboltStatementService(statementClient, false);
        fireboltStatementService.abortStatement("123", fireboltProperties);
        verify(statementClient).abortStatement("123", fireboltProperties, ImmutableMap.of("query_id", "123"));
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
        Map<String, String> statementParams = ImmutableMap.of("param_1", "value_1");
        FireboltStatementService fireboltStatementService = new FireboltStatementService(statementClient, true);
        fireboltStatementService.execute(statementInfoWrapper, fireboltProperties, statementParams);
        verify(statementClient).postSqlStatement(statementInfoWrapper, fireboltProperties,
                ImmutableMap.of("output_format", "TabSeparatedWithNamesAndTypes", "param_1", "value_1"));
    }
}
