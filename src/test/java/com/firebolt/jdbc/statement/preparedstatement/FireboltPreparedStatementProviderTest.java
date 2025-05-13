package com.firebolt.jdbc.statement.preparedstatement;


import com.firebolt.jdbc.connection.FireboltConnection;
import com.firebolt.jdbc.connection.settings.FireboltProperties;
import com.firebolt.jdbc.service.FireboltStatementService;
import com.firebolt.jdbc.type.ParserVersion;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.when;

class FireboltPreparedStatementProviderTest {
    private final FireboltConnection connection = Mockito.mock(FireboltConnection.class);
    @Mock
    private FireboltStatementService statementService;

    @Test
    void willReturnTheSameInstanceOfFireboltPreparedStatementProvider() {
        FireboltPreparedStatementProvider preparedStatementProvider1 = FireboltPreparedStatementProvider.getInstance();
        FireboltPreparedStatementProvider preparedStatementProvider2 = FireboltPreparedStatementProvider.getInstance();

        assertSame(preparedStatementProvider1, preparedStatementProvider2);
    }

    @Test
    void shouldReturnNormalPreparedStatementWithNoParameterSet() {
        FireboltPreparedStatementProvider preparedStatementProvider = FireboltPreparedStatementProvider.getInstance();
        Properties properties = new Properties();
        FireboltProperties fireboltProperties = new FireboltProperties(properties);
        when(connection.getSessionProperties()).thenReturn(fireboltProperties);
        when(connection.getParserVersion()).thenReturn(ParserVersion.CURRENT);
        assertInstanceOf(FireboltPreparedStatement.class, preparedStatementProvider.getPreparedStatement(fireboltProperties, connection, statementService, ""));
        assertFalse(preparedStatementProvider.getPreparedStatement(fireboltProperties, connection, statementService, "") instanceof FireboltBackendPreparedStatement);
    }

    @Test
    void shouldReturnNormalPreparedStatementWithNativeParamStyle() {
        FireboltPreparedStatementProvider preparedStatementProvider = FireboltPreparedStatementProvider.getInstance();
        Properties properties = new Properties();
        properties.setProperty("prepared_statement_param_style", "native");
        FireboltProperties fireboltProperties = new FireboltProperties(properties);
        when(connection.getSessionProperties()).thenReturn(fireboltProperties);
        when(connection.getParserVersion()).thenReturn(ParserVersion.CURRENT);
        assertInstanceOf(FireboltPreparedStatement.class, preparedStatementProvider.getPreparedStatement(fireboltProperties, connection, statementService, ""));
        assertFalse(preparedStatementProvider.getPreparedStatement(fireboltProperties, connection, statementService, "") instanceof FireboltBackendPreparedStatement);
    }

    @Test
    void shouldReturnBackendPreparedStatementWithFbNumericParamStyle() {
        FireboltPreparedStatementProvider preparedStatementProvider = FireboltPreparedStatementProvider.getInstance();
        Properties properties = new Properties();
        properties.setProperty("prepared_statement_param_style", "fb_numeric");
        FireboltProperties fireboltProperties = new FireboltProperties(properties);
        when(connection.getSessionProperties()).thenReturn(fireboltProperties);
        when(connection.getParserVersion()).thenReturn(ParserVersion.CURRENT);
        assertInstanceOf(FireboltBackendPreparedStatement.class, preparedStatementProvider.getPreparedStatement(fireboltProperties, connection, statementService, ""));
    }
}
