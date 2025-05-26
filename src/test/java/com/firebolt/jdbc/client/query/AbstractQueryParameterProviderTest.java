package com.firebolt.jdbc.client.query;

import com.firebolt.jdbc.connection.settings.FireboltProperties;
import com.firebolt.jdbc.statement.StatementInfoWrapper;
import com.firebolt.jdbc.statement.StatementType;
import java.util.Map;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
abstract class AbstractQueryParameterProviderTest {

    protected static final int NO_QUERY_TIMEOUT = 0;
    protected static final int QUERY_TIMEOUT = 10;

    protected static final boolean IS_SERVER_ASYNC = true;
    protected static final boolean IS_NOT_SERVER_ASYNC = false;

    protected static final String DATABASE = "sample db";
    protected static final String ACCOUNT_ID = "account id";
    protected static final String ENGINE = "sample engine";
    protected static final String PREPARED_STATEMENTS = "query param values for prepared statements";

    protected static final String STATEMENT_WRAPPER_LABEL = "label from statement";
    protected static final String SESSION_PROPERTY_QUERY_LABEL = "label from session properties";

    @Mock
    protected Map<String,String> additionalProperties;

    @Mock
    protected FireboltProperties mockFireboltProperties;

    @Mock
    protected StatementInfoWrapper mockStatementInfoWrapper;

    protected void mockIsCompressInProperties(boolean isCompress) {
        when(mockFireboltProperties.isCompress()).thenReturn(isCompress);
    }

    protected void mockDatabaseInProperties(String database) {
        when(mockFireboltProperties.getDatabase()).thenReturn(database);
    }

    protected void mockEngineInProperties(String engine) {
        when(mockFireboltProperties.getEngine()).thenReturn(engine);
    }

    protected void mockAccountIdProperties(String accountId) {
        when(mockFireboltProperties.getAccountId()).thenReturn(accountId);
    }

    protected void mockPreparedStatements(String preparedStatements) {
        when(mockStatementInfoWrapper.getPreparedStatementParameters()).thenReturn(preparedStatements);
    }

    protected void mockStatementType(StatementType statementType) {
        when(mockStatementInfoWrapper.getType()).thenReturn(statementType);
    }

    protected void mockStatementWrapperLabel(String label) {
        when(mockStatementInfoWrapper.getLabel()).thenReturn(label);
    }

    protected void mockSystemEngine() {
        when(mockFireboltProperties.isSystemEngine()).thenReturn(true);
    }

    protected void mockUserEngine() {
        when(mockFireboltProperties.isSystemEngine()).thenReturn(false);
    }

    protected void mockAdditionalProperties(Map<String, String> additionalProperties) {
        when(mockFireboltProperties.getAdditionalProperties()).thenReturn(additionalProperties);
    }

}
