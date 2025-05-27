package com.firebolt.jdbc.client.query;

import com.firebolt.jdbc.connection.settings.FireboltQueryParameterKey;
import com.firebolt.jdbc.statement.StatementType;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class FireboltCloudV2QueryParameterProviderTest extends AbstractQueryParameterProviderTest {

    private FireboltCloudV2QueryParameterProvider fireboltCloudV2QueryParameterProvider = new FireboltCloudV2QueryParameterProvider();

    @BeforeEach
    void setupMethod() {
        // have some initial properties
        mockAdditionalProperties(Map.of("key1", "value1"));
    }

    @Test
    void shouldSetQueryParamsForParamSettingStatement() {
        mockIsCompressInProperties(true);
        mockDatabaseInProperties(null);
        mockPreparedStatements(null);
        mockStatementType(StatementType.PARAM_SETTING);
        mockStatementWrapperLabel(STATEMENT_WRAPPER_LABEL);

        Map<String, String> queryParams = fireboltCloudV2QueryParameterProvider.getQueryParams(mockFireboltProperties, mockStatementInfoWrapper, NO_QUERY_TIMEOUT, IS_SERVER_ASYNC);

        assertEquals(4, queryParams.size());

        assertEquals(STATEMENT_WRAPPER_LABEL, queryParams.get(FireboltQueryParameterKey.QUERY_LABEL.getKey()));
        assertEquals("1", queryParams.get(FireboltQueryParameterKey.COMPRESS.getKey()));
        assertEquals("true", queryParams.get(FireboltQueryParameterKey.ASYNC.getKey()));
        assertEquals("value1", queryParams.get("key1"));
    }

    @Test
    void shouldSetQueryParamsForQueryStatement() {
        mockIsCompressInProperties(false);
        mockDatabaseInProperties(DATABASE);
        mockPreparedStatements(PREPARED_STATEMENTS);
        mockStatementType(StatementType.QUERY);
        mockStatementWrapperLabel(STATEMENT_WRAPPER_LABEL);

        Map<String, String> queryParams = fireboltCloudV2QueryParameterProvider.getQueryParams(mockFireboltProperties, mockStatementInfoWrapper, QUERY_TIMEOUT, IS_NOT_SERVER_ASYNC);

        assertEquals(7, queryParams.size());
        assertEquals(STATEMENT_WRAPPER_LABEL, queryParams.get(FireboltQueryParameterKey.QUERY_LABEL.getKey()));
        assertEquals("0", queryParams.get(FireboltQueryParameterKey.COMPRESS.getKey()));
        assertEquals(DATABASE, queryParams.get(FireboltQueryParameterKey.DATABASE.getKey()));
        assertEquals(String.valueOf(QUERY_TIMEOUT), queryParams.get(FireboltQueryParameterKey.MAX_EXECUTION_TIME.getKey()));
        assertEquals(FireboltCloudV2QueryParameterProvider.TAB_SEPARATED_WITH_NAMES_AND_TYPES_FORMAT, queryParams.get(FireboltQueryParameterKey.OUTPUT_FORMAT.getKey()));
        assertEquals(PREPARED_STATEMENTS, queryParams.get(FireboltQueryParameterKey.QUERY_PARAMETERS.getKey()));
        assertEquals("value1", queryParams.get("key1"));
    }

    @Test
    void shouldUseSessionPropertyQueryLabel() {
        when(mockFireboltProperties.getRuntimeAdditionalProperties()).thenReturn(Map.of(FireboltQueryParameterKey.QUERY_LABEL.getKey(), SESSION_PROPERTY_QUERY_LABEL));
        mockIsCompressInProperties(false);
        mockDatabaseInProperties(null);
        mockPreparedStatements(null);
        mockStatementType(StatementType.QUERY);

        Map<String, String> queryParams = fireboltCloudV2QueryParameterProvider.getQueryParams(mockFireboltProperties, mockStatementInfoWrapper, NO_QUERY_TIMEOUT, IS_NOT_SERVER_ASYNC);

        assertEquals(4, queryParams.size());
        assertEquals(SESSION_PROPERTY_QUERY_LABEL, queryParams.get(FireboltQueryParameterKey.QUERY_LABEL.getKey()));
        assertEquals("0", queryParams.get(FireboltQueryParameterKey.COMPRESS.getKey()));
        assertEquals(FireboltCloudV2QueryParameterProvider.TAB_SEPARATED_WITH_NAMES_AND_TYPES_FORMAT, queryParams.get(FireboltQueryParameterKey.OUTPUT_FORMAT.getKey()));
        assertEquals("value1", queryParams.get("key1"));

        verify(mockStatementInfoWrapper, never()).getLabel();
    }

    @Test
    void shouldNotAddEngineAndAccountIdCompressOrQueryTimeoutForSystemEngine() {
        mockSystemEngine();
        mockDatabaseInProperties(null);
        mockPreparedStatements(null);
        mockStatementType(StatementType.QUERY);

        Map<String, String> queryParams = fireboltCloudV2QueryParameterProvider.getQueryParams(mockFireboltProperties, mockStatementInfoWrapper, QUERY_TIMEOUT, IS_NOT_SERVER_ASYNC);

        assertEquals(2, queryParams.size());
        assertEquals(FireboltCloudV2QueryParameterProvider.TAB_SEPARATED_WITH_NAMES_AND_TYPES_FORMAT, queryParams.get(FireboltQueryParameterKey.OUTPUT_FORMAT.getKey()));
        assertEquals("value1", queryParams.get("key1"));
    }
}
