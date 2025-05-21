package com.firebolt.jdbc.client.query;

import com.firebolt.jdbc.connection.settings.FireboltQueryParameterKey;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import static com.firebolt.jdbc.client.query.QueryParameterProvider.TAB_SEPARATED_WITH_NAMES_AND_TYPES_FORMAT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class LocalhostQueryParameterProviderTest extends AbstractQueryParameterProviderTest {

    private LocalhostQueryParameterProvider localhostQueryParameterProvider = new LocalhostQueryParameterProvider();

    @BeforeEach
    void setupMethod() {
        // have some initial properties
        mockAdditionalProperties(Map.of("key1", "value1"));
    }

    @Test
    void canSetQueryParamWhenSystemEngine() {
        mockSystemEngine();

        Map<String, String> queryParams = localhostQueryParameterProvider.getQueryParams(mockFireboltProperties, mockStatementInfoWrapper, NO_QUERY_TIMEOUT, IS_NOT_SERVER_ASYNC);

        assertEquals(2, queryParams.size());

        assertEquals(TAB_SEPARATED_WITH_NAMES_AND_TYPES_FORMAT, queryParams.get(FireboltQueryParameterKey.DEFAULT_FORMAT.getKey()));
        assertEquals("value1", queryParams.get("key1"));
    }

    @Test
    void canSetQueryParamWhenUserEngine() {
        mockUserEngine();
        mockIsCompressInProperties(false);
        mockDatabaseInProperties(null);
        mockEngineInProperties(null);
        mockPreparedStatements(null);
        mockStatementWrapperLabel(STATEMENT_WRAPPER_LABEL);

        Map<String, String> queryParams = localhostQueryParameterProvider.getQueryParams(mockFireboltProperties, mockStatementInfoWrapper, NO_QUERY_TIMEOUT, IS_NOT_SERVER_ASYNC);

        assertEquals(4, queryParams.size());

        assertEquals(STATEMENT_WRAPPER_LABEL, queryParams.get(FireboltQueryParameterKey.QUERY_LABEL.getKey()));
        assertEquals("0", queryParams.get(FireboltQueryParameterKey.COMPRESS.getKey()));
        assertEquals(TAB_SEPARATED_WITH_NAMES_AND_TYPES_FORMAT, queryParams.get(FireboltQueryParameterKey.DEFAULT_FORMAT.getKey()));
        assertEquals("value1", queryParams.get("key1"));
    }

    @Test
    void canSetQueryParamWhenUserEngineAndAdditionalProperties() {
        mockUserEngine();
        mockIsCompressInProperties(true);
        mockDatabaseInProperties(DATABASE);
        mockEngineInProperties(ENGINE);
        mockPreparedStatements(PREPARED_STATEMENTS);
        mockStatementWrapperLabel(STATEMENT_WRAPPER_LABEL);
        mockAccountIdProperties(ACCOUNT_ID);

        Map<String, String> queryParams = localhostQueryParameterProvider.getQueryParams(mockFireboltProperties, mockStatementInfoWrapper, QUERY_TIMEOUT, IS_SERVER_ASYNC);

        assertEquals(10, queryParams.size());

        assertEquals(STATEMENT_WRAPPER_LABEL, queryParams.get(FireboltQueryParameterKey.QUERY_LABEL.getKey()));
        assertEquals("1", queryParams.get(FireboltQueryParameterKey.COMPRESS.getKey()));
        assertEquals("true", queryParams.get(FireboltQueryParameterKey.ASYNC.getKey()));
        assertEquals(DATABASE, queryParams.get(FireboltQueryParameterKey.DATABASE.getKey()));
        assertEquals(ENGINE, queryParams.get(FireboltQueryParameterKey.ENGINE.getKey()));
        assertEquals(ACCOUNT_ID, queryParams.get(FireboltQueryParameterKey.ACCOUNT_ID.getKey()));
        assertEquals(String.valueOf(QUERY_TIMEOUT), queryParams.get(FireboltQueryParameterKey.MAX_EXECUTION_TIME.getKey()));
        assertEquals(TAB_SEPARATED_WITH_NAMES_AND_TYPES_FORMAT, queryParams.get(FireboltQueryParameterKey.DEFAULT_FORMAT.getKey()));
        assertEquals(PREPARED_STATEMENTS, queryParams.get(FireboltQueryParameterKey.QUERY_PARAMETERS.getKey()));
        assertEquals("value1", queryParams.get("key1"));
    }

    @Test
    void canSetQueryParamWhenUserEngineAndAndQueryLabelInProperties() {
        when(mockFireboltProperties.getRuntimeAdditionalProperties()).thenReturn(Map.of(FireboltQueryParameterKey.QUERY_LABEL.getKey(), SESSION_PROPERTY_QUERY_LABEL));
        mockUserEngine();
        mockIsCompressInProperties(false);

        Map<String, String> queryParams = localhostQueryParameterProvider.getQueryParams(mockFireboltProperties, mockStatementInfoWrapper, NO_QUERY_TIMEOUT, IS_NOT_SERVER_ASYNC);

        assertEquals(4, queryParams.size());

        assertEquals(SESSION_PROPERTY_QUERY_LABEL, queryParams.get(FireboltQueryParameterKey.QUERY_LABEL.getKey()));
        assertEquals("0", queryParams.get(FireboltQueryParameterKey.COMPRESS.getKey()));
        assertEquals(TAB_SEPARATED_WITH_NAMES_AND_TYPES_FORMAT, queryParams.get(FireboltQueryParameterKey.DEFAULT_FORMAT.getKey()));
        assertEquals("value1", queryParams.get("key1"));
    }
}
