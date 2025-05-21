package com.firebolt.jdbc.client.query;

import com.firebolt.jdbc.connection.settings.FireboltQueryParameterKey;
import com.firebolt.jdbc.statement.StatementType;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class FireboltCloudV1QueryParameterProviderTest extends AbstractQueryParameterProviderTest {

    private FireboltCloudV1QueryParameterProvider fireboltCloudV1QueryParameterProvider = new FireboltCloudV1QueryParameterProvider();

    @BeforeEach
    void setupMethod() {
        // have some initial properties
        mockAdditionalProperties(Map.of("key1", "value1"));
    }

    @Test
    void shouldSetQueryParamsForQueryStatement() {
        mockIsCompressInProperties(true);
        mockDatabaseInProperties(DATABASE);
        mockStatementType(StatementType.QUERY);

        Map<String, String> queryParams = fireboltCloudV1QueryParameterProvider.getQueryParams(mockFireboltProperties, mockStatementInfoWrapper, QUERY_TIMEOUT, IS_NOT_SERVER_ASYNC);

        assertEquals(5, queryParams.size());
        assertEquals(DATABASE, queryParams.get(FireboltQueryParameterKey.DATABASE.getKey()));
        assertEquals("1", queryParams.get(FireboltQueryParameterKey.COMPRESS.getKey()));
        assertEquals(String.valueOf(QUERY_TIMEOUT), queryParams.get(FireboltQueryParameterKey.MAX_EXECUTION_TIME.getKey()));
        assertEquals(FireboltCloudV1QueryParameterProvider.TAB_SEPARATED_WITH_NAMES_AND_TYPES_FORMAT, queryParams.get(FireboltQueryParameterKey.OUTPUT_FORMAT.getKey()));
        assertEquals("value1", queryParams.get("key1"));

        assertNull(queryParams.get(FireboltQueryParameterKey.QUERY_LABEL.getKey()));
        assertNull(queryParams.get(FireboltQueryParameterKey.QUERY_PARAMETERS.getKey()));
        assertNull(queryParams.get(FireboltQueryParameterKey.ASYNC.getKey()));
    }

    @Test
    void shouldSetQueryParamsForParamSettingStatement() {
        mockIsCompressInProperties(true);
        mockDatabaseInProperties(null);
        mockStatementType(StatementType.PARAM_SETTING);

        Map<String, String> queryParams = fireboltCloudV1QueryParameterProvider.getQueryParams(mockFireboltProperties, mockStatementInfoWrapper, NO_QUERY_TIMEOUT, IS_SERVER_ASYNC);

        assertEquals(2, queryParams.size());
        assertEquals("1", queryParams.get(FireboltQueryParameterKey.COMPRESS.getKey()));
        assertEquals("value1", queryParams.get("key1"));

        assertNull(queryParams.get(FireboltQueryParameterKey.QUERY_LABEL.getKey()));
        assertNull(queryParams.get(FireboltQueryParameterKey.DATABASE.getKey()));
        assertNull(queryParams.get(FireboltQueryParameterKey.MAX_EXECUTION_TIME.getKey()));
        assertNull(queryParams.get(FireboltQueryParameterKey.QUERY_PARAMETERS.getKey()));
        assertNull(queryParams.get(FireboltQueryParameterKey.OUTPUT_FORMAT.getKey()));
        assertNull(queryParams.get(FireboltQueryParameterKey.ASYNC.getKey()));
    }

    @Test
    void shouldAddAccountIdForSystemEngine() {
        mockSystemEngine();
        mockDatabaseInProperties(null);
        mockStatementType(StatementType.QUERY);
        mockAccountIdProperties(ACCOUNT_ID);

        Map<String, String> queryParams = fireboltCloudV1QueryParameterProvider.getQueryParams(mockFireboltProperties, mockStatementInfoWrapper, NO_QUERY_TIMEOUT, IS_NOT_SERVER_ASYNC);

        assertEquals(3, queryParams.size());
        assertEquals(ACCOUNT_ID, queryParams.get(FireboltQueryParameterKey.ACCOUNT_ID.getKey()));
        assertEquals(FireboltCloudV1QueryParameterProvider.TAB_SEPARATED_WITH_NAMES_AND_TYPES_FORMAT, queryParams.get(FireboltQueryParameterKey.OUTPUT_FORMAT.getKey()));
        assertEquals("value1", queryParams.get("key1"));

        assertNull(queryParams.get(FireboltQueryParameterKey.COMPRESS.getKey()));
        assertNull(queryParams.get(FireboltQueryParameterKey.MAX_EXECUTION_TIME.getKey()));
    }

    @Test
    void shouldNotAddAccountIdForUserEngine() {
        mockUserEngine();
        mockIsCompressInProperties(false);
        mockDatabaseInProperties(null);
        mockStatementType(StatementType.QUERY);

        Map<String, String> queryParams = fireboltCloudV1QueryParameterProvider.getQueryParams(mockFireboltProperties, mockStatementInfoWrapper, NO_QUERY_TIMEOUT, IS_NOT_SERVER_ASYNC);

        assertEquals(3, queryParams.size());
        assertEquals("0", queryParams.get(FireboltQueryParameterKey.COMPRESS.getKey()));
        assertEquals(FireboltCloudV1QueryParameterProvider.TAB_SEPARATED_WITH_NAMES_AND_TYPES_FORMAT, queryParams.get(FireboltQueryParameterKey.OUTPUT_FORMAT.getKey()));
        assertEquals("value1", queryParams.get("key1"));

        assertNull(queryParams.get(FireboltQueryParameterKey.ACCOUNT_ID.getKey()));
        assertNull(queryParams.get(FireboltQueryParameterKey.MAX_EXECUTION_TIME.getKey()));
    }
} 