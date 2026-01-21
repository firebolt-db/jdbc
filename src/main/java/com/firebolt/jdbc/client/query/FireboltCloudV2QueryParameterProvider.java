package com.firebolt.jdbc.client.query;

import com.firebolt.jdbc.connection.settings.FireboltProperties;
import com.firebolt.jdbc.statement.StatementInfoWrapper;
import com.firebolt.jdbc.statement.StatementType;
import java.util.HashMap;
import java.util.Map;

import static com.firebolt.jdbc.connection.settings.FireboltQueryParameterKey.OUTPUT_FORMAT;

/**
 * Query parameters that will be added when calling the firebolt cloud v2 APIs
 */
public class FireboltCloudV2QueryParameterProvider extends AbstractQueryParameterProvider {

    @Override
    public Map<String, String> getQueryParams(FireboltProperties fireboltProperties, StatementInfoWrapper statementInfoWrapper, int queryTimeout, boolean isServerAsync) {
        Map<String, String> params = new HashMap<>(fireboltProperties.getAdditionalProperties());

        if (statementInfoWrapper.getType() == StatementType.QUERY) {
            String outputFormat = fireboltProperties.getQueryResultLocation() == null ? TAB_SEPARATED_WITH_NAMES_AND_TYPES_FORMAT : QueryParameterProvider.S3_TAB_SEPARATED_WITH_NAMES_AND_TYPES_FORMAT;
            params.put(OUTPUT_FORMAT.getKey(), outputFormat);
        }

        addQueryParameterIfNeeded(params, statementInfoWrapper.getPreparedStatementParameters());
        addDatabaseIfNeeded(params, fireboltProperties.getDatabase());
        addServerAsyncIfNeeded(params, isServerAsync);
        addTransactionIdIfNeeded(params, fireboltProperties.getTransactionId());
        addTransactionSequenceIdIfNeeded(params, fireboltProperties.getTransactionSequenceId());

        boolean systemEngine = fireboltProperties.isSystemEngine();
        if (!systemEngine) {
            addAccountIdIfNeeded(params, fireboltProperties.getAccountId());
            addEngineIfNeeded(params, fireboltProperties.getEngine());
            addQueryLabel(params, fireboltProperties, statementInfoWrapper);
            addCompress(params, fireboltProperties.isCompress());
            addQueryTimeoutIfNeeded(params, queryTimeout);
            addS3QueryResultLocationIfNeeded(params, fireboltProperties.getQueryResultLocation(), statementInfoWrapper.getType());
        }

        return params;
    }

}
