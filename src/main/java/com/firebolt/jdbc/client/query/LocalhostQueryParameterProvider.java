package com.firebolt.jdbc.client.query;

import com.firebolt.jdbc.connection.settings.FireboltProperties;
import com.firebolt.jdbc.connection.settings.FireboltQueryParameterKey;
import com.firebolt.jdbc.statement.StatementInfoWrapper;
import java.util.HashMap;
import java.util.Map;

/**
 * This is the query parameter provider for the Dev testing of PackDb
 */
public class LocalhostQueryParameterProvider extends AbstractQueryParameterProvider {

    @Override
    public Map<String, String> getQueryParams(FireboltProperties fireboltProperties, StatementInfoWrapper statementInfoWrapper, int queryTimeout, boolean isServerAsync) {
        Map<String, String> params = new HashMap<>(fireboltProperties.getAdditionalProperties());

        params.put(FireboltQueryParameterKey.DEFAULT_FORMAT.getKey(), TAB_SEPARATED_WITH_NAMES_AND_TYPES_FORMAT);

        addDatabaseIfNeeded(params, fireboltProperties.getDatabase());
        addQueryParameterIfNeeded(params, statementInfoWrapper.getPreparedStatementParameters());
        addServerAsyncIfNeeded(params, isServerAsync);

        boolean systemEngine = fireboltProperties.isSystemEngine();

        // localhost dev will only run against infra 2.0, so we should add account_id only if it was supplied by system URL returned from server.
        // In this case it will be in additionalProperties anyway.
        if (!systemEngine) {
            addAccountIdIfNeeded(params, fireboltProperties.getAccountId());
            addEngineIfNeeded(params, fireboltProperties.getEngine());
            addQueryLabel(params, fireboltProperties, statementInfoWrapper);
            addCompress(params, fireboltProperties.isCompress());
            addQueryTimeoutIfNeeded(params, queryTimeout);
        }

        return params;
    }

}
