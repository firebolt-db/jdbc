package com.firebolt.jdbc.client.query;

import com.firebolt.jdbc.connection.settings.FireboltProperties;
import com.firebolt.jdbc.statement.StatementInfoWrapper;
import com.firebolt.jdbc.statement.StatementType;
import java.util.HashMap;
import java.util.Map;

import static com.firebolt.jdbc.connection.settings.FireboltQueryParameterKey.OUTPUT_FORMAT;

/**
 * Creates the query parameters that will be sent to the core backend
 */
public class FireboltCoreQueryParameterProvider extends AbstractQueryParameterProvider {

    @Override
    public Map<String, String> getQueryParams(FireboltProperties fireboltProperties, StatementInfoWrapper statementInfoWrapper, int queryTimeout, boolean isServerAsync) {
        Map<String, String> params = new HashMap<>(fireboltProperties.getAdditionalProperties());

        // firebolt core only supports output format (default_format will be not supported soon for core)
        if (statementInfoWrapper.getType() == StatementType.QUERY) {
            params.put(OUTPUT_FORMAT.getKey(), TAB_SEPARATED_WITH_NAMES_AND_TYPES_FORMAT);
        }

        addQueryLabel(params, fireboltProperties, statementInfoWrapper);
        addCompress(params, fireboltProperties.isCompress());
        addDatabaseIfNeeded(params, fireboltProperties.getDatabase());
        addQueryParameterIfNeeded(params, statementInfoWrapper.getPreparedStatementParameters());
        addQueryTimeoutIfNeeded(params, queryTimeout);
        addServerAsyncIfNeeded(params, isServerAsync);

        return params;
    }

}
