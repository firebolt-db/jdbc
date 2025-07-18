package com.firebolt.jdbc.client.query;

import com.firebolt.jdbc.connection.settings.FireboltProperties;
import com.firebolt.jdbc.statement.StatementInfoWrapper;
import com.firebolt.jdbc.statement.StatementType;
import java.util.HashMap;
import java.util.Map;

import static com.firebolt.jdbc.connection.settings.FireboltQueryParameterKey.OUTPUT_FORMAT;

/**
 * Query parameters that will be added when calling the firebolt cloud v1 APIs
 */
public class FireboltCloudV1QueryParameterProvider extends AbstractQueryParameterProvider {

    @Override
    public Map<String, String> getQueryParams(FireboltProperties fireboltProperties, StatementInfoWrapper statementInfoWrapper, int queryTimeout, boolean isServerAsync) {
        Map<String, String> params = new HashMap<>(fireboltProperties.getAdditionalProperties());

        if (statementInfoWrapper.getType() == StatementType.QUERY) {
            params.put(OUTPUT_FORMAT.getKey(), TAB_SEPARATED_WITH_NAMES_AND_TYPES_FORMAT);
        }

        addDatabaseIfNeeded(params, fireboltProperties.getDatabase());

        boolean systemEngine = fireboltProperties.isSystemEngine();
        if (systemEngine) {
            addAccountIdIfNeeded(params, fireboltProperties.getAccountId());
        } else {
            addCompress(params, fireboltProperties.isCompress());
            addQueryTimeoutIfNeeded(params, queryTimeout);
        }

        return params;
    }

}
