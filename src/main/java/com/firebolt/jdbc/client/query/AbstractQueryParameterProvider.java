package com.firebolt.jdbc.client.query;

import com.firebolt.jdbc.connection.settings.FireboltProperties;
import com.firebolt.jdbc.connection.settings.FireboltQueryParameterKey;
import com.firebolt.jdbc.statement.StatementInfoWrapper;
import com.firebolt.jdbc.statement.StatementType;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

abstract class AbstractQueryParameterProvider implements QueryParameterProvider {

    protected void addQueryTimeoutIfNeeded(Map<String, String> params, int queryTimeout) {
        if (queryTimeout > 0) {
            params.put(FireboltQueryParameterKey.MAX_EXECUTION_TIME.getKey(), String.valueOf(queryTimeout));
        }
    }

    protected void addServerAsyncIfNeeded(Map<String, String> params, boolean isServerAsync) {
        if (isServerAsync) {
            params.put(FireboltQueryParameterKey.ASYNC.getKey(), "true");
        }
    }

    protected void addCompress(Map<String, String> params, boolean isCompress) {
        params.put(FireboltQueryParameterKey.COMPRESS.getKey(), isCompress ? "1" : "0");
    }

    protected void addQueryLabel(Map<String, String> params, FireboltProperties fireboltProperties, StatementInfoWrapper statementInfoWrapper) {
        params.put(FireboltQueryParameterKey.QUERY_LABEL.getKey(), QueryLabelResolver.getQueryLabel(fireboltProperties, statementInfoWrapper)); //QUERY_LABEL
    }

    protected void addEngineIfNeeded(Map<String, String> params, String engine) {
        if (engine != null) {
            params.put(FireboltQueryParameterKey.ENGINE.getKey(), engine);
        }
    }

    protected void addAccountIdIfNeeded(Map<String, String> params, String accountId) {
        if (accountId != null) {
            params.put(FireboltQueryParameterKey.ACCOUNT_ID.getKey(), accountId);
        }
    }

    protected void addQueryParameterIfNeeded(Map<String, String> params, String queryParameters) {
        if (StringUtils.isNotBlank(queryParameters)) {
            params.put(FireboltQueryParameterKey.QUERY_PARAMETERS.getKey(), queryParameters);
        }
    }

    protected void addDatabaseIfNeeded(Map<String,String> params, String database) {
        if (StringUtils.isNotBlank(database)) {
            params.put(FireboltQueryParameterKey.DATABASE.getKey(), database);
        }
    }

    protected void addTransactionIdIfNeeded(Map<String,String> params, String transactionId) {
        if (StringUtils.isNotBlank(transactionId)) {
            params.put(FireboltQueryParameterKey.TRANSACTION_ID.getKey(), transactionId);
        }
    }

    protected void addTransactionSequenceIdIfNeeded(Map<String,String> params, String transactionSequenceId) {
        if (StringUtils.isNotBlank(transactionSequenceId)) {
            params.put(FireboltQueryParameterKey.TRANSACTION_SEQUENCE_ID.getKey(), transactionSequenceId);
        }
    }

    /**
     * When s3 query location is used, by default we enable the advanced mode as well
     */
    protected void addS3QueryResultLocationIfNeeded(Map<String,String> params, String s3queryResultLocation, StatementType statementType) {
        if (StringUtils.isNotBlank(s3queryResultLocation) && statementType == StatementType.QUERY) {
            params.put(FireboltQueryParameterKey.QUERY_RESULT_UPLOAD_LOCATION.getKey(), s3queryResultLocation);
            params.put(FireboltQueryParameterKey.ADVANCED_MODE.getKey(), "true");
        }
    }

}
