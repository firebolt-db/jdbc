package com.firebolt.jdbc.statement.preparedstatement;

import com.firebolt.jdbc.connection.FireboltConnection;
import com.firebolt.jdbc.connection.settings.FireboltProperties;
import com.firebolt.jdbc.service.FireboltStatementService;

/**
 * Class responsible for creating the PreparedStatement.
 */
@SuppressWarnings("java:S6548") // suppress the warning for singleton. Yes this is a singleton
public class FireboltPreparedStatementProvider {

    private static FireboltPreparedStatementProvider instance;

    // disable creation of the FireboltPreparedStatementProvider using a constructor from outside this class
    private FireboltPreparedStatementProvider() {
    }

    public static synchronized FireboltPreparedStatementProvider getInstance() {
        if (instance == null) {
            instance = new FireboltPreparedStatementProvider();
        }

        return instance;
    }

    public FireboltPreparedStatement getPreparedStatement(FireboltProperties sessionProperties,
                                                  FireboltConnection fireboltConnection,
                                                  FireboltStatementService fireboltStatementService,
                                                  String sql) throws IllegalArgumentException {
        String preparedStatementParamStyleProperty = sessionProperties.getPreparedStatementParamStyle();
        PreparedStatementParamStyle preparedStatementParamStyle = PreparedStatementParamStyle.fromString(preparedStatementParamStyleProperty);
        if (preparedStatementParamStyle.equals(PreparedStatementParamStyle.FB_NUMERIC)) {
            return new FireboltBackendPreparedStatement(fireboltStatementService, fireboltConnection, sql);
        } else if (preparedStatementParamStyle.equals(PreparedStatementParamStyle.NATIVE)) {
            return new FireboltPreparedStatement(fireboltStatementService, fireboltConnection, sql);
        }

        throw new IllegalArgumentException("Unknown prepared statementStyle type: " + (preparedStatementParamStyleProperty == null ? "null" : preparedStatementParamStyleProperty));
    }
}
