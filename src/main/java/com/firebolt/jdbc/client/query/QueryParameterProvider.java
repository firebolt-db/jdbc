package com.firebolt.jdbc.client.query;

import com.firebolt.jdbc.connection.settings.FireboltProperties;
import com.firebolt.jdbc.statement.StatementInfoWrapper;
import java.util.Map;

/**
 * Class responsible for creating the query parameters that need to be sent to the backend.
 * The parameters are different for each type of backend, though some ore common. Having each backend type provide its own query parameters makes it more flexible
 */
public interface QueryParameterProvider {

    String TAB_SEPARATED_WITH_NAMES_AND_TYPES_FORMAT = "TabSeparatedWithNamesAndTypes";

    /**
     * Returns a map of parameters that will be added to the url and sent to the firebolt backend
     * @return
     */
    Map<String,String> getQueryParams(FireboltProperties properties, StatementInfoWrapper statementInfoWrapper, int queryTimeout, boolean isServerAsync);

}
