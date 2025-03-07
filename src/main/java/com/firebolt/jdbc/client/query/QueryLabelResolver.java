package com.firebolt.jdbc.client.query;

import org.apache.commons.lang3.StringUtils;

import com.firebolt.jdbc.connection.settings.FireboltProperties;
import com.firebolt.jdbc.connection.settings.FireboltQueryParameterKey;
import com.firebolt.jdbc.statement.StatementInfoWrapper;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class QueryLabelResolver {

    /**
     * Resolves a query label. If there is already one set on the session properties use that one. If not use the one from the statement
     * @param sessionProperties
     * @param statementInfoWrapper
     * @return
     */
    public static String getQueryLabel(FireboltProperties sessionProperties, StatementInfoWrapper statementInfoWrapper) {
        String existingQueryLabel = sessionProperties.getRuntimeAdditionalProperties().get(FireboltQueryParameterKey.QUERY_LABEL.getKey());
        return StringUtils.isNotBlank(existingQueryLabel) ? existingQueryLabel : statementInfoWrapper.getLabel();
    }
}
