package com.firebolt.jdbc.statement.rawstatement;

import com.firebolt.jdbc.connection.FireboltConnection;

import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;

import static com.firebolt.jdbc.connection.settings.FireboltQueryParameterKey.ACCOUNT_ID;
import static com.firebolt.jdbc.connection.settings.FireboltQueryParameterKey.DATABASE;
import static com.firebolt.jdbc.connection.settings.FireboltQueryParameterKey.ENGINE;
import static com.firebolt.jdbc.connection.settings.FireboltQueryParameterKey.OUTPUT_FORMAT;
import static java.lang.String.CASE_INSENSITIVE_ORDER;
import static java.lang.String.format;
import static java.util.stream.Collectors.toMap;

public class SetValidator implements StatementValidator {
    private static final Map<String, String> forbiddenParameters1 = caseInsensitiveNameSet(DATABASE, ENGINE, ACCOUNT_ID, OUTPUT_FORMAT);
    private static final Map<String, String> forbiddenParameters2 = caseInsensitiveNameSet(DATABASE, ENGINE, OUTPUT_FORMAT);
    private static final Map<String, String> useSupporting = caseInsensitiveNameSet(DATABASE, ENGINE);
    private static final String FORBIDDEN_PROPERTY_ERROR_PREFIX = "Could not set parameter. Set parameter '%s' is not allowed. ";
    private static final String FORBIDDEN_PROPERTY_ERROR_USE_SUFFIX = "Try again with 'USE %s' instead of SET.";
    private static final String FORBIDDEN_PROPERTY_ERROR_SET_SUFFIX = "Try again with a different parameter name.";
    private static final String USE_ERROR = FORBIDDEN_PROPERTY_ERROR_PREFIX + FORBIDDEN_PROPERTY_ERROR_USE_SUFFIX;
    private static final String SET_ERROR = FORBIDDEN_PROPERTY_ERROR_PREFIX + FORBIDDEN_PROPERTY_ERROR_SET_SUFFIX;

    private final Map<String, String> forbiddenParameters;

    public SetValidator(FireboltConnection connection) {
        forbiddenParameters = connection.getInfraVersion() < 2 ? forbiddenParameters1 : forbiddenParameters2;
    }

    @Override
    public void validate(RawStatement statement) {
        validateProperty(((SetParamRawStatement)statement).getAdditionalProperty().getKey());
    }

    private void validateProperty(String name) {
        String standardName = forbiddenParameters.get(name);
        if (standardName != null) {
            throw new IllegalArgumentException(format(useSupporting.containsKey(name) ? USE_ERROR : SET_ERROR, standardName, standardName));
        }
    }

    @SafeVarargs
    private static <T extends Enum<T>> Map<String, String> caseInsensitiveNameSet(Enum<T> ... elements) {
        return Arrays.stream(elements).map(Enum::name).collect(toMap(name -> name, name -> name, (one, two) -> two, () -> new TreeMap<>(CASE_INSENSITIVE_ORDER)));
    }
}
