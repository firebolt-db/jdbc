package com.firebolt.jdbc.statement.rawstatement;

import com.firebolt.jdbc.statement.ParamMarker;
import com.firebolt.jdbc.statement.StatementType;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

import static com.firebolt.jdbc.connection.settings.FireboltQueryParameterKey.ACCOUNT_ID;
import static com.firebolt.jdbc.connection.settings.FireboltQueryParameterKey.DATABASE;
import static com.firebolt.jdbc.connection.settings.FireboltQueryParameterKey.ENGINE;
import static com.firebolt.jdbc.connection.settings.FireboltQueryParameterKey.OUTPUT_FORMAT;
import static com.firebolt.jdbc.statement.StatementType.PARAM_SETTING;
import static java.lang.String.CASE_INSENSITIVE_ORDER;
import static java.lang.String.format;
import static java.util.stream.Collectors.toCollection;

/**
 * A Set param statement is a special statement that sets a parameter internally
 * (this type of statement starts with SET)
 */
@Getter
@EqualsAndHashCode(callSuper = true)
public class SetParamRawStatement extends RawStatement {
	private static final Set<String> forbiddenParameters = caseInsensitiveNameSet(DATABASE, ENGINE, ACCOUNT_ID, OUTPUT_FORMAT);
	private static final Set<String> useSupporting = caseInsensitiveNameSet(DATABASE, ENGINE);
	private static final String FORBIDDEN_PROPERTY_ERROR_PREFIX = "Could not set parameter. Set parameter '%s' is not allowed. ";
	private static final String FORBIDDEN_PROPERTY_ERROR_USE_SUFFIX = "Try again with 'USE %s' instead of SET.";
	private static final String FORBIDDEN_PROPERTY_ERROR_SET_SUFFIX = "Try again with a different parameter name.";
	private static final String USE_ERROR = FORBIDDEN_PROPERTY_ERROR_PREFIX + FORBIDDEN_PROPERTY_ERROR_USE_SUFFIX;
	private static final String SET_ERROR = FORBIDDEN_PROPERTY_ERROR_PREFIX + FORBIDDEN_PROPERTY_ERROR_SET_SUFFIX;

	private final Entry<String, String> additionalProperty;

	public SetParamRawStatement(String sql, String cleanSql, List<ParamMarker> paramPositions, Entry<String, String> additionalProperty) {
		super(sql, cleanSql, paramPositions);
		validateProperty(additionalProperty.getKey().toUpperCase());
		this.additionalProperty = additionalProperty;
	}

	@Override
	public StatementType getStatementType() {
		return PARAM_SETTING;
	}

	private void validateProperty(String name) {
		if (forbiddenParameters.contains(name)) {
			throw new IllegalArgumentException(format(useSupporting.contains(name) ? USE_ERROR : SET_ERROR, name, name));
		}
	}

	@SafeVarargs
	private static <T extends Enum<T>> Set<String> caseInsensitiveNameSet(Enum<T> ... elements) {
		return Arrays.stream(elements).map(Enum::name).collect(toCollection(() -> new TreeSet<>(CASE_INSENSITIVE_ORDER)));
	}
}
