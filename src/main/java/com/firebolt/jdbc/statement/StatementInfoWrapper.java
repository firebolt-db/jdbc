package com.firebolt.jdbc.statement;

import static com.firebolt.jdbc.statement.StatementType.PARAM_SETTING;

import org.apache.commons.lang3.tuple.Pair;

import com.firebolt.jdbc.statement.rawstatement.RawSqlStatement;
import com.firebolt.jdbc.statement.rawstatement.SetParamStatement;

import lombok.Builder;
import lombok.Value;

/**
 * This represents a query that is ready to be sent to Firebolt or executed
 * internally for param setting
 */
@Value
@Builder
public class StatementInfoWrapper {
	String sql;
	String id;
	StatementType type;
	Pair<String, String> param;
	RawSqlStatement initialQuery;

	public static StatementInfoWrapper of(RawSqlStatement rawStatement, String id) {
		if (rawStatement.getParamMarkers().size() != 0) {
			throw new IllegalArgumentException(String.format(
					"Cannot execute a statement that does not have all its parameter markers set ! Statement: %s",
					rawStatement.getSql()));
		}
		Pair<String, String> additionalProperties = rawStatement.getStatementType() == PARAM_SETTING
				? ((SetParamStatement) rawStatement).getAdditionalProperty()
				: null;
		return new StatementInfoWrapper(rawStatement.getSql(), id, rawStatement.getStatementType(),
				additionalProperties, rawStatement);

	}
}
