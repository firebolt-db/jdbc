package com.firebolt.jdbc.statement;

import static com.firebolt.jdbc.statement.StatementType.PARAM_SETTING;

import java.util.UUID;

import org.apache.commons.lang3.tuple.Pair;

import com.firebolt.jdbc.statement.rawstatement.RawStatement;
import com.firebolt.jdbc.statement.rawstatement.SetParamRawStatement;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NonNull;

/**
 * This represents a query that is ready to be sent to Firebolt or executed
 * internally for param setting
 */
@Data
@AllArgsConstructor
public class StatementInfoWrapper {
	String sql;
	String id;
	StatementType type;
	Pair<String, String> param;
	RawStatement initialQuery;

	public StatementInfoWrapper(String sql, String id, StatementType statementType) {
		this.sql = sql;
		this.id = id;
		this.type = statementType;
	}

	/**
	 * Creates a StatementInfoWrapper from the {@link RawStatement}.
	 * @throws IllegalArgumentException if trying to create a StatementInfoWrapper with a raw statement that has
	 * parameter markets, which indicates that the statement has params that have not been set yet.
	 * @param rawStatement the raw statement
	 * @return the statement that will be sent to the server
	 */
	public static StatementInfoWrapper of(@NonNull RawStatement rawStatement) {
		return of(rawStatement, UUID.randomUUID().toString());
	}

	/**
	 * Creates a StatementInfoWrapper from the {@link RawStatement}.
	 * @throws IllegalArgumentException if trying to create a StatementInfoWrapper with a raw statement that has
	 * parameter markets, which indicates that the statement has params that have not been set yet.
	 * @param rawStatement the raw statement
	 * @param id the id of the statement to execute
	 * @return the statement that will be sent to the server
	 */
	public static StatementInfoWrapper of(@NonNull RawStatement rawStatement, String id) {
		if (rawStatement.getParamMarkers() != null && !rawStatement.getParamMarkers().isEmpty()) {
			throw new IllegalArgumentException(String.format(
					"Cannot execute a statement that does not have all its parameter markers set. Statement: %s",
					rawStatement.getSql()));
		}
		Pair<String, String> additionalProperties = rawStatement.getStatementType() == PARAM_SETTING
				? ((SetParamRawStatement) rawStatement).getAdditionalProperty()
				: null;
		return new StatementInfoWrapper(rawStatement.getSql(), id, rawStatement.getStatementType(),
				additionalProperties, rawStatement);
	}
}
