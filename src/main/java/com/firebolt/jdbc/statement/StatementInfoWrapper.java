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
 * This represents a statement that is ready to be sent to Firebolt or executed
 * internally to set a param
 */
@Data
@AllArgsConstructor
public class StatementInfoWrapper {
	private String sql;
	private String id;
	private StatementType type;
	private Pair<String, String> param;
	private RawStatement initialStatement;

	/**
	 * Creates a StatementInfoWrapper from the {@link RawStatement}.
	 * @param rawStatement the raw statement
	 * @return the statement that will be sent to the server
	 */
	public static StatementInfoWrapper of(@NonNull RawStatement rawStatement) {
		return of(rawStatement, UUID.randomUUID().toString());
	}

	/**
	 * Creates a StatementInfoWrapper from the {@link RawStatement}.
	 * @param rawStatement the raw statement
	 * @param id the id of the statement to execute
	 * @return the statement that will be sent to the server
	 */
	public static StatementInfoWrapper of(@NonNull RawStatement rawStatement, String id) {
		Pair<String, String> additionalProperties = rawStatement.getStatementType() == PARAM_SETTING
				? ((SetParamRawStatement) rawStatement).getAdditionalProperty()
				: null;
		return new StatementInfoWrapper(rawStatement.getSql(), id, rawStatement.getStatementType(),
				additionalProperties, rawStatement);
	}
}
