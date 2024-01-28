package com.firebolt.jdbc.statement;

import com.firebolt.jdbc.statement.rawstatement.RawStatement;
import com.firebolt.jdbc.statement.rawstatement.SetParamRawStatement;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;

import java.util.Map.Entry;
import java.util.UUID;

import static com.firebolt.jdbc.statement.StatementType.PARAM_SETTING;

/**
 * This represents a statement that is ready to be sent to Firebolt or executed
 * internally to set a param
 */
@Getter
@AllArgsConstructor
public class StatementInfoWrapper {
	private final String sql;
	private final String label;
	private String id;
	private final StatementType type;
	private final Entry<String, String> param;
	private final RawStatement initialStatement;

	public StatementInfoWrapper(String sql, StatementType type, Entry<String, String> param, RawStatement initialStatement) {
		this.sql = sql;
		this.type = type;
		this.param = param;
		this.initialStatement = initialStatement;
		this.label = UUID.randomUUID().toString();
	}

	/**
	 * Creates a StatementInfoWrapper from the {@link RawStatement}.
	 * 
	 * @param rawStatement the raw statement
	 * @return the statement that will be sent to the server
	 */
	public static StatementInfoWrapper of(@NonNull RawStatement rawStatement) {
		Entry<String, String> additionalProperties = rawStatement.getStatementType() == PARAM_SETTING
				? ((SetParamRawStatement) rawStatement).getAdditionalProperty()
				: null;
		return new StatementInfoWrapper(rawStatement.getSql(), rawStatement.getStatementType(),
				additionalProperties, rawStatement);
	}
}
