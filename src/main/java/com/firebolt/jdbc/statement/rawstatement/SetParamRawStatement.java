package com.firebolt.jdbc.statement.rawstatement;

import com.firebolt.jdbc.statement.ParamMarker;
import com.firebolt.jdbc.statement.StatementType;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.List;
import java.util.Map.Entry;

import static com.firebolt.jdbc.statement.StatementType.PARAM_SETTING;

/**
 * A Set param statement is a special statement that sets a parameter internally
 * (this type of statement starts with SET)
 */
@Getter
@EqualsAndHashCode(callSuper = true)
public class SetParamRawStatement extends RawStatement {
	private final Entry<String, String> additionalProperty;

	public SetParamRawStatement(String sql, String cleanSql, List<ParamMarker> paramPositions, Entry<String, String> additionalProperty) {
		super(sql, cleanSql, paramPositions);
		this.additionalProperty = additionalProperty;
	}

	@Override
	public StatementType getStatementType() {
		return PARAM_SETTING;
	}
}
