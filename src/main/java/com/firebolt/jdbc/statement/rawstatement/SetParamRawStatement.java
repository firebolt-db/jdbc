package com.firebolt.jdbc.statement.rawstatement;

import static com.firebolt.jdbc.statement.StatementType.PARAM_SETTING;

import java.util.List;

import org.apache.commons.lang3.tuple.Pair;

import com.firebolt.jdbc.statement.ParamMarker;
import com.firebolt.jdbc.statement.StatementType;

import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * A Set param statement is a special statement that sets a parameter internally
 * (this type of statement starts with SET)
 */
@Getter
@EqualsAndHashCode(callSuper = true)
public class SetParamRawStatement extends RawStatement {

	private final Pair<String, String> additionalProperty;

	public SetParamRawStatement(String sql, String cleanSql, List<ParamMarker> paramPositions,
			Pair<String, String> additionalProperty) {
		super(sql, cleanSql, paramPositions);
		this.additionalProperty = additionalProperty;
	}

	@Override
	public StatementType getStatementType() {
		return PARAM_SETTING;
	}

}
