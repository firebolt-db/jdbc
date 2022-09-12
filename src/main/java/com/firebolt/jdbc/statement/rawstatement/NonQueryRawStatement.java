package com.firebolt.jdbc.statement.rawstatement;

import static com.firebolt.jdbc.statement.StatementType.NON_QUERY;

import java.util.List;

import com.firebolt.jdbc.statement.StatementType;

/**
 * A non query statement is a statement that does not return data (such as INSERT)
 */
public class NonQueryRawStatement extends RawStatement {

    public NonQueryRawStatement(String sql, String cleanSql, List<SqlParamMarker> paramPositions) {
        super(sql, cleanSql, paramPositions);
    }

    @Override
	public StatementType getStatementType() {
		return NON_QUERY;
	}
}
