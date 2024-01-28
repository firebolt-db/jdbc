package com.firebolt.jdbc.statement.rawstatement;

import com.firebolt.jdbc.statement.ParamMarker;
import com.firebolt.jdbc.statement.StatementType;
import com.firebolt.jdbc.statement.StatementUtil;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;

import static com.firebolt.jdbc.statement.StatementType.QUERY;

/**
 * A query statement is a statement that returns data (Typically starts with
 * SELECT, SHOW, etc)
 */
@Getter
@EqualsAndHashCode(callSuper = true)
public class QueryRawStatement extends RawStatement {

	private final String database;

	private final String table;

	public QueryRawStatement(String sql, String cleanSql, List<ParamMarker> paramPositions) {
		super(sql, cleanSql, paramPositions);
		Entry<Optional<String>, Optional<String>> databaseAndTablePair = StatementUtil
				.extractDbNameAndTableNamePairFromCleanQuery(this.getCleanSql());
		this.database = databaseAndTablePair.getKey().orElse(null);
		this.table = databaseAndTablePair.getValue().orElse(null);
	}

	@Override
	public StatementType getStatementType() {
		return QUERY;
	}

}
