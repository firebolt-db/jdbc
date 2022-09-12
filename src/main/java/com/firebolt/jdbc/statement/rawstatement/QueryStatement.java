package com.firebolt.jdbc.statement.rawstatement;

import static com.firebolt.jdbc.statement.StatementType.QUERY;

import java.util.List;
import java.util.Optional;

import org.apache.commons.lang3.tuple.Pair;

import com.firebolt.jdbc.statement.StatementType;
import com.firebolt.jdbc.statement.StatementUtil;

import lombok.Getter;

/**
 * A query statement is a statement that returns data (Typically starts with
 * SELECT, SHOW, etc)
 */
@Getter
public class QueryStatement extends RawSqlStatement {

	String database;

	String table;

	public QueryStatement(String sql, String cleanSql, List<SqlParamMarker> paramPositions) {
		super(sql, cleanSql, paramPositions);
		Pair<Optional<String>, Optional<String>> databaseAndTablePair = StatementUtil
				.extractDbNameAndTableNamePairFromQuery(this.getCleanSql());
		this.database = databaseAndTablePair.getLeft().orElse(null);
		this.table = databaseAndTablePair.getRight().orElse(null);
	}

	@Override
	public StatementType getStatementType() {
		return QUERY;
	}

}
