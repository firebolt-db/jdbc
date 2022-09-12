package com.firebolt.jdbc.statement.rawstatement;

import java.util.List;
import java.util.Optional;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import com.firebolt.jdbc.statement.StatementType;
import com.firebolt.jdbc.statement.StatementUtil;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public abstract class RawSqlStatement {

	String sql;
	String cleanSql;
	List<SqlParamMarker> paramMarkers;

	@Override
	public String toString() {
		return "RawSqlStatement{" + "sql='" + sql + '\'' + ", cleanSql='" + cleanSql + '\'' + ", paramMarkers="
				+ StringUtils.join(paramMarkers, "|") + '}';
	}

	public List<SqlParamMarker> getParamMarkers() {
		return paramMarkers;
	}

	public String getSql() {
		return sql;
	}

	public String getCleanSql() {
		return cleanSql;
	}

	public abstract StatementType getStatementType();

	public static RawSqlStatement of(String sql, List<SqlParamMarker> paramPositions, String cleanSql) {
		Optional<Pair<String, String>> additionalProperties = StatementUtil.extractPropertyFromQuery(cleanSql, sql);
		if (additionalProperties.isPresent()) {
			return new SetParamStatement(sql, cleanSql, paramPositions, additionalProperties.get());
		} else if (StatementUtil.isQuery(cleanSql, true)) {
			return new QueryStatement(sql, cleanSql, paramPositions);
		} else {
			return new NonQueryStatement(sql, cleanSql, paramPositions);
		}
	}
}
