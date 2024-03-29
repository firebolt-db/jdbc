package com.firebolt.jdbc.statement.rawstatement;

import com.firebolt.jdbc.statement.ParamMarker;
import com.firebolt.jdbc.statement.StatementType;
import com.firebolt.jdbc.statement.StatementUtil;
import lombok.Data;

import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.Collectors;

@Data
public abstract class RawStatement {

	private final String sql;
	private final String cleanSql;
	private final List<ParamMarker> paramMarkers;

	protected RawStatement(String sql, String cleanSql, List<ParamMarker> paramPositions) {
		this.sql = sql;
		this.cleanSql = cleanSql;
		this.paramMarkers = paramPositions;
	}

	public static RawStatement of(String sql, List<ParamMarker> paramPositions, String cleanSql) {
		Optional<Entry<String, String>> additionalProperties = StatementUtil.extractParamFromSetStatement(cleanSql, sql);
		if (additionalProperties.isPresent()) {
			return new SetParamRawStatement(sql, cleanSql, paramPositions, additionalProperties.get());
		} else if (StatementUtil.isQuery(cleanSql)) {
			return new QueryRawStatement(sql, cleanSql, paramPositions);
		} else {
			return new NonQueryRawStatement(sql, cleanSql, paramPositions);
		}
	}

	@Override
	public String toString() {
		return "RawSqlStatement{" + "sql='" + sql + '\'' + ", cleanSql='" + cleanSql + '\'' + ", paramMarkers="
				+ paramMarkers.stream().map(ParamMarker::toString).collect(Collectors.joining("|")) + '}';
	}

	public List<ParamMarker> getParamMarkers() {
		return paramMarkers;
	}

	public String getSql() {
		return sql;
	}

	public String getCleanSql() {
		return cleanSql;
	}

	public abstract StatementType getStatementType();
}
