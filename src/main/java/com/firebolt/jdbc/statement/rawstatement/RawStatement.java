package com.firebolt.jdbc.statement.rawstatement;

import java.util.List;
import java.util.Optional;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import com.firebolt.jdbc.statement.ParamMarker;
import com.firebolt.jdbc.statement.StatementType;
import com.firebolt.jdbc.statement.StatementUtil;

import lombok.Data;

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

	@Override
	public String toString() {
		return "RawSqlStatement{" + "sql='" + sql + '\'' + ", cleanSql='" + cleanSql + '\'' + ", paramMarkers="
				+ StringUtils.join(paramMarkers, "|") + '}';
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

	public static RawStatement of(String sql, List<ParamMarker> paramPositions, String cleanSql) {
		Optional<Pair<String, String>> additionalProperties = StatementUtil.extractPropertyFromQuery(cleanSql, sql);
		if (additionalProperties.isPresent()) {
			return new SetParamRawStatement(sql, cleanSql, paramPositions, additionalProperties.get());
		} else if (StatementUtil.isQuery(cleanSql)) {
			return new QueryRawStatement(sql, cleanSql, paramPositions);
		} else {
			return new NonQueryRawStatement(sql, cleanSql, paramPositions);
		}
	}
}
