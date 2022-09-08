package com.firebolt.jdbc.statement;

import java.util.Collection;
import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Data
public class SqlQueryWrapper {

	List<SubQuery> subQueries;

	long totalParams;


	@Override
	public String toString() {
		return "SqlQueryWrapper{" +
				"subQueries=" + subQueries +
				", totalParams=" + totalParams +
				'}';
	}

	public SqlQueryWrapper(List<SubQuery> subQueries) {
		this.subQueries = subQueries;
		this.totalParams = subQueries.stream()
				.map(SqlQueryWrapper.SubQuery::getParamPositions)
				.mapToLong(Collection::size).sum();
	}

	@Value
	@AllArgsConstructor
	public static class SubQuery {
		String sql;
		List<SqlQueryParameter> paramPositions;

		public List<SqlQueryParameter> getParamPositions() {
			return paramPositions;
		}

		public String getSql() {
			return sql;
		}

		@Value
		@AllArgsConstructor
		public static class SqlQueryParameter {
			int id;
			int position; // Position in the SQL subQuery
		}

		@Override
		public String toString() {
			return "SubQuery{" +
					"sql='" + sql + '\'' +
					", paramPositions=" + paramPositions +
					'}';
		}
	}

}
