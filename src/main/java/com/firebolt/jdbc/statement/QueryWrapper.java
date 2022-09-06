package com.firebolt.jdbc.statement;

import java.util.List;

import org.apache.commons.lang3.tuple.Pair;

import lombok.Getter;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Getter
public class QueryWrapper {
	public QueryWrapper(List<SubQuery> subQueries, int totalParams) {
		this.subQueries = subQueries;
        this.totalParams = totalParams;
	}

	List<SubQuery> subQueries;

    int totalParams;

	@Value
	@Getter
	static class SubQuery {

		public SubQuery(String sql, List<Pair<Integer, Integer>> paramPositions) {
			this.paramPositions = paramPositions;
			this.sql = sql;
		}

		List<Pair<Integer, Integer>> paramPositions;
		String sql;

	}

}
