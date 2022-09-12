package com.firebolt.jdbc.statement.rawstatement;

import java.util.Collection;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Data
public class RawStatementWrapper {

	List<RawStatement> subStatements;

	long totalParams;

	@Override
	public String toString() {
		return "SqlQueryWrapper{" +
				"subQueries=" + StringUtils.join(subStatements, "|") +
				", totalParams=" + totalParams +
				'}';
	}

	public RawStatementWrapper(List<RawStatement> subStatements) {
		this.subStatements = subStatements;
		this.totalParams = subStatements.stream()
				.map(RawStatement::getParamMarkers)
				.mapToLong(Collection::size).sum();
	}

}
