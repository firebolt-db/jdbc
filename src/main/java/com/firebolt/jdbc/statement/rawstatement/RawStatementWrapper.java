package com.firebolt.jdbc.statement.rawstatement;

import lombok.Value;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

@Value
public class RawStatementWrapper {

	List<RawStatement> subStatements;

	long totalParams;

	public RawStatementWrapper(List<RawStatement> subStatements) {
		this.subStatements = subStatements;
		this.totalParams = subStatements.stream().map(RawStatement::getParamMarkers).mapToLong(Collection::size).sum();
	}

	@Override
	public String toString() {
		return "SqlQueryWrapper{" + "subQueries=" + subStatements.stream().map(RawStatement::toString).collect(Collectors.joining("|")) + ", totalParams="
				+ totalParams + '}';
	}

}
