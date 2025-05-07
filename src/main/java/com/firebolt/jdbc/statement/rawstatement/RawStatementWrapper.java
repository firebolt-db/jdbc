package com.firebolt.jdbc.statement.rawstatement;

import com.firebolt.jdbc.statement.ParamMarker;
import lombok.CustomLog;
import lombok.Value;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

@CustomLog
@Value
public class RawStatementWrapper {

	List<RawStatement> subStatements;
	List<Integer> subStatementParamMarkersIndices;

	long totalParams;

	public RawStatementWrapper(List<RawStatement> subStatements) {
		this.subStatements = subStatements;
		this.subStatementParamMarkersIndices = subStatements.stream()
				.flatMap(subStatement -> subStatement.getParamMarkers().stream())
				.map(ParamMarker::getId)
				.distinct()
				.collect(Collectors.toList());
		this.totalParams = subStatements.stream().map(RawStatement::getParamMarkers).mapToLong(Collection::size).sum();
	}

	@Override
	public String toString() {
		return "SqlQueryWrapper{" + "subQueries=" + subStatements.stream().map(RawStatement::toString).collect(Collectors.joining("|")) + ", totalParams="
				+ totalParams + '}';
	}

}
