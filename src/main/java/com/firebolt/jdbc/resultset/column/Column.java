package com.firebolt.jdbc.resultset.column;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Builder
@Getter
@EqualsAndHashCode
@Slf4j
public final class Column {

	private final ColumnType type;
	private final String columnName;

	public static Column of(String columnType, String columnName) {
		log.debug("Creating column info for column: {} of type: {}", columnName, columnType);
		return Column.builder().columnName(columnName).type(ColumnType.of(columnType)).build();
	}
}
