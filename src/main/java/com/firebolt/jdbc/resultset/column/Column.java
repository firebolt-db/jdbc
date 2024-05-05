package com.firebolt.jdbc.resultset.column;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import java.util.logging.Level;
import java.util.logging.Logger;

@Builder
@Getter
@EqualsAndHashCode
@ToString
public final class Column {
	private static final Logger log = Logger.getLogger(Column.class.getName());
	private final ColumnType type;
	private final String columnName;

	public static Column of(String columnType, String columnName) {
		log.log(Level.FINE, "Creating column info for column: {0} of type: {1}", new Object[] {columnName, columnType});
		return Column.builder().columnName(columnName).type(ColumnType.of(columnType)).build();
	}
}
