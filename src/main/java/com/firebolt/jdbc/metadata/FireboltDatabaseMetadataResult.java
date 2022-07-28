package com.firebolt.jdbc.metadata;

import java.io.ByteArrayInputStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import com.firebolt.jdbc.resultset.FireboltResultSet;
import com.firebolt.jdbc.type.FireboltDataType;

import lombok.Builder;
import lombok.Value;

@Builder
@Value
public class FireboltDatabaseMetadataResult {

	private static final String TAB = "\t";
	private static final String NEXT_LINE = "\n";
	@Builder.Default
	List<Column> columns = new ArrayList<>();
	@Builder.Default
	List<List<?>> rows = new ArrayList<>();

	@Override
	public String toString() {
		StringBuilder stringBuilder = new StringBuilder();
		this.appendWithListValues(stringBuilder, columns.stream().map(Column::getName).collect(Collectors.toList()));
		stringBuilder.append(NEXT_LINE);
		this.appendWithListValues(stringBuilder, columns.stream().map(Column::getType)
				.map(FireboltDataType::getDisplayName).collect(Collectors.toList()));
		stringBuilder.append(NEXT_LINE);

		for (int i = 0; i < rows.size(); i++) {
			appendWithListValues(stringBuilder, rows.get(i));
			if (i != rows.size() - 1) {
				stringBuilder.append(NEXT_LINE);
			}
		}
		return stringBuilder.toString();
	}

	public StringBuilder appendWithListValues(StringBuilder destination, List<?> values) {
		Iterator<?> iterator = values.iterator();
		while (iterator.hasNext()) {
			Object value = iterator.next();
			if (null == value) {
				value = "\\N"; // null
			}
			destination.append(value);
			if (iterator.hasNext()) {
				destination.append(TAB);
			}
		}
		return destination;
	}

	public ResultSet toResultSet() throws SQLException {
		return new FireboltResultSet(new ByteArrayInputStream(this.toString().getBytes()));
	}

	@Builder
	@Value
	public static class Column {
		String name;
		FireboltDataType type;
	}
}
