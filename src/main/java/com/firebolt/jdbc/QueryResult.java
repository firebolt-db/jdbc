package com.firebolt.jdbc;

import com.firebolt.jdbc.type.FireboltDataType;
import lombok.Builder;
import lombok.Value;
import org.json.JSONArray;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static java.util.stream.Collectors.toList;

/**
 * Class containing a query result that can be used to create a
 * {@link com.firebolt.jdbc.resultset.FireboltResultSet}
 * It is particularly useful for metadata methods as a ResulSet containing metadata info must be returned.
 */
@Builder
@Value
public class QueryResult {

	private static final String TAB = "\t";
	private static final String NEXT_LINE = "\n";
	private static final String START_JSON = "{\"message_type\":\"START\",\"result_columns\":%s}\n";
	private static final String DATA_JSON = "{\"message_type\":\"DATA\",\"data\":[%s]}\n";
	private static final String FINISH_JSON = "{\"message_type\":\"FINISH_SUCCESSFULLY\"}";
	String databaseName;
	String tableName;
	@Builder.Default
	List<Column> columns = new ArrayList<>();
	@Builder.Default
	List<List<?>> rows = new ArrayList<>();

	/**
	 * @return the string representing a query response in the
	 *         TabSeparatedWithNamesAndTypes format
	 */
	@Override
	@SuppressWarnings("java:S6204") // JDK 11 compatible
	public String toString() {
		StringBuilder stringBuilder = new StringBuilder();
		JSONArray columnsArray = new JSONArray();
		columns.forEach(column -> columnsArray.put(Map.of(
				"name", column.getName(),
				"type", column.getType().getAliases()[0] + (column.isNullable() ? " null" : "")
		)));
		stringBuilder.append(String.format(START_JSON, columnsArray));

		for (int i = 0; i < rows.size(); i++) {
			stringBuilder.append(String.format(DATA_JSON, rows.get(i)));
		}
		stringBuilder.append(FINISH_JSON);
		return stringBuilder.toString();
	}

	private void appendWithListValues(StringBuilder destination, List<?> values) {
		Iterator<?> iterator = values.iterator();
		while (iterator.hasNext()) {
			Object value = iterator.next();
			if (null == value) {
				value = "\\N"; // null
			}
			if (value instanceof Boolean) {
				if (Boolean.TRUE.equals(value)) {
					destination.append('t');
				} else {
					destination.append('f');
				}
			} else {
				destination.append(value);
			}
			if (iterator.hasNext()) {
				destination.append(TAB);
			}
		}
	}

	@Builder
	@Value
	public static class Column {
		String name;
		FireboltDataType type;
		boolean nullable;
	}
}
