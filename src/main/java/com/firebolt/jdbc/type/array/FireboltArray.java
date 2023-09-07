package com.firebolt.jdbc.type.array;

import com.firebolt.jdbc.QueryResult;
import com.firebolt.jdbc.exception.FireboltException;
import com.firebolt.jdbc.resultset.FireboltResultSet;
import com.firebolt.jdbc.type.FireboltDataType;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static com.firebolt.jdbc.type.FireboltDataType.INTEGER;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

public class FireboltArray implements Array {

	private final FireboltDataType type;
	private Object array;
	private final List<QueryResult.Column> columns;

	public FireboltArray(FireboltDataType type, Object array) {
		this.type = type;
		this.array = array;
		columns = Arrays.asList(
				QueryResult.Column.builder().name("INDEX").type(INTEGER).build(),
				QueryResult.Column.builder().name("VALUE").type(type).build());
	}

	@Override
	public String getBaseTypeName() {
		return type.getDisplayName();
	}

	@Override
	public int getBaseType() {
		return type.getSqlType();
	}

	@Override
	public Object getArray() throws SQLException {
		if (array == null) {
			throw new SQLException("Cannot call method getArray() after calling free()");
		}
		return array;
	}

	@Override
	public Object getArray(Map<String, Class<?>> map) throws SQLException {
		return getArray(1, Integer.MAX_VALUE, map);
	}

	@Override
	public void free() {
		array = null;
	}

	@Override
	public Object getArray(long index, int count) throws SQLException {
		return getArray(index, count, null);
	}

	@Override
	public Object getArray(long index, int count, Map<String, Class<?>> map) throws SQLException {
		if (array == null) {
			throw new SQLException("Cannot call method getArray() after calling free()");
		}
		if (map != null && !map.isEmpty()) {
			throw new SQLFeatureNotSupportedException("Maps are not supported with Arrays");
		}
		if (index < 1) {
			throw new FireboltException(format("The array index is out of range: %d", index));
		}
		int length = java.lang.reflect.Array.getLength(array);
		int from = (int)(index - 1);
		int to = Math.min(from + count, length);
		int maxCount = length - from;
		return index == 1 && count >= maxCount ? array : Arrays.copyOfRange((Object[])array, from, to);
	}

	@Override
	public ResultSet getResultSet() throws SQLException {
		return getResultSet(null);
	}

	@Override
	public ResultSet getResultSet(Map<String, Class<?>> map) throws SQLException {
		return getResultSet(1, Integer.MAX_VALUE, map);
	}

	@Override
	public ResultSet getResultSet(long index, int count) throws SQLException {
		return getResultSet(index, count, null);
	}

	@Override
	public ResultSet getResultSet(long index, int count, Map<String, Class<?>> map) throws SQLException {
		Object[] arr = (Object[])getArray(index, count, map);
		List<List<?>> data = IntStream.range(0, arr.length).mapToObj(i -> List.of(i + 1, arr[i])).collect(toList());
		return FireboltResultSet.of(QueryResult.builder().columns(columns).rows(data).build());
	}
}
