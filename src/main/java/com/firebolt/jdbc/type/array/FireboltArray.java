package com.firebolt.jdbc.type.array;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Map;

import com.firebolt.jdbc.annotation.ExcludeFromJacocoGeneratedReport;
import com.firebolt.jdbc.annotation.NotImplemented;
import com.firebolt.jdbc.exception.FireboltSQLFeatureNotSupportedException;
import com.firebolt.jdbc.type.FireboltDataType;

import lombok.Builder;

@Builder
public class FireboltArray implements Array {

	private final FireboltDataType type;
	private Object array;

	@Override
	public String getBaseTypeName() {
		return type.getInternalName();
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
		if (map != null && !map.isEmpty()) {
			throw new SQLFeatureNotSupportedException("Maps are not supported with Arrays");
		}
		return getArray();
	}

	@Override
	public void free() {
		array = null;
	}

	/**
	 * @hidden
	 */
	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public Object getArray(long index, int count) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	/**
	 * @hidden
	 */
	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public Object getArray(long index, int count, Map<String, Class<?>> map) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	/**
	 * @hidden
	 */
	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public ResultSet getResultSet() throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	/**
	 * @hidden
	 */
	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public ResultSet getResultSet(Map<String, Class<?>> map) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	/**
	 * @hidden
	 */
	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public ResultSet getResultSet(long index, int count) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	/**
	 * @hidden
	 */
	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public ResultSet getResultSet(long index, int count, Map<String, Class<?>> map) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}
}
