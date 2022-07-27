package com.firebolt.jdbc.type.array;

import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Map;

import com.firebolt.jdbc.type.FireboltDataType;

import lombok.Builder;

@Builder
public class FireboltArray extends AbstractArray {

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
}
