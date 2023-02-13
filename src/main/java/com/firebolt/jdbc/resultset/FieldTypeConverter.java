package com.firebolt.jdbc.resultset;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import com.firebolt.jdbc.CheckedTriFunction;
import com.firebolt.jdbc.exception.FireboltException;
import com.firebolt.jdbc.resultset.column.Column;
import com.firebolt.jdbc.type.BaseType;
import com.firebolt.jdbc.type.date.SqlDateUtil;

import lombok.experimental.UtilityClass;

@UtilityClass
public class FieldTypeConverter {

	private static final Map<Class<?>, CheckedTriFunction<String, BaseType, Column, ?>> javaClassToConvertFunction;

	private static final String CONVERSION_NOT_SUPPORTED_EXCEPTION = "conversion to %s from %s not supported";

	static {
		javaClassToConvertFunction = new HashMap<>();

		javaClassToConvertFunction.put(String.class, (value, columnType, column) -> {
			verify(String.class, columnType, BaseType.STRING);
			return BaseType.STRING.transform(value, column);
		});
		javaClassToConvertFunction.put(Integer.class, (value, columnType, column) -> {
			verify(Integer.class, columnType, BaseType.INTEGER, BaseType.SHORT);
			return BaseType.INTEGER.transform(value, column);
		});
		javaClassToConvertFunction.put(Long.class, (value, columnType, column) -> {
			verify(Long.class, columnType, BaseType.INTEGER, BaseType.SHORT, BaseType.LONG);
			return BaseType.LONG.transform(value, column);
		});
		javaClassToConvertFunction.put(Double.class, (value, columnType, column) -> {
			verify(Double.class, columnType, BaseType.DOUBLE, BaseType.FLOAT);
			return BaseType.DOUBLE.transform(value, column);
		});
		javaClassToConvertFunction.put(Boolean.class, (value, columnType, column) -> {
			verify(Boolean.class, columnType, BaseType.BOOLEAN);
			return BaseType.BOOLEAN.transform(value, column);
		});
		javaClassToConvertFunction.put(Short.class, (value, columnType, column) -> {
			verify(Short.class, columnType, BaseType.SHORT);
			return BaseType.SHORT.transform(value, column);
		});
		javaClassToConvertFunction.put(BigInteger.class, (value, columnType, column) -> {
			verify(BigInteger.class, columnType, BaseType.INTEGER, BaseType.SHORT, BaseType.LONG, BaseType.BIGINT);
			return BaseType.BIGINT.transform(value, column);
		});
		javaClassToConvertFunction.put(Float.class, (value, columnType, column) -> {
			verify(Float.class, columnType, BaseType.FLOAT);
			return BaseType.FLOAT.transform(value, column);
		});
		javaClassToConvertFunction.put(BigDecimal.class, (value, columnType, column) -> {
			verify(BigDecimal.class, columnType, BaseType.BIGINT, BaseType.DECIMAL, BaseType.INTEGER, BaseType.FLOAT,
					BaseType.DOUBLE);
			return BaseType.DECIMAL.transform(value, column);
		});
		javaClassToConvertFunction.put(Date.class, (value, columnType, column) -> {
			verify(Date.class, columnType, BaseType.DATE);
			return BaseType.DATE.transform(value, column);
		});
		javaClassToConvertFunction.put(Time.class, (value, columnType, column) -> {
			verify(Time.class, columnType, BaseType.TIME);
			return BaseType.TIME.transform(value, column);
		});
		javaClassToConvertFunction.put(Timestamp.class, (value, columnType, column) -> {
			verify(Timestamp.class, columnType, BaseType.TIMESTAMP, BaseType.TIMESTAMP_WITH_TIMEZONE);
			return BaseType.TIMESTAMP.transform(value, column);
		});
		javaClassToConvertFunction.put(Array.class, (value, columnType, column) -> {
			verify(Array.class, columnType, BaseType.ARRAY);
			return BaseType.ARRAY.transform(value, column);
		});
		javaClassToConvertFunction.put(OffsetDateTime.class, (value, columnType, column) -> {
			verify(OffsetDateTime.class, columnType, BaseType.DATE, BaseType.TIMESTAMP,
					BaseType.TIMESTAMP_WITH_TIMEZONE);
			Timestamp ts = BaseType.TIMESTAMP.transform(value, column);
			return SqlDateUtil.transformFromTimestampToOffsetDateTime.apply(ts);
		});
		javaClassToConvertFunction.put(Object.class, (value, columnType, column) -> {
			verify(Object.class, columnType, BaseType.OBJECT, BaseType.NULL);
			return BaseType.OBJECT.transform(value, column);
		});
		javaClassToConvertFunction.put(byte[].class, (value, columnType, column) -> {
			if (columnType == BaseType.BYTEA) {
				verify(byte[].class, columnType, BaseType.BYTEA);
				return BaseType.BYTEA.transform(value, column);
			} else {
				return Optional.ofNullable(value).map(v -> BaseType.isNull(v) ? null : v).map(String::getBytes)
						.orElse(null);
			}
		});
	}

	private static <T> void verify(Class<T> toType, BaseType columnBaseType, BaseType... supportedTypes)
			throws FireboltException {
		if (Arrays.stream(supportedTypes).noneMatch(b -> b.equals(columnBaseType))) {
			throw new FireboltException(
					String.format(CONVERSION_NOT_SUPPORTED_EXCEPTION, toType, columnBaseType.getType().getName()));
		}
	}

	static <T> T convert(Class<T> type, String value, BaseType columnType, Column column) throws SQLException {
		if (!javaClassToConvertFunction.containsKey(type)) {
			throw new FireboltException(
					String.format(CONVERSION_NOT_SUPPORTED_EXCEPTION, type.getName(), columnType.getType().getName()));
		}
		return (T) javaClassToConvertFunction.get(type).apply(value, columnType, column);
	}
}
