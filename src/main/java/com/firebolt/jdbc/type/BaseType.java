package com.firebolt.jdbc.type;

import com.firebolt.jdbc.CheckedFunction;
import com.firebolt.jdbc.exception.FireboltException;
import com.firebolt.jdbc.resultset.column.Column;
import com.firebolt.jdbc.type.array.SqlArrayUtil;
import com.firebolt.jdbc.type.date.SqlDateUtil;
import lombok.Builder;
import lombok.CustomLog;
import lombok.Value;
import org.apache.commons.text.StringEscapeUtils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Array;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.TimeZone;

import static com.firebolt.jdbc.type.array.SqlArrayUtil.BYTE_ARRAY_PREFIX;
import static com.firebolt.jdbc.util.ByteArrayUtil.hexStringToByteArray;

/** This class contains the java types the Firebolt datatypes are mapped to */
@CustomLog
public enum BaseType {
	LONG(Long.class, conversion -> Long.parseLong(checkInfinity(conversion.getValue()))),
	INTEGER(Integer.class, conversion -> Integer.parseInt(checkInfinity(conversion.getValue()))),
	SHORT(Short.class, conversion -> Short.parseShort(checkInfinity(conversion.getValue()))),
	BIGINT(BigInteger.class, conversion -> new BigInteger(checkInfinity(conversion.getValue()))),
	TEXT(String.class, conversion -> {
		String escaped = StringEscapeUtils.unescapeJava(conversion.getValue());
		int limit = conversion.getMaxFieldSize();
		return limit > 0 && limit <= escaped.length() ? escaped.substring(0, limit) : escaped;
	}),
	REAL(Float.class, conversion -> {
		if (isNan(conversion.getValue())) {
			return Float.NaN;
		} else if (isPositiveInf(conversion.getValue())) {
			return Float.POSITIVE_INFINITY;
		} else if (isNegativeInf(conversion.getValue())) {
			return Float.NEGATIVE_INFINITY;
		} else {
			return Float.parseFloat(conversion.getValue());
		}
	}), DOUBLE(Double.class, conversion -> {
		if (isNan(conversion.getValue())) {
			return Double.NaN;
		} else if (isPositiveInf(conversion.getValue())) {
			return Double.POSITIVE_INFINITY;
		} else if (isNegativeInf(conversion.getValue())) {
			return Double.NEGATIVE_INFINITY;
		} else {
			return Double.parseDouble(conversion.getValue());
		}
	}),
	DATE(Date.class,
			conversion -> SqlDateUtil.transformToDateFunction.apply(conversion.getValue(), conversion.getTimeZone())),
	TIMESTAMP(Timestamp.class,
			conversion -> SqlDateUtil.transformToTimestampFunction.apply(conversion.getValue(),
					conversion.getTimeZone())),
	TIMESTAMP_WITH_TIMEZONE(Timestamp.class,
			conversion -> SqlDateUtil.transformToTimestampFunction.apply(conversion.getValue(),
					conversion.getTimeZone())),
	TIME(Time.class,
			conversion -> SqlDateUtil.transformToTimeFunction.apply(conversion.getValue(), conversion.getTimeZone())),
	NULL(Object.class, conversion -> null), OTHER(String.class, conversion -> "Unknown"),
	OBJECT(Object.class, StringToColumnTypeConversion::getValue),
	NUMERIC(BigDecimal.class, conversion -> new BigDecimal(conversion.getValue())),
	BOOLEAN(Boolean.class, conversion -> {
		String value = conversion.getValue();
		if ("0".equalsIgnoreCase(value) || "f".equalsIgnoreCase(value)) {
			return false;
		} else if ("1".equalsIgnoreCase(value) || "t".equalsIgnoreCase(value)) {
			return true;
		}
		throw new FireboltException(String.format("Cannot cast %s to type boolean", conversion.getValue()));
	}), ARRAY(Array.class,
			conversion -> SqlArrayUtil.transformToSqlArray(conversion.getValue(), conversion.getColumn().getType())),
	BYTEA(byte[].class, conversion -> {
		String s = conversion.getValue();
		if (s == null || s.isEmpty()) {
			return new byte[] {};
		}
		if (s.startsWith(BYTE_ARRAY_PREFIX)) {
			byte[] bytes = hexStringToByteArray(s.substring(2));
			int limit = conversion.getMaxFieldSize();
			return limit > 0 && limit <= bytes.length ? Arrays.copyOf(bytes, limit) : bytes;
		}
		// Cannot convert from other formats (such as 'Escape') for the moment
		throw new FireboltException("Cannot convert binary string in non-hex format to byte array");
	});

	public static final String NULL_VALUE = "\\N";
	private final Class<?> type;
	private final CheckedFunction<StringToColumnTypeConversion, Object> transformFunction;

	BaseType(Class<?> type, CheckedFunction<StringToColumnTypeConversion, Object> transformFunction) {
		this.type = type;
		this.transformFunction = transformFunction;
	}

	private static boolean isPositiveInf(String value) {
		return "inf".equalsIgnoreCase(value) || "+inf".equalsIgnoreCase(value);
	}

	private static boolean isNegativeInf(String value) {
		return "-inf".equalsIgnoreCase(value);
	}

	private static String checkInfinity(String s) {
		if (isPositiveInf(s) || isNegativeInf(s)) {
			throw new IllegalArgumentException("Integer does not support infinity");
		}
		return s;
	}

	public static boolean isNull(String value) {
		return NULL_VALUE.equalsIgnoreCase(value);
	}

	private static boolean isNan(String value) {
		return "nan".equalsIgnoreCase(value) || "+nan".equalsIgnoreCase(value) || "-nan".equalsIgnoreCase(value);
	}

	private static void validateObjectNotNull(String value) {
		if (value == null) {
			throw new IllegalArgumentException("The value cannot be null");
		}
	}

	public Class<?> getType() {
		return type;
	}

	public <T> T transform(String value, Column column) throws SQLException {
		return transform(value, column, null, 0);
	}

	public <T> T transform(String value) throws SQLException {
		return transform(value, null, null, 0);
	}

	public <T> T transform(String value, Column column, TimeZone timeZone, int maxFieldSize) throws SQLException {
		TimeZone fromTimeZone;
		if (column != null && column.getType().getTimeZone() != null) {
			fromTimeZone = column.getType().getTimeZone();
		} else {
			fromTimeZone = timeZone;
		}
		StringToColumnTypeConversion conversion = StringToColumnTypeConversion.builder().value(value).column(column)
				.timeZone(fromTimeZone).maxFieldSize(maxFieldSize).build();
		return transform(conversion);
	}

	private <T> T transform(StringToColumnTypeConversion conversion) throws SQLException {
		validateObjectNotNull(conversion.getValue());
		if (isNull(conversion.getValue())) {
			return null;
		}
		//noinspection unchecked
		return (T) transformFunction.apply(conversion);
	}

	@Builder
	@Value
	private static class StringToColumnTypeConversion {
		String value;
		Column column;
		TimeZone timeZone;
		int maxFieldSize;
	}
}
