package com.firebolt.jdbc.type;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;
import java.util.TimeZone;

import javax.xml.bind.DatatypeConverter;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.StringEscapeUtils;

import com.firebolt.jdbc.exception.FireboltException;
import com.firebolt.jdbc.resultset.column.Column;
import com.firebolt.jdbc.type.array.SqlArrayUtil;
import com.firebolt.jdbc.type.date.SqlDateUtil;

import lombok.Builder;
import lombok.CustomLog;
import lombok.Value;

/** This class contains the java types the Firebolt datatypes are mapped to */
@CustomLog
public enum BaseType {
	LONG(Long.class, conversion -> Long.parseLong(conversion.getValue())),
	INTEGER(Integer.class, conversion -> Integer.parseInt(conversion.getValue())),
	SHORT(Short.class, conversion -> Short.parseShort(conversion.getValue())),
	BIGINT(BigInteger.class, conversion -> new BigInteger(conversion.getValue())),
	STRING(String.class, conversion -> StringEscapeUtils.unescapeJava(conversion.getValue())),
	FLOAT(Float.class, conversion -> {
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
	DECIMAL(BigDecimal.class, conversion -> new BigDecimal(conversion.getValue())),
	BOOLEAN(Boolean.class, conversion -> {
			if (StringUtils.equalsAnyIgnoreCase(conversion.getValue(), "0", "f")) {
				return false;
			} else if (StringUtils.equalsAnyIgnoreCase(conversion.getValue(), "1", "t")) {
				return true;
			}
		throw new FireboltException(String.format("Cannot cast %s to type boolean", conversion.getValue()));
	}), ARRAY(Array.class,
			conversion -> SqlArrayUtil.transformToSqlArray(conversion.getValue(), conversion.getColumn().getType())),
	BYTEA(byte[].class, conversion -> {
		String s = conversion.getValue();
		if (StringUtils.startsWith(s, "\\x")) {
			return DatatypeConverter.parseHexBinary(s.substring(2));
		} else if (StringUtils.isEmpty(s)) {
			return new byte[] {};
		} else {
			// Cannot convert from other formats (such as 'Escape') for the moment
			throw new FireboltException("Cannot convert binary string in non-hex format to byte array");
		}
	});

	public static final String NULL_VALUE = "\\N";
	private final Class<?> type;
	private final CheckedFunction<StringToColumnTypeConversion, Object> transformFunction;

	BaseType(Class<?> type, CheckedFunction<StringToColumnTypeConversion, Object> transformFunction) {
		this.type = type;
		this.transformFunction = transformFunction;
	}

	private static boolean isPositiveInf(String value) {
		return StringUtils.equalsAnyIgnoreCase(value, "+inf", "inf");
	}

	private static boolean isNegativeInf(String value) {
		return StringUtils.equals(value, "-inf");
	}

	public static boolean isNull(String value) {
		return StringUtils.equalsIgnoreCase(value, NULL_VALUE);
	}

	private static boolean isNan(String value) {
		return StringUtils.equalsIgnoreCase(value, "nan");
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
		return this.transform(value, column, null);
	}

	public <T> T transform(String value) throws SQLException {
		return this.transform(value, null, null);
	}

	public <T> T transform(String value, Column column, TimeZone timeZone) throws SQLException {
		TimeZone fromTimeZone;
		if (column != null && column.getType().getTimeZone() != null) {
			fromTimeZone = column.getType().getTimeZone();
		} else {
			fromTimeZone = timeZone;
		}
		StringToColumnTypeConversion conversion = StringToColumnTypeConversion.builder().value(value).column(column)
				.timeZone(fromTimeZone).build();
		return this.transform(conversion);
	}

	private <T> T transform(StringToColumnTypeConversion conversion) throws SQLException {
		validateObjectNotNull(conversion.getValue());
		if (isNull(conversion.getValue())) {
			return null;
		}
		return (T) transformFunction.apply(conversion);
	}

	@Builder
	@Value
	private static class StringToColumnTypeConversion {
		String value;
		Column column;
		TimeZone timeZone;
	}
}
