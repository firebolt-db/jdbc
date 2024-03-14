package com.firebolt.jdbc.type;

import com.firebolt.jdbc.CheckedBiFunction;
import com.firebolt.jdbc.CheckedFunction;
import com.firebolt.jdbc.exception.FireboltException;
import com.firebolt.jdbc.type.array.SqlArrayUtil;
import com.firebolt.jdbc.type.date.SqlDateUtil;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Array;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.UUID;

import static com.firebolt.jdbc.exception.ExceptionType.TYPE_NOT_SUPPORTED;
import static com.firebolt.jdbc.exception.ExceptionType.TYPE_TRANSFORMATION_ERROR;

public enum JavaTypeToFireboltSQLString {
	BOOLEAN(Boolean.class, value -> Boolean.TRUE.equals(value) ? "1" : "0"),
	UUID(java.util.UUID.class, value -> ((UUID) value).toString()),
	SHORT(Short.class, value -> Short.toString((short) value)),
	STRING(String.class, getSQLStringValueOfString()),
	LONG(Long.class, String::valueOf),
	INTEGER(Integer.class, String::valueOf),
	BIG_INTEGER(BigInteger.class, String::valueOf),
	FLOAT(Float.class, String::valueOf),
	DOUBLE(Double.class, String::valueOf),
	DATE(Date.class, date -> SqlDateUtil.transformFromDateToSQLStringFunction.apply((Date) date)),
	TIMESTAMP(Timestamp.class, time -> SqlDateUtil.transformFromTimestampToSQLStringFunction.apply((Timestamp) time)),
	BIG_DECIMAL(BigDecimal.class, value -> value == null ? BaseType.NULL_VALUE : ((BigDecimal) value).toPlainString()),
	ARRAY(Array.class, SqlArrayUtil::arrayToString),
	BYTE_ARRAY(byte[].class, (value, separateEachByte) -> Optional.ofNullable(SqlArrayUtil.byteArrayToHexString((byte[])value, separateEachByte)).map(x  -> String.format("'%s'::BYTEA", x)).orElse(null))
	;

	private static final List<Entry<String, String>> characterToEscapedCharacterPairs = List.of(
			Map.entry("\0", "\\0"), Map.entry("\\", "\\\\"), Map.entry("'", "''"));
	private final Class<?> sourceType;
	private final CheckedBiFunction<Object, Boolean, String> transformToJavaTypeFunction;
	public static final String NULL_VALUE = "NULL";

	JavaTypeToFireboltSQLString(Class<?> sourceType, CheckedFunction<Object, String> transformToSqlStringFunction) {
		this(sourceType, (o, flag) -> transformToSqlStringFunction.apply(o));
	}

	JavaTypeToFireboltSQLString(Class<?> sourceType, CheckedBiFunction<Object, Boolean, String> transformToSqlStringFunction) {
		this.sourceType = sourceType;
		this.transformToJavaTypeFunction = transformToSqlStringFunction;
	}

	public static String transformAny(Object object) throws FireboltException {
		return transformAny(object, false);
	}

	public static String transformAny(Object object, boolean flag) throws FireboltException {
		Class<?> objectType;
		if (object == null) {
			return NULL_VALUE;
		} else if (object.getClass().isArray() && !byte[].class.equals(object.getClass())) {
			objectType = Array.class;
		} else {
			objectType = object.getClass();
		}
		JavaTypeToFireboltSQLString converter = Arrays.stream(JavaTypeToFireboltSQLString.values())
				.filter(c -> c.getSourceType().equals(objectType)).findAny()
				.orElseThrow(() -> new FireboltException(
						String.format("Cannot convert type %s. The type is not supported.", objectType),
						TYPE_NOT_SUPPORTED));
		return converter.transform(object, flag);
	}

	private static CheckedFunction<Object, String> getSQLStringValueOfString() {
		return value -> {
			String escaped = (String) value;
			for (Entry<String, String> specialCharacter : characterToEscapedCharacterPairs) {
				escaped = escaped.replace(specialCharacter.getKey(), specialCharacter.getValue());
			}
			return String.format("'%s'", escaped);
		};
	}

	public Class<?> getSourceType() {
		return sourceType;
	}

	public String transform(Object object) throws FireboltException {
		return transform(object, false);
	}

	public String transform(Object object, boolean transformAny) throws FireboltException {
		if (object == null) {
			return NULL_VALUE;
		} else {
			try {
				return transformToJavaTypeFunction.apply(object, transformAny);
			} catch (Exception e) {
				throw new FireboltException("Could not convert object to a String ", e, TYPE_TRANSFORMATION_ERROR);
			}
		}
	}
}
