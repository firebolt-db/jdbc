package com.firebolt.jdbc.type;

import static com.firebolt.jdbc.exception.ExceptionType.TYPE_NOT_SUPPORTED;
import static com.firebolt.jdbc.exception.ExceptionType.TYPE_TRANSFORMATION_ERROR;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Array;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import com.firebolt.jdbc.CheckedFunction;
import com.firebolt.jdbc.exception.FireboltException;
import com.firebolt.jdbc.type.array.SqlArrayUtil;
import com.firebolt.jdbc.type.date.SqlDateUtil;

public enum JavaTypeToFireboltSQLString {
	BOOLEAN(Boolean.class, value -> Boolean.TRUE.equals(value) ? "1" : "0"),
	UUID(java.util.UUID.class, value -> ((UUID) value).toString()),
	SHORT(Short.class, value -> Short.toString((short) value)), STRING(String.class, getSQLStringValueOfString()),
	LONG(Long.class, String::valueOf), INTEGER(Integer.class, String::valueOf),
	BIG_INTEGER(BigInteger.class, String::valueOf), FLOAT(Float.class, String::valueOf),
	DOUBLE(Double.class, String::valueOf),
	DATE(Date.class, date -> SqlDateUtil.transformFromDateToSQLStringFunction.apply((Date) date)),
	TIMESTAMP(Timestamp.class, time -> SqlDateUtil.transformFromTimestampToSQLStringFunction.apply((Timestamp) time)),
	BIG_DECIMAL(BigDecimal.class, value -> value == null ? BaseType.NULL_VALUE : ((BigDecimal) value).toPlainString()),
	ARRAY(Array.class, SqlArrayUtil::arrayToString);

	private static final Pair<String[], String[]> characterToEscapedCharacterPair = new ImmutablePair<>(
			new String[] { "\0", "\\", "'" }, new String[] { "\\0", "\\\\", "\\'" });
	private final Class<?> sourceType;
	private final CheckedFunction<Object, String> transformToJavaTypeFunction;

	JavaTypeToFireboltSQLString(Class<?> sourceType, CheckedFunction<Object, String> transformToSqlStringFunction) {
		this.sourceType = sourceType;
		this.transformToJavaTypeFunction = transformToSqlStringFunction;
	}

	public static String transformAny(Object object) throws FireboltException {
		Class<?> objectType;
		if (object == null) {
			return BaseType.NULL_VALUE;
		} else if (object.getClass().isArray()) {
			objectType = Array.class;
		} else {
			objectType = object.getClass();
		}
		JavaTypeToFireboltSQLString converter = Arrays.stream(JavaTypeToFireboltSQLString.values())
				.filter(c -> c.getSourceType().equals(objectType)).findAny()
				.orElseThrow(() -> new FireboltException(
						String.format("Cannot convert type %s. The type is not supported.", objectType),
						TYPE_NOT_SUPPORTED));
		return converter.transform(object);
	}

	private static CheckedFunction<Object, String> getSQLStringValueOfString() {
		return value -> {
			String escaped = StringUtils.replaceEach((String) value, characterToEscapedCharacterPair.getLeft(),
					characterToEscapedCharacterPair.getRight());
			return String.format("'%s'", escaped);
		};
	}

	public Class<?> getSourceType() {
		return sourceType;
	}

	public String transform(Object object) throws FireboltException {
		if (object == null) {
			return BaseType.NULL_VALUE;
		} else {
			try {
				return this.transformToJavaTypeFunction.apply(object);
			} catch (Exception e) {
				throw new FireboltException("Could not convert object to a String ", e, TYPE_TRANSFORMATION_ERROR);
			}
		}
	}
}
