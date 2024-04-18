package com.firebolt.jdbc.type;

import com.firebolt.jdbc.CheckedBiFunction;
import com.firebolt.jdbc.CheckedFunction;
import com.firebolt.jdbc.exception.FireboltException;
import com.firebolt.jdbc.type.array.SqlArrayUtil;
import com.firebolt.jdbc.type.date.SqlDateUtil;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.JDBCType;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.TimeZone;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static com.firebolt.jdbc.exception.ExceptionType.TYPE_NOT_SUPPORTED;
import static com.firebolt.jdbc.exception.ExceptionType.TYPE_TRANSFORMATION_ERROR;
import static com.firebolt.jdbc.type.array.SqlArrayUtil.byteArrayToHexString;
import static java.lang.String.format;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toMap;

public enum JavaTypeToFireboltSQLString {
	BOOLEAN(Boolean.class, value -> Boolean.TRUE.equals(value) ? "1" : "0"),
	UUID(java.util.UUID.class, Object::toString),
	BYTE(Byte.class, value -> Byte.toString(((Number) value).byteValue())),
	SHORT(Short.class, value -> Short.toString(((Number) value).shortValue())),
	STRING(String.class, getSQLStringValueOfString()),
	LONG(Long.class, value -> Long.toString(((Number)value).longValue())),
	INTEGER(Integer.class, value -> Integer.toString(((Number)value).intValue())),
	BIG_INTEGER(BigInteger.class, value -> value instanceof BigInteger ? value.toString() : Long.toString(((Number)value).longValue())),
	FLOAT(Float.class, value -> Float.toString(((Number)value).floatValue())),
	DOUBLE(Double.class, value -> Double.toString(((Number)value).doubleValue())),
	DATE(Date.class, date -> SqlDateUtil.transformFromDateToSQLStringFunction.apply((Date) date), (date, tz) -> SqlDateUtil.transformFromDateWithTimezoneToSQLStringFunction.apply((Date) date, toTimeZone(tz))),
	TIMESTAMP(Timestamp.class, time -> SqlDateUtil.transformFromTimestampToSQLStringFunction.apply((Timestamp) time), (ts, tz) -> SqlDateUtil.transformFromTimestampWithTimezoneToSQLStringFunction.apply((Timestamp) ts, toTimeZone(tz))),
	BIG_DECIMAL(BigDecimal.class, value -> value == null ? BaseType.NULL_VALUE : ((BigDecimal) value).toPlainString()),
	ARRAY(Array.class, SqlArrayUtil::arrayToString),
	BYTE_ARRAY(byte[].class, value -> ofNullable(byteArrayToHexString((byte[])value, true)).map(x  -> format("E'%s'::BYTEA", x)).orElse(null)),
	;
	private static final List<Entry<String, String>> characterToEscapedCharacterPairs = List.of(
			Map.entry("\0", "\\0"), Map.entry("\\", "\\\\"), Map.entry("'", "''"));
	//https://docs.oracle.com/javase/1.5.0/docs/guide/jdbc/getstart/mapping.html
	private static final Map<JDBCType, Class<?>> jdbcTypeToClass = Map.ofEntries(
			Map.entry(JDBCType.CHAR, String.class),
			Map.entry(JDBCType.VARCHAR, String.class),
			Map.entry(JDBCType.LONGVARCHAR,String.class),
			Map.entry(JDBCType.NUMERIC, java.math.BigDecimal.class),
			Map.entry(JDBCType.DECIMAL, java.math.BigDecimal.class),
			Map.entry(JDBCType.BIT, Boolean.class),
			Map.entry(JDBCType.BOOLEAN, Boolean.class),
			Map.entry(JDBCType.TINYINT, Short.class),
			Map.entry(JDBCType.SMALLINT, Short.class),
			Map.entry(JDBCType.INTEGER, Integer.class),
			Map.entry(JDBCType.BIGINT, Long.class),
			Map.entry(JDBCType.REAL, Float.class),
			Map.entry(JDBCType.FLOAT, Double.class),
			Map.entry(JDBCType.DOUBLE, Double.class),
			Map.entry(JDBCType.BINARY, byte[].class),
			Map.entry(JDBCType.VARBINARY, byte[].class),
			Map.entry(JDBCType.LONGVARBINARY, byte[].class),
			Map.entry(JDBCType.DATE, java.sql.Date.class),
			Map.entry(JDBCType.TIME, java.sql.Time.class),
			Map.entry(JDBCType.TIMESTAMP, java.sql.Timestamp.class),
			//DISTINCT        Object type of underlying type
			Map.entry(JDBCType.CLOB, Clob.class),
			Map.entry(JDBCType.BLOB, Blob.class),
			Map.entry(JDBCType.ARRAY, Array.class)
			//STRUCT,       Struct or SQLData
			//Map.entry(JDBCType.REF, Ref.class)
			//Map.entry(JDBCType.JAVA_OBJECT, Object.class)
	);

	private final Class<?> sourceType;
	private final CheckedFunction<Object, String> transformToJavaTypeFunction;
	private final CheckedBiFunction<Object, Object, String> transformToJavaTypeFunctionWithParameter;
	public static final String NULL_VALUE = "NULL";
	private static final Map<Class<?>, JavaTypeToFireboltSQLString> classToType = Stream.of(JavaTypeToFireboltSQLString.values())
			.collect(toMap(type -> type.sourceType, type -> type));

	JavaTypeToFireboltSQLString(Class<?> sourceType, CheckedFunction<Object, String> transformToSqlStringFunction) {
		this(sourceType, transformToSqlStringFunction, null);
	}

	JavaTypeToFireboltSQLString(Class<?> sourceType,
								CheckedFunction<Object, String> transformToSqlStringFunction,
								CheckedBiFunction<Object, Object, String> transformToJavaTypeFunctionWithParameter) {
		this.sourceType = sourceType;
		this.transformToJavaTypeFunction = transformToSqlStringFunction;
		this.transformToJavaTypeFunctionWithParameter = transformToJavaTypeFunctionWithParameter;
	}

	public static String transformAny(Object object) throws FireboltException {
		return transformAny(object, () -> getType(object));
	}

	public static String transformAny(Object object, int sqlType) throws SQLException {
		return transformAny(object, () -> getType(sqlType));
	}

	private static String transformAny(Object object, Supplier<Class<?>> classSupplier) throws FireboltException {
		return object == null ? NULL_VALUE : transformAny(object, classSupplier.get());
	}

	private static String transformAny(Object object, Class<?> objectType) throws FireboltException {
		JavaTypeToFireboltSQLString converter = Optional.ofNullable(classToType.get(objectType))
				.orElseThrow(() -> new FireboltException(
						format("Cannot convert type %s. The type is not supported.", objectType),
						TYPE_NOT_SUPPORTED));
		return converter.transform(object);
	}

	private static Class<?> getType(Object object) {
		return object.getClass().isArray() && !byte[].class.equals(object.getClass()) ? Array.class : object.getClass();
	}

	private static Class<?> getType(int sqlType) {
		return jdbcTypeToClass.get(JDBCType.valueOf(sqlType));
	}

	private static CheckedFunction<Object, String> getSQLStringValueOfString() {
		return value -> {
			String escaped = (String) value;
			for (Entry<String, String> specialCharacter : characterToEscapedCharacterPairs) {
				escaped = escaped.replace(specialCharacter.getKey(), specialCharacter.getValue());
			}
			return format("'%s'", escaped);
		};
	}

	public Class<?> getSourceType() {
		return sourceType;
	}

	public String transform(Object object, Object ... more) throws FireboltException {
		if (object == null) {
			return NULL_VALUE;
		} else {
			try {
				if (more.length > 0) {
					return transformToJavaTypeFunctionWithParameter.apply(object, more[0]);
				}
				return transformToJavaTypeFunction.apply(object);
			} catch (Exception e) {
				throw new FireboltException("Could not convert object to a String ", e, TYPE_TRANSFORMATION_ERROR);
			}
		}
	}

	@SuppressWarnings("java:S6201") // Pattern Matching for "instanceof" was introduced in java 16 while we still try to be compliant with java 11
	private static TimeZone toTimeZone(Object tz) {
		if (tz instanceof TimeZone) {
			return (TimeZone)tz;
		}
		if (tz instanceof String) {
			return TimeZone.getTimeZone((String)tz);
		}
		throw new IllegalArgumentException(format("Cannot convert %s to TimeZone", tz));
	}
}
