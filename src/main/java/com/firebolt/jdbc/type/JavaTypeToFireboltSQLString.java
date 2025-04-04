package com.firebolt.jdbc.type;

import com.firebolt.jdbc.CheckedBiFunction;
import com.firebolt.jdbc.CheckedFunction;
import com.firebolt.jdbc.CheckedSupplier;
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
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.TimeZone;
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
	STRING(String.class, getSQLStringValueOfString(ParserVersion.CURRENT), getSQLStringValueOfStringVersioned()),
	LONG(Long.class, value -> Long.toString(((Number)value).longValue())),
	INTEGER(Integer.class, value -> Integer.toString(((Number)value).intValue())),
	BIG_INTEGER(BigInteger.class, value -> value instanceof BigInteger ? value.toString() : Long.toString(((Number)value).longValue())),
	FLOAT(Float.class, value -> Float.toString(((Number)value).floatValue())),
	DOUBLE(Double.class, value -> Double.toString(((Number)value).doubleValue())),
	DATE(Date.class, date -> SqlDateUtil.transformFromDateToSQLStringFunction.apply((Date) date), (date, tz) -> SqlDateUtil.transformFromDateWithTimezoneToSQLStringFunction.apply((Date) date, toTimeZone(tz))),
	LOCAL_DATE(LocalDate.class, date -> SqlDateUtil.transformFromLocalDateToSQLStringFunction.apply((LocalDate) date)),
	TIMESTAMP(Timestamp.class, time -> SqlDateUtil.transformFromTimestampToSQLStringFunction.apply((Timestamp) time), (ts, tz) -> SqlDateUtil.transformFromTimestampWithTimezoneToSQLStringFunction.apply((Timestamp) ts, toTimeZone(tz))),
	LOCAL_DATE_TIME(LocalDateTime.class, time -> SqlDateUtil.transformFromLocalDateTimeToSQLStringFunction.apply((LocalDateTime) time)),
	BIG_DECIMAL(BigDecimal.class, value -> value == null ? BaseType.NULL_VALUE : ((BigDecimal) value).toPlainString()),
	ARRAY(Array.class, SqlArrayUtil::arrayToString),
	BYTE_ARRAY(byte[].class, value -> ofNullable(byteArrayToHexString((byte[])value, true)).map(x  -> format("E'%s'::BYTEA", x)).orElse(null)),
	;

	private static final List<Entry<String, String>> legacyCharacterToEscapedCharacterPairs = List.of(
			Map.entry("\0", "\\0"), Map.entry("\\", "\\\\"), Map.entry("'", "''"));
	private static final List<Entry<String, String>> characterToEscapedCharacterPairs = List.of(
			Map.entry("'", "''"));
	//https://docs.oracle.com/javase/1.5.0/docs/guide/jdbc/getstart/mapping.html
	private static final Map<JDBCType, List<Class<?>>> jdbcTypeToClass = Map.ofEntries(
			Map.entry(JDBCType.CHAR, List.of(String.class)),
			Map.entry(JDBCType.VARCHAR, List.of(String.class)),
			Map.entry(JDBCType.LONGVARCHAR, List.of(String.class)),
			Map.entry(JDBCType.NUMERIC, List.of(java.math.BigDecimal.class)),
			Map.entry(JDBCType.DECIMAL, List.of(java.math.BigDecimal.class)),
			Map.entry(JDBCType.BIT, List.of(Boolean.class)),
			Map.entry(JDBCType.BOOLEAN, List.of(Boolean.class)),
			Map.entry(JDBCType.TINYINT, List.of(Short.class)),
			Map.entry(JDBCType.SMALLINT, List.of(Short.class)),
			Map.entry(JDBCType.INTEGER, List.of(Integer.class)),
			Map.entry(JDBCType.BIGINT, List.of(Long.class)),
			Map.entry(JDBCType.REAL, List.of(Float.class)),
			Map.entry(JDBCType.FLOAT, List.of(Double.class)),
			Map.entry(JDBCType.DOUBLE, List.of(Double.class)),
			Map.entry(JDBCType.BINARY, List.of(byte[].class)),
			Map.entry(JDBCType.VARBINARY, List.of(byte[].class)),
			Map.entry(JDBCType.LONGVARBINARY, List.of(byte[].class)),
			Map.entry(JDBCType.DATE, List.of(Date.class, LocalDate.class)),
			Map.entry(JDBCType.TIME, List.of(Time.class)),
			Map.entry(JDBCType.TIMESTAMP, List.of(Timestamp.class, LocalDateTime.class)),
			Map.entry(JDBCType.TIMESTAMP_WITH_TIMEZONE, List.of(Timestamp.class)),
			//DISTINCT        Object type of underlying type
			Map.entry(JDBCType.CLOB, List.of(Clob.class)),
			Map.entry(JDBCType.BLOB, List.of(Blob.class)),
			Map.entry(JDBCType.ARRAY, List.of(Array.class))
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

	public static String transformAny(Object object) throws SQLException {
		return transformAny(object, ParserVersion.CURRENT);
	}

	public static String transformAny(Object object, ParserVersion version) throws SQLException {
		return transformAny(object, () -> getType(object), version);
	}

	public static String transformAny(Object object, int sqlType) throws SQLException {
		return transformAny(object, sqlType, ParserVersion.CURRENT);
	}

	public static String transformAny(Object object, int sqlType, ParserVersion version) throws SQLException {
		return transformAny(object, () -> getType(sqlType, object), version);
	}

	private static String transformAny(Object object, CheckedSupplier<Class<?>> classSupplier, ParserVersion version) throws SQLException {
		return object == null ? NULL_VALUE : transformAny(object, classSupplier.get(), version);
	}

	private static String transformAny(Object object, Class<?> objectType, ParserVersion version) throws SQLException {
		JavaTypeToFireboltSQLString converter = Optional.ofNullable(classToType.get(objectType))
				.orElseThrow(() -> new FireboltException(
						format("Cannot convert type %s. The type is not supported.", objectType),
						TYPE_NOT_SUPPORTED));
		if (version == ParserVersion.LEGACY && object instanceof String) {
			return converter.transform(object, version);
		}
		return converter.transform(object);
	}

	private static Class<?> getType(Object object) {
		return object.getClass().isArray() && !byte[].class.equals(object.getClass()) ? Array.class : object.getClass();
	}

	private static Class<?> getType(int sqlType, Object o) throws SQLException {
		try {
			JDBCType jdbcType = JDBCType.valueOf(sqlType);
			List<Class<?>> classes = Optional.ofNullable(jdbcTypeToClass.get(jdbcType))
					.orElseThrow(() -> new FireboltException(format("Unsupported JDBC type %s", jdbcType), TYPE_NOT_SUPPORTED));
			if (classes.size() > 1 && classes.contains(o.getClass())) {
				return o.getClass();
			} else if (classes.size() == 1) {
				return classes.get(0);
			} else {
				throw new FireboltException(format("Unsupported JDBC type %s", jdbcType), TYPE_NOT_SUPPORTED);
			}
		} catch(IllegalArgumentException e) {
			throw new FireboltException(format("Unsupported SQL type %d", sqlType), TYPE_NOT_SUPPORTED);
		}
	}

	private static CheckedBiFunction<Object, Object, String> getSQLStringValueOfStringVersioned() {
		return (value, version) -> getSQLStringValueOfString((ParserVersion) version).apply(value);
	}

	private static CheckedFunction<Object, String> getSQLStringValueOfString(ParserVersion version) {
		return value -> {
			String escaped = (String) value;
			List<Entry<String, String>> charactersToEscape = version == ParserVersion.LEGACY ? legacyCharacterToEscapedCharacterPairs : characterToEscapedCharacterPairs;
			for (Entry<String, String> specialCharacter : charactersToEscape) {
				escaped = escaped.replace(specialCharacter.getKey(), specialCharacter.getValue());
			}
			return format("'%s'", escaped);
		};
	}

	public String transform(Object object, Object ... more) throws SQLException {
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
