package io.firebolt.jdbc.resultset.type;

import io.firebolt.jdbc.resultset.type.array.SqlArrayUtil;
import io.firebolt.jdbc.resultset.type.date.SqlDateUtil;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Array;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;
import java.util.function.Function;

public enum JavaTypeToStringConverter {

  NULL(Object.class, value -> "\\N"),
  BOOLEAN(Boolean.class, value -> Boolean.TRUE.equals(value) ? "1" : "0"),
  COLLECTION(
          Collection.class, value -> SqlArrayUtil.arrayToString(((Collection<?>) value).toArray())),
  UUID(java.util.UUID.class, value -> value == null ? getNullValue() : ((UUID) value).toString()),
  SHORT(Short.class, value -> value == null ? getNullValue() : Short.toString((short) value)),
  STRING(String.class, value -> String.format("'%s'", value)),
  LONG(Long.class, getValueOfFunction()),
  INTEGER(Integer.class, getValueOfFunction()),
  BIG_INTEGER(BigInteger.class, getValueOfFunction()),
  FLOAT(Float.class, getValueOfFunction()),
  DOUBLE(Double.class, getValueOfFunction()),
  DATE(Date.class, date -> SqlDateUtil.transformFromDateFunction.apply((Date) date)),
  TIME(Time.class, time -> SqlDateUtil.transformFromTimeFunction.apply((Time) time)),
  TIMESTAMP(
          Timestamp.class, time -> SqlDateUtil.transformFromTimestampFunction.apply((Timestamp) time)),
  BIG_DECIMAL(
          BigDecimal.class,
          value -> value == null ? getNullValue() : ((BigDecimal) value).toPlainString()),
  ARRAY(Array.class, SqlArrayUtil::arrayToString);

  private final Class<?> sourceType;
  private final Function<Object, String> transformToJavaTypeFunction;

  JavaTypeToStringConverter(
          Class<?> sourceType, Function<Object, String> transformToSqlStringFunction) {
    this.sourceType = sourceType;
    this.transformToJavaTypeFunction = transformToSqlStringFunction;
  }

  public Class<?> getSourceType() {
    return sourceType;
  }

  public Function<Object, String> getTransformToJavaTypeFunction() {
    return transformToJavaTypeFunction;
  }

  public static String toString(Object object) {
    Class<?> objectType;
    if (object == null) {
      objectType = Object.class;
    } else if (object.getClass().isArray()) {
      objectType = Array.class;
    } else {
      objectType = object.getClass();
    }
    return Arrays.stream(JavaTypeToStringConverter.values())
            .filter(converter -> converter.getSourceType().equals(objectType))
            .findAny()
            .map(formatter -> formatter.transformToJavaTypeFunction.apply(object))
            .orElseThrow(
                    () ->
                            new UnsupportedOperationException(
                                    String.format("Cannot convert type %s", objectType)));
  }

  private static String getNullValue() {
    return "\\t";
  }

  private static Function<Object, String> getValueOfFunction() {
    return value -> value == null ? getNullValue() : String.valueOf(value);
  }
}
