package io.firebolt.jdbc.resultset.type;

import io.firebolt.jdbc.exception.FireboltException;
import io.firebolt.jdbc.resultset.FireboltColumn;
import io.firebolt.jdbc.resultset.type.array.SqlArrayUtil;
import io.firebolt.jdbc.resultset.type.date.SqlDateUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.StringEscapeUtils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Array;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

/** This class contains the java types the Firebolt datatypes are mapped to */
@Slf4j
public enum BaseType {
  LONG(Long.class, (value, subType) -> Long.parseLong(value)),
  INTEGER(Integer.class, (value, subType) -> Integer.parseInt(value)),
  SHORT(Short.class, (value, subType) -> Short.parseShort(value)),
  BIG_INTEGER(BigInteger.class, (value, subType) -> new BigInteger(value)),
  STRING(String.class, (value, subType) -> StringEscapeUtils.unescapeJava(value)),
  FLOAT(
      Float.class,
      (value, subType) -> {
        if (isNan(value)) {
          return Float.NaN;
        } else if (isPositiveInf(value)) {
          return Float.POSITIVE_INFINITY;
        } else if (isNegativeInf(value)) {
          return Float.NEGATIVE_INFINITY;
        } else {
          return Float.parseFloat(value);
        }
      }),
  DOUBLE(
      Double.class,
      (value, subType) -> {
        if (isNan(value)) {
          return Double.NaN;
        } else if (isPositiveInf(value)) {
          return Double.POSITIVE_INFINITY;
        } else if (isNegativeInf(value)) {
          return Double.NEGATIVE_INFINITY;
        } else {
          return Double.parseDouble(value);
        }
      }),
  DATE(Date.class, (value, subType) -> SqlDateUtil.transformToDateFunction.apply(value)),
  TIMESTAMP(
      Timestamp.class, (value, subType) -> SqlDateUtil.transformToTimestampFunction.apply(value)),
  TIME(Time.class, (value, subType) -> SqlDateUtil.transformToTimeFunction.apply(value)),
  NULL(Object.class, (value, subType) -> null),
  OTHER(String.class, (value, subType) -> "Unknown"),
  OBJECT(Object.class, (value, subType) -> value),
  DECIMAL(BigDecimal.class, (value, subType) -> new BigDecimal(value)),
  BOOLEAN(Boolean.class, (value, subType) -> !"0".equals(value)),
  ARRAY(Array.class, SqlArrayUtil::transformToSqlArray);

  private static boolean isPositiveInf(String value) {
    return StringUtils.equalsAnyIgnoreCase(value, "+inf", "inf");
  }

  private static boolean isNegativeInf(String value) {
    return StringUtils.equals(value, "-inf");
  }

  public static final String NULL_VALUE = "\\N";
  private final Class<?> type;
  private final CheckedBiFunction<String, FireboltColumn, Object> transformFunction;

  BaseType(Class<?> type, CheckedBiFunction<String, FireboltColumn, Object> transformFunction) {
    this.type = type;
    this.transformFunction = transformFunction;
  }

  public Class<?> getType() {
    return type;
  }

  private static void validateThatValueDoesNotContainException(String value)
      throws FireboltException {
    if (StringUtils.contains(value, "DB::Exception")) {
      String errorResponseMessage =
          String.format("Server failed to execute query with the following error:%n%s", value);
      throw new FireboltException(errorResponseMessage);
    }
  }

  public <T> T transform(String value, FireboltColumn column) throws FireboltException {
    validateObjectNotNull(value);
    if (isNull(value)) {
      return null;
    } else if (this.getType() != String.class) {
      validateThatValueDoesNotContainException(value);
    }
    return (T) transformFunction.apply(value, column);
  }

  public <T> T transform(String value) throws FireboltException {
    return this.transform(value, null);
  }

  public static boolean isNull(String value) {
    return StringUtils.equalsIgnoreCase(value, NULL_VALUE);
  }

  private static boolean isNan(String value) {
    return StringUtils.equalsIgnoreCase(value, "nan");
  }

  private static void validateObjectNotNull(String value) {
    if (value == null) {
      throw new NumberFormatException("The value cannot be null");
    }
  }
}
