package io.firebolt.jdbc.resultset.type;

import io.firebolt.jdbc.resultset.FireboltColumn;
import io.firebolt.jdbc.resultset.type.array.SqlArrayUtil;
import io.firebolt.jdbc.resultset.type.date.SqlDateUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Array;
import java.sql.Timestamp;
import java.util.Date;
import java.util.function.BiFunction;

/** This class contains the java types the Firebolt datatypes are mapped to */
@Slf4j
public enum BaseType {
  LONG(Long.class, (value, subType) -> Long.parseLong(value)),
  INTEGER(Integer.class, (value, subType) -> Integer.parseInt(value)),
  BIG_INTEGER(BigInteger.class, (value, subType) -> new BigInteger(value)),
  STRING(String.class, (value, subType) -> String.valueOf(value)),
  FLOAT(Float.class, (value, subType) -> isNan(value) ? Float.NaN : Float.parseFloat(value)),
  DOUBLE(Double.class, (value, subType) -> isNan(value) ? Double.NaN : Double.parseDouble(value)),
  DATE(Date.class, (value, subType) -> SqlDateUtil.transformToDateFunction.apply(value)),
  TIMESTAMP(
      Timestamp.class, (value, subType) -> SqlDateUtil.transformToTimestampFunction.apply(value)),
  NULL(Object.class, (value, subType) -> null),
  OTHER(String.class, (value, subType) -> "Unknown"),
  OBJECT(Object.class, (value, subType) -> value),
  DECIMAL(BigDecimal.class, (value, subType) -> new BigDecimal(value)),
  BOOLEAN(Boolean.class, (value, subType) -> !"0".equals(value)),
  ARRAY(Array.class, SqlArrayUtil.transformToSqlArrayFunction::apply);

  public static final String NULL_VALUE = "\\N";
  private final Class<?> type;
  private final BiFunction<String, FireboltColumn, Object> transformFunction;

  BaseType(Class<?> type, BiFunction<String, FireboltColumn, Object> transformFunction) {
    this.type = type;
    this.transformFunction = transformFunction;
  }

  public Class<?> getType() {
    return type;
  }

  public <T> T transform(String value, FireboltColumn column) {
    validateObjectNotNull(value);
    if (isNull(value)) {
      return null;
    }
    return (T) transformFunction.apply(value, column);
  }

  public <T> T transform(String value) {
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
