package io.firebolt.jdbc.resultset.type;

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

/**
 * This class contains the java types the Firebolt datatypes are mapped to
 */
@Slf4j
public enum BaseType {
  LONG(Long.class, (value, subType) -> Long.valueOf(value)),
  INTEGER(Integer.class, (value, subType) -> Integer.parseInt(value)),
  STRING(
      String.class,
      (value, subType) -> StringUtils.equals(value, "\\N") ? null : String.valueOf(value)),
  FLOAT(Float.class, (value, subType) -> Float.valueOf(value)),
  DOUBLE(Double.class, (value, subType) -> Double.valueOf(value)),
  DATE(Date.class, (value, subType) -> SqlDateUtil.transformToDateFunction.apply(value)),
  TIMESTAMP(
      Timestamp.class, (value, subType) -> SqlDateUtil.transformToTimestampFunction.apply(value)),
  NULL(Object.class, (value, subType) -> null),
  OTHER(String.class, (value, subType) -> "Unknown"),
  DECIMAL(BigDecimal.class, (value, subType) -> new BigDecimal(value)),
  BOOLEAN(Boolean.class, (value, subType) -> "1".equals(value)),
  ARRAY(Array.class, SqlArrayUtil.transformToSqlArrayFunction::apply);

  private final Class<?> type;
  private final BiFunction<String, FireboltDataType, Object> transformFunction;

  BaseType(Class<?> type, BiFunction<String, FireboltDataType, Object> transformFunction) {
    this.type = type;
    this.transformFunction = transformFunction;
  }

  public Class<?> getType() {
    return type;
  }

  public <T> T transform(String value, FireboltDataType subType) {
    return (T) transformFunction.apply(value, subType);
  }

  public <T> T transform(String value) {
    return this.transform(value, null);
  }
}
