package io.firebolt.jdbc.resultset.type;

import java.sql.Types;

/** Supported data types. */
public enum FireboltDataType {
  U_INT_8(Types.TINYINT, "UInt8", BaseType.LONG, false, 3, 0),
  INT_8(Types.TINYINT, "Int8", BaseType.INTEGER, true, 4, 0),
  INT_32(Types.INTEGER, "Int32", BaseType.BIG_INTEGER, true, 11, 0, "INTEGER", "INT"),
  INT_64(Types.BIGINT, "Int64", BaseType.BIG_INTEGER, true, 20, 0, "BIGINT"),
  FLOAT_32(Types.FLOAT, "Float32", BaseType.FLOAT, true, 8, 8, "FLOAT"),
  FLOAT_64(Types.DOUBLE, "Float64", BaseType.DOUBLE, true, 17, 17, "DOUBLE"),
  STRING(Types.VARCHAR, "String", BaseType.STRING, false, 0, 0, "VARCHAR", "TEXT"),
  DATE(Types.DATE, "Date", BaseType.DATE, false, 10, 0),
  DATE_32(Types.DATE, "Date32", BaseType.DATE, false, 10, 0, "DATE_EXT"),
  DATE_TIME_64(Types.TIMESTAMP, "DateTime64", BaseType.TIMESTAMP, false, 6, 0, "TIMESTAMP_EXT"),
  DATE_TIME(Types.TIMESTAMP, "DateTime", BaseType.TIMESTAMP, false, 19, 0, "TIMESTAMP"),
  NOTHING(Types.NULL, "Nothing", BaseType.NULL, false, 0, 0),
  UNKNOWN(Types.OTHER, "Unknown", BaseType.OTHER, false, 0, 0),
  DECIMAL(Types.DECIMAL, "Decimal", BaseType.DECIMAL, true, 0, 0, "DEC"),
  ARRAY(Types.ARRAY, "Array", BaseType.ARRAY, false, 0, 0),
  TUPLE(Types.OTHER, "Tuple",  BaseType.STRING, false, 0, 0);

  private final int sqlType;

  private final String name;
  private final BaseType fireboltBaseType;

  private final boolean signed;
  private final int defaultPrecision;
  private final int defaultScale;
  private final String[] aliases;

  public static final String NULLABLE_TYPE = "Nullable";

  FireboltDataType(
      int sqlType,
      String name,
      BaseType baseType,
      boolean signed,
      int defaultPrecision,
      int defaultScale,
      String... aliases) {
    this.sqlType = sqlType;
    this.name = name;
    this.fireboltBaseType = baseType;
    this.signed = signed;
    this.defaultPrecision = defaultPrecision;
    this.defaultScale = defaultScale;
    this.aliases = aliases;
  }

  public static FireboltDataType ofType(String type) {
    String s = type.trim();
    try {
      for (FireboltDataType dataType : values()) {
        if (s.equalsIgnoreCase(dataType.name)) {
          return dataType;
        }
      }
      return FireboltDataType.valueOf(type);
    } catch (IllegalArgumentException e) {
      for (FireboltDataType dataType : values()) {
        for (String alias : dataType.aliases) {
          if (s.equalsIgnoreCase(alias)) {
            return dataType;
          }
        }
      }
    }
    return FireboltDataType.UNKNOWN;
  }

  public int getSqlType() {
    return sqlType;
  }

  public String getName() {
    return name;
  }

  public BaseType getBaseType() {
    return fireboltBaseType;
  }

  public boolean isSigned() {
    return signed;
  }

  public int getDefaultPrecision() {
    return defaultPrecision;
  }

  public int getDefaultScale() {
    return defaultScale;
  }
}
