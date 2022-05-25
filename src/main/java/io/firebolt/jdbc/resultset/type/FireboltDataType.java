package io.firebolt.jdbc.resultset.type;

import java.sql.Types;

/** Supported data types. */
public enum FireboltDataType {
  U_INT_8(Types.TINYINT, "UInt8", "BOOLEAN", BaseType.BOOLEAN, false, 3, 0, "BOOLEAN"),
  INT_32(Types.INTEGER, "Int32","INTEGER", BaseType.INTEGER, true, 11, 0, "INTEGER", "INT", "Int8", "Int16", "UInt16", "UInt32"),
  INT_64(Types.BIGINT, "Int64","BIGINT", BaseType.LONG, true, 20, 0, "BIGINT"),
  FLOAT_32(Types.FLOAT, "Float32","FLOAT", BaseType.FLOAT, true, 8, 8, "FLOAT"),
  FLOAT_64(Types.DOUBLE, "Float64","DOUBLE", BaseType.DOUBLE, true, 17, 17, "DOUBLE"),
  STRING(Types.VARCHAR, "String","STRING", BaseType.STRING,  false, 0, 0, "VARCHAR", "TEXT"),
  DATE(Types.DATE, "Date","DATE", BaseType.DATE, false, 10, 0),
  DATE_32(Types.DATE, "Date32","DATE_EXT", BaseType.DATE, false, 10, 0, "DATE_EXT"),
  DATE_TIME_64(Types.TIMESTAMP, "DateTime64", "TIMESTAMP_EXT", BaseType.TIMESTAMP, false, 6, 0, "TIMESTAMP_EXT"),
  DATE_TIME(Types.TIMESTAMP, "DateTime","TIMESTAMP", BaseType.TIMESTAMP, false, 19, 0, "TIMESTAMP"),
  NOTHING(Types.NULL, "Nothing",null, BaseType.NULL, false, 0, 0),
  UNKNOWN(Types.OTHER, "Unknown","UNKNOWN", BaseType.OTHER, false, 0, 0),
  DECIMAL(Types.DECIMAL, "Decimal", "DECIMAL", BaseType.DECIMAL, true, 0, 0, "DEC"),
  ARRAY(Types.ARRAY, "Array", "ARRAY", BaseType.ARRAY, false, 0, 0),
  TUPLE(Types.OTHER, "Tuple",  "TUPLE", BaseType.STRING, false, 0, 0);

  private final int sqlType;

  private final String internalName;

  private final String displayName;
  private final BaseType fireboltBaseType;

  private final boolean signed;
  private final int defaultPrecision;
  private final int defaultScale;
  private final String[] aliases;

  public static final String NULLABLE_TYPE = "Nullable";

  FireboltDataType(
      int sqlType,
      String internalName,
      String displayName,
      BaseType baseType,
      boolean signed,
      int defaultPrecision,
      int defaultScale,
      String... aliases) {
    this.sqlType = sqlType;
    this.internalName = internalName;
    this.displayName = displayName;
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
        if (s.equalsIgnoreCase(dataType.internalName)) {
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

  public String getInternalName() {
    return internalName;
  }

  public String getDisplayName() {
    return displayName;
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
