package com.firebolt.jdbc.resultset.type;

import java.sql.Types;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/** Supported data types. */
public enum FireboltDataType {
  U_INT_8(
      Types.TINYINT,
      "UInt8",
      BaseType.INTEGER.name(),
      BaseType.INTEGER,
      false,
      false,
      3,
      0,
      "BOOLEAN"),
  INT_32(
      Types.INTEGER,
      "Int32",
      BaseType.INTEGER.name(),
      BaseType.INTEGER,
      true,
      false,
      11,
      0,
      "INTEGER",
      "INT",
      "Int8",
      "Int16",
      "UInt16",
      "UInt32"),
  INT_64(Types.BIGINT, "Int64", "BIGINT", BaseType.LONG, true, false, 20, 0, "LONG"),
  U_INT_64(
      Types.BIGINT,
      "UInt64",
      "BIGINT",
      BaseType.BIG_INTEGER,
      false,
      false,
      20,
      0), // Although not supported, this type is still coming from Firebolt and needs to be handled
  // for now
  FLOAT_32(
      Types.FLOAT, "Float32", BaseType.FLOAT.name(), BaseType.FLOAT, true, false, 8, 8, "FLOAT"),
  FLOAT_64(
      Types.DOUBLE,
      "Float64",
      BaseType.DOUBLE.name(),
      BaseType.DOUBLE,
      true,
      false,
      17,
      17,
      BaseType.DOUBLE.name()),
  STRING(
      Types.VARCHAR,
      "String",
      BaseType.STRING.name(),
      BaseType.STRING,
      false,
      true,
      0,
      0,
      "VARCHAR",
      "TEXT"),
  DATE(Types.DATE, "Date", BaseType.DATE.name(), BaseType.DATE, false, false, 10, 0),
  DATE_32(Types.DATE, "Date32", "DATE_EXT", BaseType.DATE, false, false, 10, 0, "DATE_EXT"),
  DATE_TIME_64(
      Types.TIMESTAMP, "DateTime64", "TIMESTAMP_EXT", BaseType.TIMESTAMP, false, false, 19, 0, "TIMESTAMP_EXT"),
  DATE_TIME(
      Types.TIMESTAMP,
      "DateTime",
      "TIMESTAMP",
      BaseType.TIMESTAMP,
      false,
      false,
      19,
      0,
      "TIMESTAMP"),
  NOTHING(Types.NULL, "Nothing", BaseType.NULL.name(), BaseType.NULL, false, false, 0, 0),
  UNKNOWN(Types.OTHER, "Unknown", "UNKNOWN", BaseType.OTHER, false, false, 0, 0),
  DECIMAL(
      Types.DECIMAL,
      "Decimal",
      BaseType.DECIMAL.name(),
      BaseType.DECIMAL,
      true,
      false,
      0,
      0,
      "DEC"),
  ARRAY(Types.ARRAY, "Array", BaseType.ARRAY.name(), BaseType.ARRAY, false, true, 0, 0),
  TUPLE(Types.OTHER, "Tuple", "TUPLE", BaseType.OBJECT, false, true, 0, 0);

  private final int sqlType;

  private final String internalName;

  private final String displayName;
  private final BaseType fireboltBaseType;

  private final boolean signed;

  private final boolean caseSensitive;
  private final int defaultPrecision;
  private final int defaultScale;
  private final String[] aliases;

  public static final String NULLABLE_TYPE = "NULLABLE";

  private static Map<String, FireboltDataType> typeNameOrAliasToType;

  static {
    typeNameOrAliasToType = new HashMap<>();
    for (FireboltDataType dataType : values()) {
      typeNameOrAliasToType.put(dataType.internalName.toUpperCase(), dataType);
      Arrays.stream(dataType.aliases)
          .forEach(alias -> typeNameOrAliasToType.put(alias.toUpperCase(), dataType));
    }
  }

  FireboltDataType(
      int sqlType,
      String internalName,
      String displayName,
      BaseType baseType,
      boolean signed,
      boolean caseSensitive,
      int defaultPrecision,
      int defaultScale,
      String... aliases) {
    this.sqlType = sqlType;
    this.internalName = internalName;
    this.displayName = displayName;
    this.fireboltBaseType = baseType;
    this.signed = signed;
    this.caseSensitive = caseSensitive;
    this.defaultPrecision = defaultPrecision;
    this.defaultScale = defaultScale;
    this.aliases = aliases;
  }

  public static FireboltDataType ofType(String type) {
    String formattedType = type.trim().toUpperCase();
    return Optional.ofNullable(typeNameOrAliasToType.get(formattedType)).orElse(UNKNOWN);
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

  public boolean isCaseSensitive() {
    return caseSensitive;
  }

  public int getDefaultPrecision() {
    return defaultPrecision;
  }

  public int getDefaultScale() {
    return defaultScale;
  }
}
