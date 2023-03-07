package com.firebolt.jdbc.type;

import java.sql.Types;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import lombok.Getter;

/** Supported data types. */
@Getter
public enum FireboltDataType {
	// Not officially supported but U_INT_8 is still coming from Firebolt
	U_INT_8(Types.TINYINT, FireboltDataTypeDisplayNames.INTEGER, BaseType.INTEGER, false, false, 3, null, null, false, "UInt8"),
	BOOLEAN(Types.BOOLEAN, FireboltDataTypeDisplayNames.BOOLEAN, BaseType.BOOLEAN, false, false, 1, null, null, false, "Boolean", "BOOL"),
	INTEGER(Types.INTEGER, FireboltDataTypeDisplayNames.INTEGER, BaseType.INTEGER, true, false, 11, null, null, false, "Int32", "INTEGER",
			"INT", "Int8", "Int16", "UInt16", "UInt32"),
	BIG_INT(Types.BIGINT, FireboltDataTypeDisplayNames.BIGINT, BaseType.LONG, true, false, 20, null, null, false, "LONG", "BIGINT"),

	// Although not supported, U_INT_64 is still coming from Firebolt and needs to
	// be handled for now
	BIG_INT_64(Types.BIGINT, FireboltDataTypeDisplayNames.BIGINT, BaseType.BIGINT, false, false, 20, null, null, false, "Int64", "UInt64"),
	// Although not supported, U_INT_64 is still coming from Firebolt and needs to
	// be handled for now
	UNISGNED_BIG_INT_64(Types.BIGINT, FireboltDataTypeDisplayNames.BIGINT, BaseType.BIGINT, false, false, 20, null, null, false, "UInt64"),

	REAL(Types.REAL, FireboltDataTypeDisplayNames.REAL, BaseType.REAL, true, false, 24, null, null, false, "Float32", "FLOAT", "REAL"),
	DOUBLE_PRECISION(Types.DOUBLE, FireboltDataTypeDisplayNames.DOUBLE_PRECISION, BaseType.DOUBLE, true, false, 53, null, null, false,
			BaseType.DOUBLE.name(), "Float64", "DOUBLE PRECISION"),
	TEXT(Types.VARCHAR, FireboltDataTypeDisplayNames.TEXT, BaseType.TEXT, false, true, 0, null, null, false, "String", "VARCHAR",
			"TEXT"),
	DATE(Types.DATE, FireboltDataTypeDisplayNames.DATE, BaseType.DATE, false, false, 10, null, null, true, "Date", "PGDate"),
	DATE_32(Types.DATE, "date ext", BaseType.DATE, false, false, 10, null, null, true, "Date32", "DATE_EXT"),
	DATE_TIME_64(Types.TIMESTAMP, FireboltDataTypeDisplayNames.TIMESTAMP_EXT, BaseType.TIMESTAMP, false, false, 19, null, 6, true,
			"DateTime64", "TIMESTAMP_EXT"),
	DATE_TIME(Types.TIMESTAMP,  FireboltDataTypeDisplayNames.TIMESTAMP, BaseType.TIMESTAMP, false, false, 19, null, 6, true, "DateTime", "TIMESTAMP"),
	TIMESTAMP(Types.TIMESTAMP, FireboltDataTypeDisplayNames.TIMESTAMP, BaseType.TIMESTAMP, false, false, 26, null, 6, true, "TimestampNtz"),
	TIMESTAMP_WITH_TIMEZONE(Types.TIMESTAMP_WITH_TIMEZONE, FireboltDataTypeDisplayNames.TIMESTAMP_WITH_TIMEZONE, BaseType.TIMESTAMP_WITH_TIMEZONE, false, false, 32, null, 6, true, "Timestamptz"),
	NOTHING(Types.NULL, FireboltDataTypeDisplayNames.NULL, BaseType.NULL, false, false, 0, null, null, false, "Nothing", "NULL"),
	UNKNOWN(Types.OTHER, FireboltDataTypeDisplayNames.UNKNOWN, BaseType.OTHER, false, false, 0, null, null, false, "Unknown"),
	NUMERIC(Types.NUMERIC, FireboltDataTypeDisplayNames.NUMERIC, BaseType.NUMERIC, true, false, 1000, null, 999, false, "Decimal", "DEC", "NUMERIC"),
	ARRAY(Types.ARRAY, FireboltDataTypeDisplayNames.ARRAY, BaseType.ARRAY, false, true, 0, null, null, false, "Array"),
	TUPLE(Types.OTHER, FireboltDataTypeDisplayNames.TUPLE, BaseType.OBJECT, false, true, 0, null, null, false, "Tuple"),
	BYTEA(Types.BINARY, FireboltDataTypeDisplayNames.BYTEA, BaseType.BYTEA, false, true, 2147483647, null, null, false, "ByteA");

	private static final Map<String, FireboltDataType> typeNameOrAliasToType;

	static {
		typeNameOrAliasToType = new HashMap<>();
		for (FireboltDataType dataType : values()) {
			Arrays.stream(dataType.aliases).forEach(alias -> typeNameOrAliasToType.put(alias.toUpperCase(), dataType));
		}
	}

	private final int sqlType;
	private final String displayName;
	private final BaseType baseType;
	private final boolean signed;
	private final boolean caseSensitive;
	private final Integer precision;

	private final Integer minScale;
	private final Integer maxScale;

	private final boolean time;
	private final String[] aliases;

	FireboltDataType(int sqlType, String displayName, BaseType baseType, boolean signed,
					 boolean caseSensitive, Integer precision, Integer minScale, Integer maxScale, boolean isTime, String... aliases) {
		this.sqlType = sqlType;
		this.displayName = displayName;
		this.baseType = baseType;
		this.signed = signed;
		this.caseSensitive = caseSensitive;
		this.precision = precision;
		this.maxScale = maxScale;
		this.aliases = aliases;
		this.time = isTime;
		this.minScale = minScale;
	}

	public static FireboltDataType ofType(String type) {
		String formattedType = type.trim().toUpperCase();
		return Optional.ofNullable(typeNameOrAliasToType.get(formattedType)).orElse(UNKNOWN);
	}

}
