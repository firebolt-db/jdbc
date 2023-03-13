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
	U_INT_8(Types.TINYINT, FireboltDataTypeDisplayNames.INTEGER, BaseType.INTEGER, true, false, 3, 0, 0, false, "UInt8"),
	BOOLEAN(Types.BOOLEAN, FireboltDataTypeDisplayNames.BOOLEAN, BaseType.BOOLEAN, true, false, 1, 0, 0, false, "Boolean", "BOOL"),
	INTEGER(Types.INTEGER, FireboltDataTypeDisplayNames.INTEGER, BaseType.INTEGER, true, false, 0, 0, 0, false, "Int32", "INTEGER",
			"INT", "Int8", "Int16", "UInt16", "UInt32"),
	BIG_INT(Types.BIGINT, FireboltDataTypeDisplayNames.BIGINT, BaseType.LONG, true, false, 0, 0, 0, false,"LONG", "BIGINT"),

	// Although not supported, U_INT_64 is still coming from Firebolt and needs to
	// be handled for now
	BIG_INT_64(Types.BIGINT, FireboltDataTypeDisplayNames.BIGINT, BaseType.BIGINT, true, false, 0, 0, 0, false, "Int64", "UInt64"),
	// Although not supported, U_INT_64 is still coming from Firebolt and needs to
	// be handled for now
	UNSIGNED_BIG_INT_64(Types.BIGINT, FireboltDataTypeDisplayNames.BIGINT, BaseType.BIGINT, false, false, 0, 0, 0, false,"UInt64"),

	REAL(Types.REAL, FireboltDataTypeDisplayNames.REAL, BaseType.REAL, true, false, 0, 0, 0, false,"Float32", "FLOAT", "REAL"),
	DOUBLE_PRECISION(Types.DOUBLE, FireboltDataTypeDisplayNames.DOUBLE_PRECISION, BaseType.DOUBLE, true, false, 0, 0, 0, false,
			BaseType.DOUBLE.name(), "Float64", "DOUBLE PRECISION"),
	TEXT(Types.VARCHAR, FireboltDataTypeDisplayNames.TEXT, BaseType.TEXT, false, true, 0, 0, 0, false,"String", "VARCHAR",
			"TEXT"),
	DATE(Types.DATE, FireboltDataTypeDisplayNames.DATE, BaseType.DATE, false, false, 0, 0, 0, true,"Date", "PGDate"),
	DATE_32(Types.DATE, "date ext", BaseType.DATE, false, false, 10, 0, 0, true,"Date32", "DATE_EXT"),
	DATE_TIME_64(Types.TIMESTAMP, FireboltDataTypeDisplayNames.TIMESTAMP_EXT, BaseType.TIMESTAMP, false, false, 19, 0, 6, true,
			"DateTime64", "TIMESTAMP_EXT"),
	TIMESTAMP(Types.TIMESTAMP,  FireboltDataTypeDisplayNames.TIMESTAMP, BaseType.TIMESTAMP, false, false, 6, 0, 0, true,"DateTime", "TIMESTAMP", "TimestampNtz"),
	TIMESTAMP_WITH_TIMEZONE(Types.TIMESTAMP, FireboltDataTypeDisplayNames.TIMESTAMP, BaseType.TIMESTAMP_WITH_TIMEZONE, false, false, 6, 0, 0, true,"Timestamptz"),
	NOTHING(Types.NULL, FireboltDataTypeDisplayNames.NULL, BaseType.NULL, false, false, 0, 0, 0, false,"Nothing", "NULL"),
	UNKNOWN(Types.OTHER, FireboltDataTypeDisplayNames.UNKNOWN, BaseType.OTHER, false, false, 0, 0, 0, false,"Unknown"),
	NUMERIC(Types.NUMERIC, FireboltDataTypeDisplayNames.NUMERIC, BaseType.NUMERIC, true, false, 38, 0, 37, false, "Decimal", "DEC", "NUMERIC"),
	ARRAY(Types.ARRAY, FireboltDataTypeDisplayNames.ARRAY, BaseType.ARRAY, false, true, 0, 0, 0, false,"Array"),
	TUPLE(Types.OTHER, FireboltDataTypeDisplayNames.TUPLE, BaseType.OBJECT, false, true, 0, 0, 0, false,"Tuple"),
	BYTEA(Types.BINARY, FireboltDataTypeDisplayNames.BYTEA, BaseType.BYTEA, false, true, 0, 0, 0, false,"ByteA");

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
	private final int precision;

	private final int minScale;
	private final int maxScale;

	private final boolean time;
	private final String[] aliases;

	FireboltDataType(int sqlType, String displayName, BaseType baseType, boolean signed,
					 boolean caseSensitive, int precision, int minScale, int maxScale, boolean isTime, String... aliases) {
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
