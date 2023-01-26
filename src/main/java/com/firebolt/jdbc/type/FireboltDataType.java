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
	U_INT_8(Types.TINYINT, "UInt8", BaseType.INTEGER.name(), BaseType.INTEGER, false, false, 3, 0, false),
	BOOLEAN(Types.BOOLEAN, "Boolean", BaseType.BOOLEAN.name(), BaseType.BOOLEAN, false, false, 1, 0, false, "BOOL"),
	INT_32(Types.INTEGER, "Int32", BaseType.INTEGER.name(), BaseType.INTEGER, true, false, 11, 0, false, "INTEGER",
			"INT", "Int8", "Int16", "UInt16", "UInt32"),
	INT_64(Types.BIGINT, "Int64", BaseType.BIGINT.name(), BaseType.LONG, true, false, 20, 0, false, "LONG", "BIGINT"),
	// Although not supported, U_INT_64 is still coming from Firebolt and needs to
	// be handled for now
	U_INT_64(Types.BIGINT, "UInt64", BaseType.BIGINT.name(), BaseType.BIGINT, false, false, 20, 0, false),

	FLOAT_32(Types.FLOAT, "Float32", BaseType.FLOAT.name(), BaseType.FLOAT, true, false, 8, 8, false, "FLOAT"),
	FLOAT_64(Types.DOUBLE, "Float64", BaseType.DOUBLE.name(), BaseType.DOUBLE, true, false, 17, 17, false,
			BaseType.DOUBLE.name()),
	STRING(Types.VARCHAR, "String", BaseType.STRING.name(), BaseType.STRING, false, true, 0, 0, false, "VARCHAR",
			"TEXT"),
	DATE(Types.DATE, "Date", BaseType.DATE.name(), BaseType.DATE, false, false, 10, 0, true, "PGDate"),
	DATE_32(Types.DATE, "Date32", "DATE_EXT", BaseType.DATE, false, false, 10, 0, true, "DATE_EXT"),
	DATE_TIME_64(Types.TIMESTAMP, "DateTime64", "TIMESTAMP_EXT", BaseType.TIMESTAMP, false, false, 19, 0, true,
			"TIMESTAMP_EXT"),
	DATE_TIME(Types.TIMESTAMP, "DateTime", "TIMESTAMP", BaseType.TIMESTAMP, false, false, 19, 0, true, "TIMESTAMP"),
	TIMESTAMP(Types.TIMESTAMP, "TimestampNtz", BaseType.TIMESTAMP.name(), BaseType.TIMESTAMP, false, false, 26, 6, true),
	TIMESTAMP_WITH_TIMEZONE(Types.TIMESTAMP_WITH_TIMEZONE, "Timestamptz", BaseType.TIMESTAMP_WITH_TIMEZONE.name(), BaseType.TIMESTAMP_WITH_TIMEZONE, false, false, 32, 6, true),
	NOTHING(Types.NULL, "Nothing", "NOTHING", BaseType.NULL, false, false, 0, 0, false, "NULL"),
	UNKNOWN(Types.OTHER, "Unknown", "UNKNOWN", BaseType.OTHER, false, false, 0, 0, false),
	DECIMAL(Types.DECIMAL, "Decimal", BaseType.DECIMAL.name(), BaseType.DECIMAL, true, false, 0, 0, false, "DEC"),
	ARRAY(Types.ARRAY, "Array", BaseType.ARRAY.name(), BaseType.ARRAY, false, true, 0, 0, false),
	TUPLE(Types.OTHER, "Tuple", "TUPLE", BaseType.OBJECT, false, true, 0, 0, false),
	BYTEA(Types.BINARY, "ByteA", "BYTEA", BaseType.BYTEA, false, true, 0, 0, false);

	public static final String NULLABLE_TYPE = "NULLABLE";

	private static final Map<String, FireboltDataType> typeNameOrAliasToType;

	static {
		typeNameOrAliasToType = new HashMap<>();
		for (FireboltDataType dataType : values()) {
			typeNameOrAliasToType.put(dataType.internalName.toUpperCase(), dataType);
			Arrays.stream(dataType.aliases).forEach(alias -> typeNameOrAliasToType.put(alias.toUpperCase(), dataType));
		}
	}

	private final int sqlType;
	private final String internalName;
	private final String displayName;
	private final BaseType baseType;
	private final boolean signed;
	private final boolean caseSensitive;
	private final int defaultPrecision;
	private final int defaultScale;

	private final boolean time;
	private final String[] aliases;

	FireboltDataType(int sqlType, String internalName, String displayName, BaseType baseType, boolean signed,
			boolean caseSensitive, int defaultPrecision, int defaultScale, boolean isTime, String... aliases) {
		this.sqlType = sqlType;
		this.internalName = internalName;
		this.displayName = displayName;
		this.baseType = baseType;
		this.signed = signed;
		this.caseSensitive = caseSensitive;
		this.defaultPrecision = defaultPrecision;
		this.defaultScale = defaultScale;
		this.aliases = aliases;
		this.time = isTime;
	}

	public static FireboltDataType ofType(String type) {
		String formattedType = type.trim().toUpperCase();
		return Optional.ofNullable(typeNameOrAliasToType.get(formattedType)).orElse(UNKNOWN);
	}

}
