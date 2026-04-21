package com.firebolt.jdbc.type;

/**
 * Utility class for computing JDBC column display sizes.
 * Based on PostgreSQL JDBC driver (pgjdbc) implementation.
 *
 * @see <a href="https://github.com/pgjdbc/pgjdbc/blob/master/pgjdbc/src/main/java/org/postgresql/jdbc/TypeInfoCache.java#L658-L735">pgjdbc TypeInfoCache.getDisplaySize()</a>
 */
public final class TypeDisplaySize {

	/**
	 * Default value to return for types with unknown/unlimited length.
	 * Following PostgreSQL JDBC convention.
	 */
	public static final int UNKNOWN_LENGTH = Integer.MAX_VALUE;

	private TypeDisplaySize() {
	}

	/**
	 * Returns the display size for a column type.
	 * Display sizes for numeric types match PostgreSQL JDBC exactly.
	 *
	 * @param dataType  the Firebolt data type
	 * @param precision the column precision (type modifier), 0 or negative if unknown
	 * @param scale     the column scale, 0 or negative if unknown
	 * @return the display size in characters
	 */
	public static int getDisplaySize(FireboltDataType dataType, int precision, int scale) {
		switch (dataType) {
			// Boolean - PostgreSQL returns 1 for BOOL
			case BOOLEAN:
			case U_INT_8:    // UInt8 is used for boolean internally
				return 1;

			// Integer types - display size includes sign
			// PostgreSQL: INT4 -> 11, INT8 -> 20
			case INTEGER:    // Int32 / INTEGER (also covers Int8, Int16 which map to INTEGER)
				return 11; // -2147483648 to +2147483647
			case BIG_INT:    // Int64 / BIGINT
			case BIG_INT_64:
			case UNSIGNED_BIG_INT_64:
				return 20; // -9223372036854775808 to +9223372036854775807

			// Floating point types - includes sign, digits, decimal, exponent
			// PostgreSQL: FLOAT4 -> 15, FLOAT8 -> 25
			case REAL:       // Float32 / REAL
				return 15; // sign + 9 digits + decimal point + e + sign + 2 digits
			case DOUBLE_PRECISION: // Float64 / DOUBLE PRECISION
				return 25; // sign + 18 digits + decimal point + e + sign + 3 digits

			// Text/String types
			case TEXT:
				// If precision is specified (VARCHAR(n)), use it
				// Otherwise return UNKNOWN_LENGTH for TEXT
				if (precision > 0) {
					return precision;
				}
				return UNKNOWN_LENGTH;

			// Date/Time types - PostgreSQL reference values
			case DATE:
			case DATE_32:
				return 13; // "4713-01-01 BC" format (PostgreSQL uses 13)
			case TIMESTAMP:
			case DATE_TIME_64:
				// timestamp without time zone: YYYY-MM-DD HH:MM:SS.ffffff
				return 26; // 10 (date) + 1 (space) + 15 (time with microseconds)
			case TIMESTAMP_WITH_TIMEZONE:
				// timestamp with time zone: YYYY-MM-DD HH:MM:SS.ffffff+HH:MM
				return 32; // 26 + 6 (timezone offset)

			// Numeric/Decimal
			// PostgreSQL: unbounded NUMERIC -> 131089
			case NUMERIC:
				if (precision > 0) {
					// sign + precision digits + decimal point (if scale > 0)
					return 1 + precision + (scale > 0 ? 1 : 0);
				}
				// PostgreSQL returns 131089 for unbounded NUMERIC
				return 131089;

			// Binary types - unlimited
			case BYTEA:
				return UNKNOWN_LENGTH;

			// Complex/unlimited types
			case ARRAY:
			case TUPLE:
			case STRUCT:
			case JSON:
			case GEOGRAPHY:
				return UNKNOWN_LENGTH;

			// Unknown or other types - return UNKNOWN_LENGTH
			case NOTHING:
			case UNKNOWN:
			default:
				return UNKNOWN_LENGTH;
		}
	}
}
