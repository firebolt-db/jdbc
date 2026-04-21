package com.firebolt.jdbc.type;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests for {@link TypeDisplaySize}.
 * Display sizes follow PostgreSQL JDBC conventions.
 *
 * @see <a href="https://github.com/pgjdbc/pgjdbc/blob/master/pgjdbc/src/main/java/org/postgresql/jdbc/TypeInfoCache.java#L658-L735">pgjdbc TypeInfoCache.getDisplaySize()</a>
 */
class TypeDisplaySizeTest {

	@Test
	void booleanDisplaySize() {
		// PostgreSQL returns 1 for BOOL
		assertEquals(1, TypeDisplaySize.getDisplaySize(FireboltDataType.BOOLEAN, 0, 0));
	}

	@Test
	void uint8DisplaySize() {
		// UInt8 is used internally for boolean
		assertEquals(1, TypeDisplaySize.getDisplaySize(FireboltDataType.U_INT_8, 0, 0));
	}

	@Test
	void integerDisplaySize() {
		// PostgreSQL INT4 -> 11 (-2147483648 to +2147483647)
		assertEquals(11, TypeDisplaySize.getDisplaySize(FireboltDataType.INTEGER, 0, 0));
	}

	@Test
	void bigIntDisplaySize() {
		// PostgreSQL INT8 -> 20
		assertEquals(20, TypeDisplaySize.getDisplaySize(FireboltDataType.BIG_INT, 0, 0));
	}

	@Test
	void bigInt64DisplaySize() {
		// Legacy type, same as BIG_INT
		assertEquals(20, TypeDisplaySize.getDisplaySize(FireboltDataType.BIG_INT_64, 0, 0));
	}

	@Test
	void unsignedBigInt64DisplaySize() {
		// Legacy type, same as BIG_INT
		assertEquals(20, TypeDisplaySize.getDisplaySize(FireboltDataType.UNSIGNED_BIG_INT_64, 0, 0));
	}

	@Test
	void realDisplaySize() {
		// PostgreSQL FLOAT4 -> 15
		assertEquals(15, TypeDisplaySize.getDisplaySize(FireboltDataType.REAL, 0, 0));
	}

	@Test
	void doublePrecisionDisplaySize() {
		// PostgreSQL FLOAT8 -> 25
		assertEquals(25, TypeDisplaySize.getDisplaySize(FireboltDataType.DOUBLE_PRECISION, 0, 0));
	}

	@Test
	void textWithoutPrecisionDisplaySize() {
		// TEXT without precision returns Integer.MAX_VALUE (unlimited)
		assertEquals(Integer.MAX_VALUE, TypeDisplaySize.getDisplaySize(FireboltDataType.TEXT, 0, 0));
		assertEquals(Integer.MAX_VALUE, TypeDisplaySize.getDisplaySize(FireboltDataType.TEXT, -1, 0));
	}

	@Test
	void textWithPrecisionDisplaySize() {
		// VARCHAR(n) returns n
		assertEquals(100, TypeDisplaySize.getDisplaySize(FireboltDataType.TEXT, 100, 0));
		assertEquals(255, TypeDisplaySize.getDisplaySize(FireboltDataType.TEXT, 255, 0));
	}

	@Test
	void dateDisplaySize() {
		// PostgreSQL DATE -> 13
		assertEquals(13, TypeDisplaySize.getDisplaySize(FireboltDataType.DATE, 0, 0));
	}

	@Test
	void date32DisplaySize() {
		// Legacy type, same as DATE
		assertEquals(13, TypeDisplaySize.getDisplaySize(FireboltDataType.DATE_32, 0, 0));
	}

	@Test
	void timestampDisplaySize() {
		// TIMESTAMP without time zone: YYYY-MM-DD HH:MM:SS.ffffff -> 26
		assertEquals(26, TypeDisplaySize.getDisplaySize(FireboltDataType.TIMESTAMP, 0, 0));
	}

	@Test
	void dateTime64DisplaySize() {
		// Legacy type, same as TIMESTAMP
		assertEquals(26, TypeDisplaySize.getDisplaySize(FireboltDataType.DATE_TIME_64, 0, 0));
	}

	@Test
	void timestampWithTimezoneDisplaySize() {
		// TIMESTAMP with time zone: adds +HH:MM -> 32
		assertEquals(32, TypeDisplaySize.getDisplaySize(FireboltDataType.TIMESTAMP_WITH_TIMEZONE, 0, 0));
	}

	@ParameterizedTest
	@CsvSource({
			"10, 2, 12",  // sign + 10 digits + decimal point = 12
			"5, 0, 6",    // sign + 5 digits + no decimal = 6
			"38, 10, 40"  // sign + 38 digits + decimal point = 40
	})
	void numericWithPrecisionDisplaySize(int precision, int scale, int expectedDisplaySize) {
		assertEquals(expectedDisplaySize, TypeDisplaySize.getDisplaySize(FireboltDataType.NUMERIC, precision, scale));
	}

	@Test
	void numericWithoutPrecisionDisplaySize() {
		// PostgreSQL returns 131089 for unbounded NUMERIC
		assertEquals(131089, TypeDisplaySize.getDisplaySize(FireboltDataType.NUMERIC, 0, 0));
	}

	@Test
	void byteaDisplaySize() {
		// BYTEA is unlimited
		assertEquals(Integer.MAX_VALUE, TypeDisplaySize.getDisplaySize(FireboltDataType.BYTEA, 0, 0));
	}

	@Test
	void arrayDisplaySize() {
		// ARRAY is unlimited
		assertEquals(Integer.MAX_VALUE, TypeDisplaySize.getDisplaySize(FireboltDataType.ARRAY, 0, 0));
	}

	@Test
	void tupleDisplaySize() {
		// TUPLE is unlimited
		assertEquals(Integer.MAX_VALUE, TypeDisplaySize.getDisplaySize(FireboltDataType.TUPLE, 0, 0));
	}

	@Test
	void structDisplaySize() {
		// STRUCT is unlimited
		assertEquals(Integer.MAX_VALUE, TypeDisplaySize.getDisplaySize(FireboltDataType.STRUCT, 0, 0));
	}

	@Test
	void jsonDisplaySize() {
		// JSON is unlimited
		assertEquals(Integer.MAX_VALUE, TypeDisplaySize.getDisplaySize(FireboltDataType.JSON, 0, 0));
	}

	@Test
	void geographyDisplaySize() {
		// GEOGRAPHY is unlimited
		assertEquals(Integer.MAX_VALUE, TypeDisplaySize.getDisplaySize(FireboltDataType.GEOGRAPHY, 0, 0));
	}

	@Test
	void nothingDisplaySize() {
		// NULL type returns unlimited
		assertEquals(Integer.MAX_VALUE, TypeDisplaySize.getDisplaySize(FireboltDataType.NOTHING, 0, 0));
	}

	@Test
	void unknownDisplaySize() {
		// Unknown type returns unlimited
		assertEquals(Integer.MAX_VALUE, TypeDisplaySize.getDisplaySize(FireboltDataType.UNKNOWN, 0, 0));
	}
}
