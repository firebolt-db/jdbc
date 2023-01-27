package com.firebolt.jdbc.resultset;

import static org.junit.jupiter.api.Assertions.*;

import java.util.TimeZone;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import com.firebolt.jdbc.resultset.column.ColumnType;
import com.firebolt.jdbc.type.FireboltDataType;

class ColumnTypeTest {

	@Test
	void shouldCreateColumDataForNullableString() {
		String type = "Nullable(String)";
		ColumnType columnType = ColumnType.of(type);
		assertEquals(type.toUpperCase(), columnType.getName());
		assertEquals(FireboltDataType.STRING, columnType.getDataType());
		assertEquals("STRING", columnType.getCompactTypeName());
	}

	@Test
	void shouldCreateColumDataForArray() {
		String type = "Array(Array(Nullable(DateTime64(4, \\'EST\\'))))";
		ColumnType columnType = ColumnType.of(type);
		assertEquals(type.toUpperCase(), columnType.getName());
		assertEquals(FireboltDataType.ARRAY, columnType.getDataType());
		assertEquals(FireboltDataType.DATE_TIME_64, columnType.getArrayBaseColumnType().getDataType());
		assertEquals(TimeZone.getTimeZone("EST"), columnType.getArrayBaseColumnType().getTimeZone());
		assertEquals("ARRAY(ARRAY(TIMESTAMP_EXT))", columnType.getCompactTypeName());
	}

	@Test
	void shouldCreateColumDataForArrayOfArrayOfInteger() {
		String type = "Array(Array(integer))";
		ColumnType columnType = ColumnType.of(type);
		assertEquals(type.toUpperCase(), columnType.getName());
		assertEquals(FireboltDataType.ARRAY, columnType.getDataType());
		assertEquals(FireboltDataType.INT_32, columnType.getArrayBaseColumnType().getDataType());
		assertEquals("ARRAY(ARRAY(INTEGER))", columnType.getCompactTypeName());
	}

	@Test
	void shouldCreateColumDataForDecimalWithArgs() {
		String type = "Nullable(Decimal(1,2))";
		ColumnType columnType = ColumnType.of(type);
		assertEquals(type.toUpperCase(), columnType.getName());
		assertEquals(FireboltDataType.DECIMAL, columnType.getDataType());
		assertEquals(1, columnType.getPrecision());
		assertEquals(2, columnType.getScale());
		assertEquals("DECIMAL", columnType.getCompactTypeName());
	}

	@Test
	void shouldCreateColumDataForArrayOfArrayOfNullableDouble() {
		String type = "Array(Array(Nullable(double)))";
		ColumnType columnType = ColumnType.of(type);
		assertEquals(type.toUpperCase(), columnType.getName());
		assertEquals(FireboltDataType.ARRAY, columnType.getDataType());
		assertEquals(FireboltDataType.FLOAT_64, columnType.getArrayBaseColumnType().getDataType());
		assertEquals("ARRAY(ARRAY(DOUBLE))", columnType.getCompactTypeName());
	}

	@Test
	void shouldCreateColumDataForNullableArrayOfNullableArrayOfNullableDouble() {
		String type = "Nullable(Array(Nullable(Array(Nullable(double)))))";
		ColumnType columnType = ColumnType.of(type);
		assertEquals(type.toUpperCase(), columnType.getName());
		assertEquals(FireboltDataType.ARRAY, columnType.getDataType());
		assertEquals(FireboltDataType.FLOAT_64, columnType.getArrayBaseColumnType().getDataType());
		assertEquals("ARRAY(ARRAY(DOUBLE))", columnType.getCompactTypeName());
	}

	@Test
	void shouldCreateColumDataForBoolean() {
		String type = "Nullable(String)";
		ColumnType columnType = ColumnType.of(type);
		assertEquals(type.toUpperCase(), columnType.getName());
		assertEquals(FireboltDataType.STRING, columnType.getDataType());
		assertEquals("STRING", columnType.getCompactTypeName());
	}

	@ParameterizedTest
	@CsvSource(value = {
			"Tuple(Array(Tuple(int,int)), int);TUPLE(ARRAY(TUPLE(INTEGER, INTEGER)), INTEGER)" }, delimiter = ';')
	void shouldCreateColumDataForTupleOfArrayOfTuple(String inputType, String expectedType) {
		ColumnType type = ColumnType.of(inputType);
		assertEquals(inputType.toUpperCase(), type.getName());
		assertEquals(FireboltDataType.TUPLE, type.getDataType());
		assertEquals(expectedType, type.getCompactTypeName());
	}

	@ParameterizedTest
	@CsvSource(value = { "ARRAY(ARRAY(ARRAY(TEXT)));ARRAY(ARRAY(ARRAY(STRING)))",
			"Array(Tuple(Array(Tuple(UInt8, String)),Array(Tuple(UInt8, String))));ARRAY(TUPLE(ARRAY(TUPLE(INTEGER, STRING)), ARRAY(TUPLE(INTEGER, STRING))))",
			"Array(Tuple(UInt8, String));ARRAY(TUPLE(INTEGER, STRING))",
			"Array(Array(Tuple(UInt8, String)));ARRAY(ARRAY(TUPLE(INTEGER, STRING)))" }, delimiter = ';')
	void shouldCreateColumDataForArrayOfArrayOfArray(String inputType, String expectedType) {
		ColumnType columnType = ColumnType.of(inputType);
		assertEquals(inputType.toUpperCase(), columnType.getName());
		assertEquals(FireboltDataType.ARRAY, columnType.getDataType());
		assertEquals(expectedType, columnType.getCompactTypeName());
	}

	@Test
	void shouldCreateColumDataForDateTime64() {
		String type = "DateTime64(4, \\'EST\\')";
		ColumnType columnType = ColumnType.of(type);
		assertEquals(type.toUpperCase(), columnType.getName());
		assertEquals(FireboltDataType.DATE_TIME_64, columnType.getDataType());
		assertEquals(TimeZone.getTimeZone("EST"), columnType.getTimeZone());
		assertEquals(4, columnType.getScale());
		assertEquals(23, columnType.getPrecision());
		assertEquals("TIMESTAMP_EXT", columnType.getCompactTypeName());
	}

	@Test
	void shouldCreateColumDataForDateTime64WithoutTimeZone() {
		String type = "Nullable(DateTime64(6))";
		ColumnType columnType = ColumnType.of(type);
		assertEquals(type.toUpperCase(), columnType.getName());
		assertNull(columnType.getTimeZone());
		assertEquals(6, columnType.getScale());
		assertEquals(25, columnType.getPrecision());
	}

	@Test
	void shouldCreateColumDataForDateWithoutTimeZone() {
		String type = "Nullable(Date)";
		ColumnType columnType = ColumnType.of(type);
		assertEquals(type.toUpperCase(), columnType.getName());
		assertNull(columnType.getTimeZone());
	}

	@Test
	void shouldCreateColumDataForDateTimeWithTimeZone() {
		String type = "DateTime(\\'EST\\')";
		ColumnType columnType = ColumnType.of(type);
		assertEquals(type.toUpperCase(), columnType.getName());
		assertEquals(FireboltDataType.DATE_TIME, columnType.getDataType());
		assertEquals(TimeZone.getTimeZone("EST"), columnType.getTimeZone());
	}

	@Test
	void shouldCreateColumDataForNullableDateTimeWithTimeZone() {
		String type = "Nullable(DateTime(\\'EST\\'))";
		ColumnType columnType = ColumnType.of(type);
		assertEquals(type.toUpperCase(), columnType.getName());
		assertEquals(FireboltDataType.DATE_TIME, columnType.getDataType());
		assertEquals(TimeZone.getTimeZone("EST"), columnType.getTimeZone());
	}

	@Test
	void shouldCreateColumDataForArrayOfDateTimeWithTimeZone() {
		String type = "Array(Nullable(DateTime(\\'EST\\')))";
		ColumnType columnType = ColumnType.of(type);
		assertEquals(type.toUpperCase(), columnType.getName());
		assertEquals(FireboltDataType.DATE_TIME, columnType.getArrayBaseColumnType().getDataType());
		assertEquals(TimeZone.getTimeZone("EST"), columnType.getArrayBaseColumnType().getTimeZone());
	}

	@Test
	void shouldCreateColumDataForDateTimeWithoutTimezoneWhenTheTimezoneIsInvalid() {
		String type = "DateTime(\\'HelloTz\\')";
		ColumnType columnType = ColumnType.of(type);
		assertEquals(type.toUpperCase(), columnType.getName());
		assertEquals(FireboltDataType.DATE_TIME, columnType.getDataType());
		assertNull(columnType.getTimeZone());
	}

	@Test
	void shouldCreateColumDataForArrayWithoutNullKeywordWhenIntInArrayIsNullable() {
		String type = "Array(INTEGER NULL)";
		ColumnType columnType = ColumnType.of(type);
		assertEquals(type.toUpperCase(), columnType.getName());
		assertEquals(FireboltDataType.ARRAY, columnType.getDataType());
		assertEquals(FireboltDataType.INT_32, columnType.getInnerTypes().get(0).getDataType());
		assertTrue(columnType.getInnerTypes().get(0).isNullable());
		assertFalse(columnType.isNullable());
		assertEquals("ARRAY(INTEGER)", columnType.getCompactTypeName());

	}

	@Test
	void shouldCreateColumDataForArrayWithoutNullKeywordWhenIntInArrayIsNotNullable() {
		String type = "Array(INTEGER NOT NULL)";
		ColumnType columnType = ColumnType.of(type);
		assertEquals(type.toUpperCase(), columnType.getName());
		assertEquals(FireboltDataType.ARRAY, columnType.getDataType());
		assertEquals(FireboltDataType.INT_32, columnType.getInnerTypes().get(0).getDataType());
		assertFalse(columnType.getInnerTypes().get(0).isNullable());
		assertFalse(columnType.isNullable());
		assertEquals("ARRAY(INTEGER)", columnType.getCompactTypeName());
	}

	@Test
	void shouldCreateColumnTypeForBoolean() {
		String type = "Nullable(Boolean)";
		ColumnType columnType = ColumnType.of(type);
		assertEquals(FireboltDataType.BOOLEAN, columnType.getDataType());
		assertTrue(columnType.isNullable());
	}

	@Test
	void shouldCreateColumnTypeForNull() {
		String type = "null";
		ColumnType columnType = ColumnType.of(type);
		assertEquals(FireboltDataType.NOTHING, columnType.getDataType());
		assertTrue(columnType.isNullable());
	}

	@Test
	void shouldCreateColumnTypeForNothingNull() {
		String type = "nothing null";
		ColumnType columnType = ColumnType.of(type);
		assertEquals(FireboltDataType.NOTHING, columnType.getDataType());
		assertTrue(columnType.isNullable());
	}
}
