package com.firebolt.jdbc.resultset;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.TimeZone;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import com.firebolt.jdbc.type.FireboltDataType;

class FireboltColumnTest {

	@Test
	void shouldCreateColumDataForNullableString() {
		String type = "Nullable(String)";
		String name = "name";
		FireboltColumn column = FireboltColumn.of(type, name);
		assertEquals(name, column.getColumnName());
		assertEquals(type.toUpperCase(), column.getColumnType());
		assertEquals(FireboltDataType.STRING, column.getDataType());
		assertEquals("STRING", column.getCompactTypeName());
	}

	@Test
	void shouldCreateColumDataForArray() {
		String type = "Array(Array(Nullable(DateTime64(4, \\'EST\\'))))";
		String name = "name";
		FireboltColumn column = FireboltColumn.of(type, name);
		assertEquals(name, column.getColumnName());
		assertEquals(type.toUpperCase(), column.getColumnType());
		assertEquals(FireboltDataType.ARRAY, column.getDataType());
		assertEquals(FireboltDataType.DATE_TIME_64, column.getArrayBaseDataType());
		assertEquals(TimeZone.getTimeZone("EST"), column.getTimeZone());
		assertEquals("ARRAY(ARRAY(TIMESTAMP_EXT))", column.getCompactTypeName());
	}

	@Test
	void shouldCreateColumDataForArrayOfArrayOfInteger() {
		String type = "Array(Array(integer))";
		String name = "age";
		FireboltColumn column = FireboltColumn.of(type, name);
		assertEquals(name, column.getColumnName());
		assertEquals(type.toUpperCase(), column.getColumnType());
		assertEquals(FireboltDataType.ARRAY, column.getDataType());
		assertEquals(FireboltDataType.INT_32, column.getArrayBaseDataType());
		assertEquals("ARRAY(ARRAY(INTEGER))", column.getCompactTypeName());
	}

	@Test
	void shouldCreateColumDataForDecimalWithArgs() {
		String type = "Nullable(Decimal(1,2))";
		String name = "name";
		FireboltColumn column = FireboltColumn.of(type, name);
		assertEquals(name, column.getColumnName());
		assertEquals(type.toUpperCase(), column.getColumnType());
		assertEquals(FireboltDataType.DECIMAL, column.getDataType());
		assertEquals(1, column.getPrecision());
		assertEquals(2, column.getScale());
		assertEquals("DECIMAL(1,2)", column.getCompactTypeName());
	}

	@Test
	void shouldCreateColumDataForArrayOfArrayOfNullableDouble() {
		String type = "Array(Array(Nullable(double))";
		String name = "weight";
		FireboltColumn column = FireboltColumn.of(type, name);
		assertEquals(name, column.getColumnName());
		assertEquals(type.toUpperCase(), column.getColumnType());
		assertEquals(FireboltDataType.ARRAY, column.getDataType());
		assertEquals(FireboltDataType.FLOAT_64, column.getArrayBaseDataType());
		assertEquals("ARRAY(ARRAY(DOUBLE))", column.getCompactTypeName());
	}

	@Test
	void shouldCreateColumDataForBoolean() {
		String type = "Nullable(String)";
		String name = "name";
		FireboltColumn column = FireboltColumn.of(type, name);
		assertEquals(name, column.getColumnName());
		assertEquals(type.toUpperCase(), column.getColumnType());
		assertEquals(FireboltDataType.STRING, column.getDataType());
		assertEquals("STRING", column.getCompactTypeName());
	}

	@ParameterizedTest
	@CsvSource(value = { "Tuple(Int64);TUPLE(BIGINT)",
			"Tuple(Array(Tuple(int,int)), int);TUPLE(ARRAY(TUPLE(INTEGER, INTEGER)), INTEGER)",
			"Tuple(Array(int),Array(int));TUPLE(ARRAY(INTEGER), ARRAY(INTEGER))", "Tuple(Date);TUPLE(DATE)",
			"Tuple(Array(int), Array(Nullable(Int64)), Int64);TUPLE(ARRAY(INTEGER), ARRAY(BIGINT), BIGINT)",
			"Tuple(Array(int), Array(Nullable(Int64)));TUPLE(ARRAY(INTEGER), ARRAY(BIGINT))" }, delimiter = ';')
	void shouldCreateColumDataForTupleOfArrayOfTuple(String inputType, String expectedType) {
		String name = "name";
		FireboltColumn column = FireboltColumn.of(inputType, name);
		assertEquals(name, column.getColumnName());
		assertEquals(inputType.toUpperCase(), column.getColumnType());
		assertEquals(FireboltDataType.TUPLE, column.getDataType());
		assertEquals(expectedType, column.getCompactTypeName());
	}

	@ParameterizedTest
	@CsvSource(value = { "ARRAY(ARRAY(ARRAY(TEXT)));ARRAY(ARRAY(ARRAY(STRING)))",
			"Array(Tuple(Array(Tuple(UInt8, String)),Array(Tuple(UInt8, String))));ARRAY(TUPLE(ARRAY(TUPLE(INTEGER, STRING)), ARRAY(TUPLE(INTEGER, STRING))))",
			"Array(Tuple(UInt8, String));ARRAY(TUPLE(INTEGER, STRING))",
			"Array(Array(Tuple(UInt8, String)));ARRAY(ARRAY(TUPLE(INTEGER, STRING)))" }, delimiter = ';')
	void shouldCreateColumDataForArrayOfArrayOfArray(String inputType, String expectedType) {
		String name = "name";
		FireboltColumn column = FireboltColumn.of(inputType, name);
		assertEquals(name, column.getColumnName());
		assertEquals(inputType.toUpperCase(), column.getColumnType());
		assertEquals(FireboltDataType.ARRAY, column.getDataType());
		assertEquals(expectedType, column.getCompactTypeName());
	}

	@Test
	void shouldCreateColumDataForDateTime64() {
		String type = "DateTime64(4, \\'EST\\')";
		String name = "my_d";
		FireboltColumn column = FireboltColumn.of(type, name);
		assertEquals(name, column.getColumnName());
		assertEquals(type.toUpperCase(), column.getColumnType());
		assertEquals(FireboltDataType.DATE_TIME_64, column.getDataType());
		assertEquals(TimeZone.getTimeZone("EST"), column.getTimeZone());
		assertEquals(4, column.getScale());
		assertEquals(23, column.getPrecision());
	}

	@Test
	void shouldCreateColumDataForDateTime64WithoutTimeZone() {
		String type = "Nullable(DateTime64(6))";
		String name = "my_d";
		FireboltColumn column = FireboltColumn.of(type, name);
		assertEquals(name, column.getColumnName());
		assertEquals(type.toUpperCase(), column.getColumnType());
		assertNull(column.getTimeZone());
		assertEquals(6, column.getScale());
		assertEquals(25, column.getPrecision());
	}

	@Test
	void shouldCreateColumDataForDateWithoutTimeZone() {
		String type = "Nullable(Date)";
		String name = "my_d";
		FireboltColumn column = FireboltColumn.of(type, name);
		assertEquals(name, column.getColumnName());
		assertEquals(type.toUpperCase(), column.getColumnType());
		assertNull(column.getTimeZone());
	}

	@Test
	void shouldCreateColumDataForDateTimeWithTimeZone() {
		String type = "DateTime(\\'EST\\')";
		String name = "my_d";
		FireboltColumn column = FireboltColumn.of(type, name);
		assertEquals(name, column.getColumnName());
		assertEquals(type.toUpperCase(), column.getColumnType());
		assertEquals(FireboltDataType.DATE_TIME, column.getDataType());
		assertEquals(TimeZone.getTimeZone("EST"), column.getTimeZone());
	}
	@Test
	void shouldCreateColumDataForNullableDateTimeWithTimeZone() {
		String type = "Nullable(DateTime(\\'EST\\'))";
		String name = "my_d";
		FireboltColumn column = FireboltColumn.of(type, name);
		assertEquals(name, column.getColumnName());
		assertEquals(type.toUpperCase(), column.getColumnType());
		assertEquals(FireboltDataType.DATE_TIME, column.getDataType());
		assertEquals(TimeZone.getTimeZone("EST"), column.getTimeZone());
	}

	@Test
	void shouldCreateColumDataForDateTimeWithoutTimezoneWhenTheTimezoneIsInvalid() {
		String type = "DateTime(\\'HelloTz\\')";
		String name = "my_d";
		FireboltColumn column = FireboltColumn.of(type, name);
		assertEquals(name, column.getColumnName());
		assertEquals(type.toUpperCase(), column.getColumnType());
		assertEquals(FireboltDataType.DATE_TIME, column.getDataType());
		assertNull(column.getTimeZone());
	}
}
