package com.firebolt.jdbc.resultset;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

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
		String type = "Array(Array(Nullable(String)))";
		String name = "name";
		FireboltColumn column = FireboltColumn.of(type, name);
		assertEquals(name, column.getColumnName());
		assertEquals(type.toUpperCase(), column.getColumnType());
		assertEquals(FireboltDataType.ARRAY, column.getDataType());
		assertEquals(FireboltDataType.STRING, column.getArrayBaseDataType());
		assertEquals("ARRAY(ARRAY(STRING))", column.getCompactTypeName());
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
	void shouldCreateColumDataForTupleOfArray() {
		String type = "Tuple(Array(int), Array(Nullable(Int64)))";
		String name = "my_tuple";
		FireboltColumn column = FireboltColumn.of(type, name);
		assertEquals(name, column.getColumnName());
		assertEquals(type.toUpperCase(), column.getColumnType());
		assertEquals(FireboltDataType.TUPLE, column.getDataType());
		assertEquals("TUPLE(ARRAY(INTEGER), ARRAY(BIGINT))", column.getCompactTypeName());
	}

	@Test
	void shouldCreateColumDataForTupleWithMoreThan2Elements() {
		String type = "Tuple(Array(int), Array(Nullable(Int64)), Int64)";
		String name = "my_tuple";
		FireboltColumn column = FireboltColumn.of(type, name);
		assertEquals(name, column.getColumnName());
		assertEquals(type.toUpperCase(), column.getColumnType());
		assertEquals(FireboltDataType.TUPLE, column.getDataType());
		assertEquals("TUPLE(ARRAY(INTEGER), ARRAY(BIGINT), BIGINT)", column.getCompactTypeName());
	}

	@Test
	void shouldCreateColumDataForTupleWithOneElement() {
		String type = "Tuple(Int64)";
		String name = "my_tuple";
		FireboltColumn column = FireboltColumn.of(type, name);
		assertEquals(name, column.getColumnName());
		assertEquals(type.toUpperCase(), column.getColumnType());
		assertEquals(FireboltDataType.TUPLE, column.getDataType());
		assertEquals("TUPLE(BIGINT)", column.getCompactTypeName());
	}

	@Test
	void shouldCreateColumDataForTupleWithOnlyOneArgument() {
		String type = "Tuple(Date)";
		String name = "TUPLE(CAST('2019-02-03' AS timestamp))";
		FireboltColumn column = FireboltColumn.of(type, name);
		assertEquals(name, column.getColumnName());
		assertEquals(type.toUpperCase(), column.getColumnType());
		assertEquals(FireboltDataType.TUPLE, column.getDataType());
		assertEquals("TUPLE(DATE)", column.getCompactTypeName());
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

	@Test
	void shouldCreateColumDataForTupleOfArrays() {
		String type = "Tuple(Array(int),Array(int))";
		String name = "name";
		FireboltColumn column = FireboltColumn.of(type, name);
		assertEquals(name, column.getColumnName());
		assertEquals(type.toUpperCase(), column.getColumnType());
		assertEquals(FireboltDataType.TUPLE, column.getDataType());
		assertEquals("TUPLE(ARRAY(INTEGER), ARRAY(INTEGER))", column.getCompactTypeName());
	}

	@Test
	void shouldCreateColumDataForTupleOfArrayOfTuple() {
		String type = "Tuple(Array(Tuple(int,int)), int)";
		String name = "name";
		FireboltColumn column = FireboltColumn.of(type, name);
		assertEquals(name, column.getColumnName());
		assertEquals(type.toUpperCase(), column.getColumnType());
		assertEquals(FireboltDataType.TUPLE, column.getDataType());
		assertEquals("TUPLE(ARRAY(TUPLE(INTEGER, INTEGER)), INTEGER)", column.getCompactTypeName());
	}

	@Test
	void shouldCreateColumDataForArrayOfArrayOfArray() {
		String type = "ARRAY(ARRAY(ARRAY(TEXT)))";
		String name = "name";
		FireboltColumn column = FireboltColumn.of(type, name);
		assertEquals(name, column.getColumnName());
		assertEquals(type.toUpperCase(), column.getColumnType());
		assertEquals(FireboltDataType.ARRAY, column.getDataType());
		assertEquals("ARRAY(ARRAY(ARRAY(STRING)))", column.getCompactTypeName());
	}

	@Test
	void shouldCreateColumDataForArrayOfTuple() {
		String type = "Array(Tuple(UInt8, String))";
		String name = "ARRAY_OF_TUPLE";
		FireboltColumn column = FireboltColumn.of(type, name);
		assertEquals(name, column.getColumnName());
		assertEquals(type.toUpperCase(), column.getColumnType());
		assertEquals(FireboltDataType.ARRAY, column.getDataType());
		assertEquals("ARRAY(TUPLE(INTEGER, STRING))", column.getCompactTypeName());
	}

	@Test
	void shouldCreateColumDataForArrayOfArrayOfTuple() {
		String type = "Array(Array(Tuple(UInt8, String)))";
		String name = "COMPLEX_ARRAY";
		FireboltColumn column = FireboltColumn.of(type, name);
		assertEquals(name, column.getColumnName());
		assertEquals(type.toUpperCase(), column.getColumnType());
		assertEquals(FireboltDataType.ARRAY, column.getDataType());
		assertEquals("ARRAY(ARRAY(TUPLE(INTEGER, STRING)))", column.getCompactTypeName());
	}

	@Test
	void shouldCreateColumDataForArrayOfTupleOfArrayOfTuple() {
		String type = "Array(Tuple(Array(Tuple(UInt8, String)),Array(Tuple(UInt8, String))))";
		String name = "COMPLEX_ARRAY";
		FireboltColumn column = FireboltColumn.of(type, name);
		assertEquals(name, column.getColumnName());
		assertEquals(type.toUpperCase(), column.getColumnType());
		assertEquals(FireboltDataType.ARRAY, column.getDataType());
		assertEquals("ARRAY(TUPLE(ARRAY(TUPLE(INTEGER, STRING)), ARRAY(TUPLE(INTEGER, STRING))))",
				column.getCompactTypeName());
	}

}
