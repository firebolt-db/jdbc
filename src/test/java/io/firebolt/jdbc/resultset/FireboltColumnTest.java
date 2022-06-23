package io.firebolt.jdbc.resultset;

import io.firebolt.jdbc.resultset.type.FireboltDataType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
  void shouldCreateColumDataForTuple() {
    String type = "Tuple(Array(int), Array(Nullable(INT_64)))";
    String name = "my_tuple";
    FireboltColumn column = FireboltColumn.of(type, name);
    assertEquals(name, column.getColumnName());
    assertEquals(type.toUpperCase(), column.getColumnType());
    assertEquals(FireboltDataType.TUPLE, column.getDataType());
    assertEquals("TUPLE(ARRAY(INTEGER), ARRAY(BIGINT))", column.getCompactTypeName());
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
  void shouldGetCompactTextForArrayOfArrayOfArrayOfText() {
    String type = "ARRAY(ARRAY(ARRAY(TEXT)))";
    String name = "name";
    FireboltColumn column = FireboltColumn.of(type, name);
    assertEquals(name, column.getColumnName());
    assertEquals(type.toUpperCase(), column.getColumnType());
    assertEquals(FireboltDataType.ARRAY, column.getDataType());
    assertEquals("ARRAY(ARRAY(ARRAY(STRING)))", column.getCompactTypeName());
  }
}
