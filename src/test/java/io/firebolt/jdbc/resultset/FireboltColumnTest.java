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
    assertEquals(type, column.getColumnType());
    assertEquals(FireboltDataType.STRING, column.getFireboltDataType());
    assertEquals("String", column.getCompactTypeName());
  }

  @Test
  void shouldCreateColumDataForArray() {
    String type = "Array(Array(Nullable(String)))";
    String name = "name";
    FireboltColumn column = FireboltColumn.of(type, name);
    assertEquals(name, column.getColumnName());
    assertEquals(type, column.getColumnType());
    assertEquals(FireboltDataType.STRING, column.getArrayType());
    assertEquals("Array(Array(String))", column.getCompactTypeName());
  }

  @Test
  void shouldCreateColumDataForDecimalWithArgs() {
    String type = "Nullable(Decimal(1,2))";
    String name = "name";
    FireboltColumn column = FireboltColumn.of(type, name);
    assertEquals(name, column.getColumnName());
    assertEquals(type, column.getColumnType());
    assertEquals(FireboltDataType.DECIMAL, column.getFireboltDataType());
    assertEquals(1, column.getPrecision());
    assertEquals(2, column.getScale());
    assertEquals("Decimal(1,2)", column.getCompactTypeName());
  }
}
