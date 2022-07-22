package com.firebolt.jdbc.resultset;

import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class FireboltResultSetMetaDataTest {

  @Test
  void shouldReturnColumnCount() throws SQLException {
    FireboltResultSetMetaData fireboltResultSetMetaData =
        FireboltResultSetMetaData.builder()
            .columns(getColumns())
            .tableName("table-name")
            .dbName("db-name")
            .build();
    assertEquals(3, fireboltResultSetMetaData.getColumnCount());
  }

  @Test
  void shouldReturn1WhenColumnIsNullable() throws SQLException {
    FireboltResultSetMetaData fireboltResultSetMetaData =
        FireboltResultSetMetaData.builder()
            .columns(getColumns())
            .tableName("table-name")
            .dbName("db-name")
            .build();
    assertEquals(1, fireboltResultSetMetaData.isNullable(1));
  }

  @Test
  void shouldReturn0WhenColumnIsNotNullable() throws SQLException {
    FireboltResultSetMetaData fireboltResultSetMetaData =
        FireboltResultSetMetaData.builder()
            .columns(getColumns())
            .tableName("table-name")
            .dbName("db-name")
            .build();
    assertEquals(0, fireboltResultSetMetaData.isNullable(2));
  }

  @Test
  void shouldReturnTrueWhenColumnSigned() throws SQLException {
    FireboltResultSetMetaData fireboltResultSetMetaData =
        FireboltResultSetMetaData.builder()
            .columns(getColumns())
            .tableName("table-name")
            .dbName("db-name")
            .build();
    assertTrue(fireboltResultSetMetaData.isSigned(2));
  }

  @Test
  void shouldReturnTrueWhenColumnNotSigned() throws SQLException {
    FireboltResultSetMetaData fireboltResultSetMetaData =
        FireboltResultSetMetaData.builder()
            .columns(getColumns())
            .tableName("table-name")
            .dbName("db-name")
            .build();
    assertFalse(fireboltResultSetMetaData.isSigned(1));
  }

  @Test
  void shouldReturnColumnNameAndLabel() throws SQLException {
    FireboltResultSetMetaData fireboltResultSetMetaData =
        FireboltResultSetMetaData.builder()
            .columns(getColumns())
            .tableName("table-name")
            .dbName("db-name")
            .build();
    assertEquals("name", fireboltResultSetMetaData.getColumnName(1));
    assertEquals("name", fireboltResultSetMetaData.getColumnLabel(1));
  }

  @Test
  void shouldReturnEmptyWhenGettingSchemaAsItIsNotSupported() throws SQLException {
    FireboltResultSetMetaData fireboltResultSetMetaData =
        FireboltResultSetMetaData.builder()
            .columns(getColumns())
            .tableName("table-name")
            .dbName("db-name")
            .build();
    assertEquals(StringUtils.EMPTY, fireboltResultSetMetaData.getSchemaName(1));
  }

  @Test
  void ShouldReturnScale() throws SQLException {
    FireboltResultSetMetaData fireboltResultSetMetaData =
        FireboltResultSetMetaData.builder()
            .columns(getColumns())
            .tableName("table-name")
            .dbName("db-name")
            .build();
    assertEquals(2, fireboltResultSetMetaData.getScale(3));
  }

  @Test
  void ShouldReturnPrecision() throws SQLException {
    FireboltResultSetMetaData fireboltResultSetMetaData =
        FireboltResultSetMetaData.builder()
            .columns(getColumns())
            .tableName("table-name")
            .dbName("db-name")
            .build();
    assertEquals(1, fireboltResultSetMetaData.getPrecision(3));
  }

  @Test
  void ShouldReturnTableName() throws SQLException {
    FireboltResultSetMetaData fireboltResultSetMetaData =
        FireboltResultSetMetaData.builder()
            .columns(getColumns())
            .tableName("table-name")
            .dbName("db-name")
            .build();
    assertEquals("table-name", fireboltResultSetMetaData.getTableName());
  }

  @Test
  void ShouldReturnDbName() throws SQLException {
    FireboltResultSetMetaData fireboltResultSetMetaData =
        FireboltResultSetMetaData.builder()
            .columns(getColumns())
            .tableName("table-name")
            .dbName("db-name")
            .build();
    assertEquals("db-name", fireboltResultSetMetaData.getDbName());
  }

  @Test
  void ShouldReturnColumnType() throws SQLException {
    FireboltResultSetMetaData fireboltResultSetMetaData =
        FireboltResultSetMetaData.builder()
            .columns(getColumns())
            .tableName("table-name")
            .dbName("db-name")
            .build();
    assertEquals(Types.VARCHAR, fireboltResultSetMetaData.getColumnType(1));
  }

  @Test
  void ShouldReturnColumnClassName() throws SQLException {
    FireboltResultSetMetaData fireboltResultSetMetaData =
        FireboltResultSetMetaData.builder()
            .columns(getColumns())
            .tableName("table-name")
            .dbName("db-name")
            .build();
    assertEquals("java.math.BigDecimal", fireboltResultSetMetaData.getColumnClassName(3));
  }

  private List<FireboltColumn> getColumns() {
    return Arrays.asList(
        FireboltColumn.of("Nullable(String)", "name"),
        FireboltColumn.of("Integer", "age"),
        FireboltColumn.of("Decimal(1,2)", "Weight"));
  }
}
