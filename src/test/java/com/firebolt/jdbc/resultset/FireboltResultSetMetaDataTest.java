package com.firebolt.jdbc.resultset;

import static java.sql.ResultSetMetaData.columnNoNulls;
import static java.sql.ResultSetMetaData.columnNullable;
import static org.junit.jupiter.api.Assertions.*;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Test;

import com.firebolt.jdbc.resultset.column.Column;
import org.junit.jupiter.params.ParameterizedTest;

class FireboltResultSetMetaDataTest {

	@Test
	void shouldReturnColumnCount() throws SQLException {
		assertEquals(3, getMetaData().getColumnCount());
	}

	@Test
	void shouldReturn1WhenColumnIsNullable() throws SQLException {
		assertEquals(columnNullable, getMetaData().isNullable(1));
	}

	@Test
	void shouldReturn0WhenColumnIsNotNullable() throws SQLException {
		assertEquals(columnNoNulls, getMetaData().isNullable(2));
	}

	@Test
	void shouldReturnTrueWhenColumnSigned() throws SQLException {
		assertTrue(getMetaData().isSigned(2));
	}

	@Test
	void shouldReturnTrueWhenColumnNotSigned() throws SQLException {
		assertFalse(getMetaData().isSigned(1));
	}

	@Test
	void shouldReturnColumnNameAndLabel() throws SQLException {
		FireboltResultSetMetaData fireboltResultSetMetaData = getMetaData();
		assertEquals("name", fireboltResultSetMetaData.getColumnName(1));
		assertEquals("name", fireboltResultSetMetaData.getColumnLabel(1));
	}

	@Test
	void shouldReturnEmptyWhenGettingSchemaAsItIsNotSupported() throws SQLException {
		assertEquals(StringUtils.EMPTY, getMetaData().getSchemaName(1));
	}

	@Test
	void shouldReturnScale() throws SQLException {
		assertEquals(2, getMetaData().getScale(3));
	}

	@Test
	void shouldReturnPrecision() throws SQLException {
		assertEquals(1, getMetaData().getPrecision(3));
	}

	@Test
	void shouldReturnTableName() throws SQLException {
		assertEquals("table-name", getMetaData().getTableName());
	}

	@Test
	void shouldReturnDbName() throws SQLException {
		assertEquals("db-name", getMetaData().getDbName());
	}

	@Test
	void shouldReturnColumnType() throws SQLException {
		assertEquals(Types.VARCHAR, getMetaData().getColumnType(1));
	}

	@Test
	void shouldReturnColumnTypeName() throws SQLException {
		assertEquals("text", getMetaData().getColumnTypeName(1));
	}

	@Test
	void shouldReturnColumnClassName() throws SQLException {
		assertEquals("java.math.BigDecimal", getMetaData().getColumnClassName(3));
	}

	@Test
	void shouldReturnTrueWhenColumnIsCaseSensitiveAndFalseOtherwise() throws SQLException {
		FireboltResultSetMetaData fireboltResultSetMetaData = getMetaData();
		assertTrue(fireboltResultSetMetaData.isCaseSensitive(1));
		assertFalse(fireboltResultSetMetaData.isCaseSensitive(2));
	}

	@Test
	void isReadOnly() throws SQLException {
		assertTrue(getMetaData().isReadOnly(1));
	}

	@Test
	void wrap() throws SQLException {
		FireboltResultSetMetaData fireboltResultSetMetaData = getMetaData();
		assertTrue(fireboltResultSetMetaData.isWrapperFor(ResultSetMetaData.class));
		assertSame(fireboltResultSetMetaData, fireboltResultSetMetaData.unwrap(ResultSetMetaData.class));

		assertFalse(fireboltResultSetMetaData.isWrapperFor(ResultSet.class));
		assertThrows(SQLException.class, () -> fireboltResultSetMetaData.unwrap(ResultSet.class));
	}

	private List<Column> getColumns() {
		return Arrays.asList(Column.of("Nullable(String)", "name"), Column.of("Integer", "age"),
				Column.of("Decimal(1,2)", "Weight"));
	}

	private  FireboltResultSetMetaData getMetaData() {
		return new FireboltResultSetMetaData(getColumns(), "table-name", "db-name");
	}
}
