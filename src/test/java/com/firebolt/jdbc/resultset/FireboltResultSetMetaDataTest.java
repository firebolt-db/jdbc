package com.firebolt.jdbc.resultset;

import com.firebolt.jdbc.CheckedFunction;
import com.firebolt.jdbc.resultset.column.Column;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.List;

import static java.lang.String.format;
import static java.sql.ResultSetMetaData.columnNoNulls;
import static java.sql.ResultSetMetaData.columnNullable;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
		ResultSetMetaData fireboltResultSetMetaData = getMetaData();
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
		assertEquals("table-name", getMetaData().getTableName(1));
	}

	@Test
	void shouldReturnDbName() throws SQLException {
		assertEquals("db-name", getMetaData().getCatalogName(1));
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
		ResultSetMetaData fireboltResultSetMetaData = getMetaData();
		assertTrue(fireboltResultSetMetaData.isCaseSensitive(1));
		assertFalse(fireboltResultSetMetaData.isCaseSensitive(2));
	}

	@Test
	void trivialMarkers() throws SQLException {
		ResultSetMetaData md = getMetaData();
		assertTrue(md.isReadOnly(1));
		assertFalse(md.isAutoIncrement(1));
		assertTrue(md.isSearchable(1));
		assertFalse(md.isCurrency(1));
		assertEquals(80, md.getColumnDisplaySize(1));
		assertEquals("", md.getSchemaName(1));
		assertFalse(md.isWritable(1));
		assertFalse(md.isDefinitelyWritable(1));
	}

	@ParameterizedTest
	@ValueSource(ints = {-1, 0, 4})
	void wrongIndex(int column) {
		ResultSetMetaData md = getMetaData();
		assertFunction(md::isNullable, column);
		assertFunction(md::isSigned, column);
		assertFunction(md::getColumnLabel, column);
		assertFunction(md::getColumnName, column);
		assertFunction(md::getPrecision, column);
		assertFunction(md::getScale, column);
		assertFunction(md::getTableName, column);
		assertFunction(md::getCatalogName, column);
		assertFunction(md::getColumnType, column);
		assertFunction(md::getColumnTypeName, column);
		assertFunction(md::getColumnClassName, column);
		assertFunction(md::isCaseSensitive, column);
		assertFunction(md::isAutoIncrement, column);
		assertFunction(md::isSearchable, column);
		assertFunction(md::isCurrency, column);
		assertFunction(md::getColumnDisplaySize, column);
		assertFunction(md::getSchemaName, column);
		assertFunction(md::isReadOnly, column);
		assertFunction(md::isWritable, column);
		assertFunction(md::isDefinitelyWritable, column);
	}

	private <R> void assertFunction(CheckedFunction<Integer, R> function, int column) {
		assertEquals(format("Invalid column number %d", column), assertThrows(SQLException.class, () -> function.apply(column)).getMessage());
	}

	@Test
	void wrap() throws SQLException {
		ResultSetMetaData fireboltResultSetMetaData = getMetaData();
		assertTrue(fireboltResultSetMetaData.isWrapperFor(ResultSetMetaData.class));
		assertSame(fireboltResultSetMetaData, fireboltResultSetMetaData.unwrap(ResultSetMetaData.class));

		assertFalse(fireboltResultSetMetaData.isWrapperFor(ResultSet.class));
		assertThrows(SQLException.class, () -> fireboltResultSetMetaData.unwrap(ResultSet.class));
	}

	private List<Column> getColumns() {
		return Arrays.asList(Column.of("Nullable(String)", "name"), Column.of("Integer", "age"),
				Column.of("Decimal(1,2)", "Weight"));
	}

	private  ResultSetMetaData getMetaData() {
		return new FireboltResultSetMetaData("db-name", "table-name", getColumns());
	}
}
