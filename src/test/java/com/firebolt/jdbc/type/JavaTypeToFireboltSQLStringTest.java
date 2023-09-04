package com.firebolt.jdbc.type;

import com.firebolt.jdbc.exception.FireboltException;
import com.firebolt.jdbc.resultset.column.ColumnType;
import com.firebolt.jdbc.type.array.FireboltArray;
import com.firebolt.jdbc.type.array.SqlArrayUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junitpioneer.jupiter.DefaultTimeZone;

import java.sql.Date;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.UUID;

import static com.firebolt.jdbc.exception.ExceptionType.TYPE_NOT_SUPPORTED;
import static com.firebolt.jdbc.exception.ExceptionType.TYPE_TRANSFORMATION_ERROR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class JavaTypeToFireboltSQLStringTest {

	@Test
	void shouldTransformAnyNullToString() throws FireboltException {
		assertEquals("NULL", JavaTypeToFireboltSQLString.transformAny(null));
	}

	@ParameterizedTest
	@EnumSource(value = JavaTypeToFireboltSQLString.class)
	void shouldTransformNull(JavaTypeToFireboltSQLString type) throws FireboltException {
		assertEquals("NULL", type.transform(null));
	}

	@Test
	void shouldTransformBooleanToString() throws FireboltException {
		assertEquals("1", JavaTypeToFireboltSQLString.BOOLEAN.transform(true));

		assertEquals("0", JavaTypeToFireboltSQLString.BOOLEAN.transform(false));

		assertEquals("NULL", JavaTypeToFireboltSQLString.BOOLEAN.transform(null));
	}

	@Test
	void shouldTransformUUIDToString() throws FireboltException {
		String uuidValue = "2ac03dc8-f7c9-11ec-b939-0242ac120002";
		UUID uuid = UUID.fromString(uuidValue);
		assertEquals(uuidValue, JavaTypeToFireboltSQLString.UUID.transform(uuid));

		assertEquals(uuidValue, JavaTypeToFireboltSQLString.transformAny(uuid));

		assertEquals("NULL", JavaTypeToFireboltSQLString.UUID.transform(null));
	}

	@Test
	void shouldTransformShortToString() throws FireboltException {
		short s = 123;
		assertEquals("123", JavaTypeToFireboltSQLString.SHORT.transform(s));

		assertEquals("123", JavaTypeToFireboltSQLString.transformAny(s));

		assertEquals("NULL", JavaTypeToFireboltSQLString.BOOLEAN.transform(null));
	}

	@Test
	void shouldEscapeCharactersWhenTransformingFromString() throws FireboltException {
		assertEquals("'105\\' OR 1=1--\\' '", JavaTypeToFireboltSQLString.STRING.transform("105' OR 1=1--' "));

		assertEquals("'105\\' OR 1=1--\\' '", JavaTypeToFireboltSQLString.transformAny("105' OR 1=1--' "));
	}

	@Test
	void shouldTransformLongToString() throws FireboltException {
		assertEquals("105", JavaTypeToFireboltSQLString.LONG.transform(105L));

		assertEquals("105", JavaTypeToFireboltSQLString.transformAny(105L));

		assertEquals("NULL", JavaTypeToFireboltSQLString.LONG.transform(null));
	}

	@Test
	void shouldTransformIntegerToString() throws FireboltException {
		assertEquals("105", JavaTypeToFireboltSQLString.INTEGER.transform(105));

		assertEquals("105", JavaTypeToFireboltSQLString.transformAny(105));

		assertEquals("NULL", JavaTypeToFireboltSQLString.INTEGER.transform(null));
	}

	@Test
	void shouldTransformBigIntegerToString() throws FireboltException {
		assertEquals("1111111111", JavaTypeToFireboltSQLString.BIG_INTEGER.transform(1111111111));

		assertEquals("1111111111", JavaTypeToFireboltSQLString.transformAny(1111111111));

		assertEquals("NULL", JavaTypeToFireboltSQLString.BIG_INTEGER.transform(null));
	}

	@Test
	void shouldTransformFloatToString() throws FireboltException {
		assertEquals("1.5", JavaTypeToFireboltSQLString.FLOAT.transform(1.50f));

		assertEquals("1.5", JavaTypeToFireboltSQLString.transformAny(1.50f));

		assertEquals("NULL", JavaTypeToFireboltSQLString.FLOAT.transform(null));
	}

	@Test
	void shouldTransformDoubleToString() throws FireboltException {
		assertEquals("105", JavaTypeToFireboltSQLString.DOUBLE.transform(105));

		assertEquals("105", JavaTypeToFireboltSQLString.transformAny(105));

		assertEquals("NULL", JavaTypeToFireboltSQLString.DOUBLE.transform(null));
	}

	@Test
	@DefaultTimeZone("Europe/London")
	void shouldTransformDateToString() throws FireboltException {
		Date d = Date.valueOf(LocalDate.of(2022, 5, 23));
		String expectedDateString = "'2022-05-23'";
		assertEquals(expectedDateString, JavaTypeToFireboltSQLString.DATE.transform(d));
		assertEquals(expectedDateString, JavaTypeToFireboltSQLString.transformAny((d)));
	}

	@Test
	void shouldTransformTimeToString() throws FireboltException {
		assertEquals("105", JavaTypeToFireboltSQLString.DOUBLE.transform(105));

		assertEquals("105", JavaTypeToFireboltSQLString.transformAny(105));

		assertEquals("NULL", JavaTypeToFireboltSQLString.DOUBLE.transform(null));
	}

	@Test
	void shouldTransformTimeStampToString() throws FireboltException {
		Timestamp ts = Timestamp.valueOf(LocalDateTime.of(2022, 5, 23, 12, 57, 13, 173456789));
		assertEquals("'2022-05-23 12:57:13.173456789'", JavaTypeToFireboltSQLString.TIMESTAMP.transform(ts));
		assertEquals("'2022-05-23 12:57:13.173456789'", JavaTypeToFireboltSQLString.transformAny(ts));
		assertEquals("NULL", JavaTypeToFireboltSQLString.TIMESTAMP.transform(null));
	}

	@Test
	void shouldTransformSqlArray() throws SQLException {
		String value = "[1,2,3,NULL,5]";
		ColumnType columnType = ColumnType.of("Array(INT32)");
		FireboltArray fireboltArray = SqlArrayUtil.transformToSqlArray(value, columnType);
		assertEquals(value, JavaTypeToFireboltSQLString.ARRAY.transform(fireboltArray));
	}

	@Test
	void shouldTransformArrayOfArray() throws SQLException {
		String value = "[['a','b'],['c']]";
		ColumnType columnType = ColumnType.of("Array(Array(string))");
		FireboltArray fireboltArray = SqlArrayUtil.transformToSqlArray(value, columnType);
		assertEquals(value, JavaTypeToFireboltSQLString.ARRAY.transform(fireboltArray));
	}

	@Test
	void shouldTransformJavaArrayOfArray() throws FireboltException {
		String[][] arr = new String[][] { { "a", "b" }, { "c" } };
		assertEquals("[['a','b'],['c']]", JavaTypeToFireboltSQLString.ARRAY.transform(arr));
	}

	@Test
	void shouldTransformJavaArrayOfPrimitives() throws FireboltException {
		assertEquals("[5]", JavaTypeToFireboltSQLString.ARRAY.transform(new int[] {5}));
	}

	@Test
	void shouldTransformEmptyArray() throws FireboltException {
		assertEquals("[]", JavaTypeToFireboltSQLString.ARRAY.transform(new int[0]));
	}

	@Test
	void shouldThrowExceptionWhenObjectTypeIsNotSupported() {
		FireboltException ex = assertThrows(FireboltException.class, () -> JavaTypeToFireboltSQLString.transformAny(Map.of()));
		assertEquals(TYPE_NOT_SUPPORTED, ex.getType());
	}

	@Test
	void shouldThrowExceptionWhenObjectCouldNotBeTransformed() {
		FireboltException ex = assertThrows(FireboltException.class, () -> JavaTypeToFireboltSQLString.ARRAY.transform(Map.of()));
		assertEquals(TYPE_TRANSFORMATION_ERROR, ex.getType());
	}
}
