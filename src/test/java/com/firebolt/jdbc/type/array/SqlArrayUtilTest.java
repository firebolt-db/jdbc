package com.firebolt.jdbc.type.array;

import static com.firebolt.jdbc.type.FireboltDataType.BIG_INT;
import static com.firebolt.jdbc.type.FireboltDataType.INTEGER;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.sql.Array;
import java.sql.SQLException;
import java.util.stream.Stream;

import lombok.Value;
import org.junit.jupiter.api.Test;

import com.firebolt.jdbc.resultset.column.ColumnType;
import com.firebolt.jdbc.type.FireboltDataType;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

class SqlArrayUtilTest {

	@Test
	void shouldTransformToEmptyArray() throws SQLException {
		String value = "[]";
		FireboltArray emptyArray = new FireboltArray(INTEGER, new Integer[] {});
		Array result = SqlArrayUtil.transformToSqlArray(value, ColumnType.of("Array(INT32)"));

		assertEquals(emptyArray.getBaseType(), result.getBaseType());
		assertArrayEquals((Integer[]) emptyArray.getArray(), (Integer[]) result.getArray());
	}

	@ParameterizedTest
	@ValueSource(strings = {"Array(INT32)", "Array(int)", "Array(int null)", "Array(int) null", "Array(int null) null"})
	void shouldTransformIntArray(String type) throws SQLException {
		String value = "[1,2,3,\\N,5]";
		FireboltArray expectedArray = new FireboltArray(INTEGER, new Integer[] { 1, 2, 3, null, 5 });
		Array result = SqlArrayUtil.transformToSqlArray(value, ColumnType.of(type));

		assertEquals(expectedArray.getBaseType(), result.getBaseType());
		assertArrayEquals((Integer[]) expectedArray.getArray(), (Integer[]) result.getArray());
	}

	@Test
	void shouldTransformStringArray() throws SQLException {
		String value = "['1','2','3','',\\N,'5']";
		FireboltArray expectedArray = new FireboltArray(FireboltDataType.TEXT, new String[] { "1", "2", "3", "", null, "5" });
		Array result = SqlArrayUtil.transformToSqlArray(value, ColumnType.of("Array(TEXT)"));

		assertEquals(expectedArray.getBaseType(), result.getBaseType());
		assertArrayEquals((String[]) expectedArray.getArray(), (String[]) result.getArray());
	}

	@Test
	void shouldTransformStringArrayWithComma() throws SQLException {
		String value = "['1','2,','3','',\\N,'5']";
		FireboltArray expectedArray = new FireboltArray(FireboltDataType.TEXT, new String[] { "1", "2,", "3", "", null, "5" });
		Array result = SqlArrayUtil.transformToSqlArray(value, ColumnType.of("Array(TEXT)"));

		assertEquals(expectedArray.getBaseType(), result.getBaseType());
		assertArrayEquals((String[]) expectedArray.getArray(), (String[]) result.getArray());
	}

	@Test
	void shouldTransformArrayOfTuples() throws SQLException {
		String value = "[(1,'a'),(2,'b'),(3,'c')]";
		Object[][] expectedArray = new Object[][] { { 1, "a" }, { 2, "b" }, { 3, "c" } };
		FireboltArray expectedFireboltArray = new FireboltArray(FireboltDataType.TUPLE, expectedArray);
		Array result = SqlArrayUtil.transformToSqlArray(value, ColumnType.of("Array(TUPLE(int,string))"));

		assertEquals(expectedFireboltArray.getBaseType(), result.getBaseType());
		assertArrayEquals(expectedArray, (Object[]) result.getArray());
	}

	@Test
	void shouldTransformArrayOfArrayTuples() throws SQLException {
		String value = "[[(1,'(a))'),(2,'[b]'),(3,'[]c[')],[(4,'d')]]";
		Object[][][] expectedArray = new Object[][][] {
				{ { 1, "(a))" }, { 2, "[b]" }, { 3, "[]c[" } },
				{ { 4, "d" } }
		};
		FireboltArray expectedFireboltArray = new FireboltArray(FireboltDataType.TUPLE, expectedArray);
		Array result = SqlArrayUtil.transformToSqlArray(value, ColumnType.of("Array(Array(TUPLE(int,string)))"));

		assertEquals(expectedFireboltArray.getBaseType(), result.getBaseType());
		assertArrayEquals(expectedArray, (Object[][]) result.getArray());
	}

	@Test
	void shouldTransformArrayOfTuplesWithSpecialCharacters() throws SQLException {
		String value = "[(1,'a','1a'),(2,'b','2b'),(3,'[c]','3c')]";
		Object[][] expectedArray = new Object[][] { { 1, "a", "1a" }, { 2, "b", "2b" }, { 3, "[c]", "3c" } };
		FireboltArray expectedFireboltArray = new FireboltArray(FireboltDataType.TUPLE, expectedArray);
		Array result = SqlArrayUtil.transformToSqlArray(value, ColumnType.of("Array(TUPLE(int,string,string))"));

		assertEquals(expectedFireboltArray.getBaseType(), result.getBaseType());
		assertArrayEquals(expectedArray, (Object[]) result.getArray());
	}

	@ParameterizedTest
	@ValueSource(strings = {"Array(Array(int))", "Array(Array(int null))", "Array(Array(int null) null)", "Array(Array(int null) null) null"})
	void shouldTransformBiDimensionalEmptyIntArray(String type) throws SQLException {
		shouldTransformArray(type, "[[]]", INTEGER, new Integer[][]{new Integer[]{}});
	}

	@ParameterizedTest
	@ValueSource(strings = {"Array(Array(int))", "Array(Array(int null))", "Array(Array(int null) null)", "Array(Array(int null) null) null"})
	void shouldTransformBiDimensionalIntArrayOneElement(String type) throws SQLException {
		shouldTransformArray(type, "[[1,2,3]]", INTEGER, new Integer[][]{{1, 2, 3}});
	}

	@ParameterizedTest
	@ValueSource(strings = {"Array(double) null", "Array(Array(int null) null) null"})
	void shouldTransformNullArrayToNull(String type) throws SQLException {
		assertNull(SqlArrayUtil.transformToSqlArray("NULL", ColumnType.of(type)));
	}

	private static Stream<Arguments> biDimensionalIntArray() {
		String nullableTwoDimArray = "Array(Array(int null) null) null";
		String threeDimLongArray = "Array(Array(Array(long)))";
		return Stream.of(
				// 2 dim
				Arguments.of(nullableTwoDimArray, "[[1, 2], [3]]", INTEGER, new Integer[][] {{1,2}, {3}}),
				Arguments.of(nullableTwoDimArray, "[[4, NULL, 5]]", INTEGER, new Integer[][] {{4, null, 5}}),
				Arguments.of(nullableTwoDimArray, "[NULL, [4], NULL]", INTEGER, new Integer[][] {null, {4}, null}),
				Arguments.of(nullableTwoDimArray, "[[4], NULL, [5, NULL, 6]]", INTEGER, new Integer[][] {{4}, null, {5,null,6}}),
				Arguments.of(nullableTwoDimArray, "[[NULL,7]]", INTEGER, new Integer[][] {{null,7}}),
				// 3 dim
				Arguments.of(threeDimLongArray, "[[[1, 2, 3]]]", BIG_INT, new Long[][][] {{{1L, 2L, 3L}}}),
				Arguments.of(threeDimLongArray, "[[[10]], [[1], [2, 3], [4, 5]], [[20]]]", BIG_INT, new Long[][][] {{{10L}}, {{1L}, {2L, 3L}, {4L, 5L}}, {{20L}}}),
				Arguments.of(threeDimLongArray, "[NULL, [NULL, [1, 2, 3], NULL], NULL]", BIG_INT, new Long[][][] {null, {null, {1L, 2L, 3L}, null}, null}),
				Arguments.of(threeDimLongArray, "[[[]]]", BIG_INT, new Long[][][] {{{}}})
		);
	}
	@ParameterizedTest(name = "{0}")
	@MethodSource("biDimensionalIntArray")
	void shouldTransformBiDimensionalIntArraySeveralElements(String type, String input, FireboltDataType expectedType, Object[] expected) throws SQLException {
		shouldTransformArray(type, input.replace(" ", ""), expectedType, expected);
	}


	void shouldTransformArray(String typeDef, String value, FireboltDataType expectedType, Object expectedValue) throws SQLException {
		FireboltArray expectedArray = new FireboltArray(expectedType, expectedValue);
		Array result = SqlArrayUtil.transformToSqlArray(value, ColumnType.of(typeDef));

		assertEquals(expectedArray.getBaseType(), result.getBaseType());
		assertArrayEquals((Object[])expectedArray.getArray(), (Object[])result.getArray());
	}

}
