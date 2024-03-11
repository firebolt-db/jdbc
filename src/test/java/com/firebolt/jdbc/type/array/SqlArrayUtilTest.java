package com.firebolt.jdbc.type.array;

import com.firebolt.jdbc.resultset.column.ColumnType;
import com.firebolt.jdbc.type.FireboltDataType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.sql.Array;
import java.sql.JDBCType;
import java.sql.SQLException;
import java.util.stream.Stream;

import static com.firebolt.jdbc.type.FireboltDataType.BIG_INT;
import static com.firebolt.jdbc.type.FireboltDataType.INTEGER;
import static com.firebolt.jdbc.type.FireboltDataType.TEXT;
import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class SqlArrayUtilTest {
	private static final String nullableTwoDimIntArray = "Array(Array(int null) null) null";
	private static final String threeDimLongArray = "Array(Array(Array(long)))";
	private static final String textArray = "Array(TEXT)";
	private static final String nullableTwoDimTextArray = "Array(Array(TEXT null) null) null";
	private static final String FB1 = "Firebolt.1";
	private static final String PG = "PostreSQL compliant";

	@Test
	void shouldTransformToEmptyArray() throws SQLException {
		String value = "[]";
		FireboltArray emptyArray = new FireboltArray(INTEGER, new Integer[] {});
		Array result = SqlArrayUtil.transformToSqlArray(value, ColumnType.of("Array(INT32)"));

		assertEquals(emptyArray.getBaseType(), result.getBaseType());
		assertArrayEquals((Integer[]) emptyArray.getArray(), (Integer[]) result.getArray());
	}

	@ParameterizedTest
	@CsvSource(value = {
			// old format
			"Array(INT32);[1,2,3,\\N,5]",
			"Array(int);[1,2,3,\\N,5]",
			"Array(int null);[1,2,3,\\N,5]",
			"Array(int) null;[1,2,3,\\N,5]",
			"Array(int null) null;[1,2,3,\\N,5]",
			// new/v2/postgres compliant
			"Array(INT32);{1,2,3,\\N,5}",
			"Array(int);{1,2,3,\\N,5}",
			"Array(int null);{1,2,3,\\N,5}",
			"Array(int) null;{1,2,3,\\N,5}",
			"Array(int null) null;{1,2,3,\\N,5}"
	},
			delimiter = ';')
	void shouldTransformIntArray(String type, String value) throws SQLException {
		FireboltArray expectedArray = new FireboltArray(INTEGER, new Integer[] { 1, 2, 3, null, 5 });
		Array result = SqlArrayUtil.transformToSqlArray(value, ColumnType.of(type));

		assertJdbcType(expectedArray.getBaseType(), result.getBaseType());
		assertArrayEquals((Integer[]) expectedArray.getArray(), (Integer[]) result.getArray());
	}

	@ParameterizedTest
	@ValueSource(strings = {
			"['1','2','3','',\\N,'5']",
			"['1','2','3','',NULL,'5']",
			"{1,2,3,\"\",\\N,5}",
			"{1,2,3,\"\",NULL,5}",
	})
	void shouldTransformStringArray(String value) throws SQLException {
		FireboltArray expectedArray = new FireboltArray(FireboltDataType.TEXT, new String[] { "1", "2", "3", "", null, "5" });
		Array result = SqlArrayUtil.transformToSqlArray(value, ColumnType.of("Array(TEXT)"));

		assertJdbcType(expectedArray.getBaseType(), result.getBaseType());
		assertArrayEquals((String[]) expectedArray.getArray(), (String[]) result.getArray());
	}

	@ParameterizedTest
	@ValueSource(strings = {
			"[' a','b ',' c ',' ','a b',' c d ']",
			"{\" a\",\"b \",\" c \",\" \",\"a b\",\" c d \"}"
	})
	void shouldTransformStringArrayWithSpaces(String value) throws SQLException {
		FireboltArray expectedArray = new FireboltArray(FireboltDataType.TEXT, new String[] { " a", "b ", " c ", " ", "a b", " c d " });
		Array result = SqlArrayUtil.transformToSqlArray(value, ColumnType.of("Array(TEXT)"));

		assertJdbcType(expectedArray.getBaseType(), result.getBaseType());
		assertArrayEquals((String[]) expectedArray.getArray(), (String[]) result.getArray());
	}

	@ParameterizedTest
	@ValueSource(strings = {
			"['1','2,','3','',\\N,'5']",
			"{1,\"2,\",3,\"\",\\N,5}"
	})
	void shouldTransformStringArrayWithComma(String value) throws SQLException {
		FireboltArray expectedArray = new FireboltArray(FireboltDataType.TEXT, new String[] { "1", "2,", "3", "", null, "5" });
		Array result = SqlArrayUtil.transformToSqlArray(value, ColumnType.of("Array(TEXT)"));

		assertJdbcType(expectedArray.getBaseType(), result.getBaseType());
		assertArrayEquals((String[]) expectedArray.getArray(), (String[]) result.getArray());
	}

	@ParameterizedTest
	@ValueSource(strings = {
			"[(1,'a'),(2,'b'),(3,'c')]",
			"{\"('1','a')\",\"('2','b')\",\"('3','c')\"}"
	})
	void shouldTransformArrayOfTuples(String value) throws SQLException {
		Object[][] expectedArray = new Object[][] { { 1, "a" }, { 2, "b" }, { 3, "c" } };
		FireboltArray expectedFireboltArray = new FireboltArray(FireboltDataType.TUPLE, expectedArray);
		Array result = SqlArrayUtil.transformToSqlArray(value, ColumnType.of("Array(TUPLE(int,string))"));

		assertJdbcType(expectedFireboltArray.getBaseType(), result.getBaseType());
		assertArrayEquals(expectedArray, (Object[]) result.getArray());
	}

	@ParameterizedTest
	@ValueSource(strings = {
			"[[(1,'(a))'),(2,'[b]'),(3,'[]c[')],[(4,'d')]]",
			"{{\"(1,'(a))')\",\"(2,'[b]')\",\"(3,'[]c[')\"},{\"(4,'d')\"}}"
	})
	void shouldTransformArrayOfArrayTuples(String value) throws SQLException {
		Object[][][] expectedArray = new Object[][][] {
				{ { 1, "(a))" }, { 2, "[b]" }, { 3, "[]c[" } },
				{ { 4, "d" } }
		};
		FireboltArray expectedFireboltArray = new FireboltArray(FireboltDataType.TUPLE, expectedArray);
		Array result = SqlArrayUtil.transformToSqlArray(value, ColumnType.of("Array(Array(TUPLE(int,string)))"));

		assertJdbcType(expectedFireboltArray.getBaseType(), result.getBaseType());
		assertArrayEquals(expectedArray, (Object[][]) result.getArray());
	}

	@ParameterizedTest
	@ValueSource(strings = {
			"[(1,'a','1a'),(2,'b','2b'),(3,'[c]','3c')]",
			"{\"(1,'a','1a')\",\"(2,'b','2b')\",\"(3,'[c]','3c')\"}"
	})
	void shouldTransformArrayOfTuplesWithSpecialCharacters(String value) throws SQLException {
		Object[][] expectedArray = new Object[][] { { 1, "a", "1a" }, { 2, "b", "2b" }, { 3, "[c]", "3c" } };
		FireboltArray expectedFireboltArray = new FireboltArray(FireboltDataType.TUPLE, expectedArray);
		Array result = SqlArrayUtil.transformToSqlArray(value, ColumnType.of("Array(TUPLE(int,string,string))"));

		assertJdbcType(expectedFireboltArray.getBaseType(), result.getBaseType());
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

	@ParameterizedTest
	@ValueSource(booleans = {true, false})
	void nullByteArrayToString(boolean separateEachByte) {
		assertNull(SqlArrayUtil.byteArrayToHexString(null, separateEachByte));
	}

	@ParameterizedTest
	@CsvSource({
			"ABC,false,\\x414243",
			"abc,true,\\x61\\x62\\x63"
	})
	void byteArrayToString(String str, boolean separateEachByte, String expectedHex) {
		assertEquals(expectedHex, SqlArrayUtil.byteArrayToHexString(str.getBytes(), separateEachByte));
	}

	@Test
	void nullHexStringToByteArray() {
		assertNull(SqlArrayUtil.hexStringToByteArray(null));
	}

	@ParameterizedTest
	@CsvSource({
			"\\x78797A,xyz",
			"\\x4a4B4c,JKL",
			"hello,hello" // not hex string
	})
	void hexStringToByteArray(String hex, String expected) {
		assertArrayEquals(expected.getBytes(), SqlArrayUtil.hexStringToByteArray(hex));
	}

	@Test
	void notHexStringToByteArray() {
		assertArrayEquals("nothing".getBytes(), SqlArrayUtil.hexStringToByteArray("nothing"));
	}

	private static Stream<Arguments> biDimensionalIntArray() {
		return Stream.of(
				// 2 dim integer
				Arguments.of(FB1, nullableTwoDimIntArray, "[[1,2],[3]]", INTEGER, new Integer[][] {{1,2}, {3}}),
				Arguments.of(FB1, nullableTwoDimIntArray, "[[4,NULL,5]]", INTEGER, new Integer[][] {{4, null, 5}}),
				Arguments.of(FB1, nullableTwoDimIntArray, "[NULL,[4],NULL]", INTEGER, new Integer[][] {null, {4}, null}),
				Arguments.of(FB1, nullableTwoDimIntArray, "[[4],NULL,[5,NULL,6]]", INTEGER, new Integer[][] {{4}, null, {5,null,6}}),
				Arguments.of(FB1, nullableTwoDimIntArray, "[[NULL,7]]", INTEGER, new Integer[][] {{null,7}}),
				// 3 dim integer
				Arguments.of(FB1, threeDimLongArray, "[[[1,2,3]]]", BIG_INT, new Long[][][] {{{1L, 2L, 3L}}}),
				Arguments.of(FB1, threeDimLongArray, "[[[10]],[[1],[2,3],[4,5]],[[20]]]", BIG_INT, new Long[][][] {{{10L}}, {{1L}, {2L, 3L}, {4L, 5L}}, {{20L}}}),
				Arguments.of(FB1, threeDimLongArray, "[NULL,[NULL,[1,2,3],NULL],NULL]", BIG_INT, new Long[][][] {null, {null, {1L, 2L, 3L}, null}, null}),
				Arguments.of(FB1, threeDimLongArray, "[[[]]]", BIG_INT, new Long[][][] {{{}}}),
				// text array
				Arguments.of(FB1, textArray, "['Hello','Bye']", TEXT, new String[] {"Hello", "Bye"}),
				Arguments.of(FB1, textArray, "['What\\'s up','ok','a[1]']", TEXT, new String[] {"What's up", "ok", "a[1]"}),
				Arguments.of(FB1, nullableTwoDimTextArray, "[['one','two'],['three']]", TEXT, new String[][] {{"one", "two"}, {"three"}})
		);
	}

	private static Stream<Arguments> biDimensionalIntArrayPostgresCompliant() {
		return Stream.of(
				// 2 dim integer
				Arguments.of(PG, nullableTwoDimIntArray, "{{1,2},{3}}", INTEGER, new Integer[][] {{1,2}, {3}}),
				Arguments.of(PG, nullableTwoDimIntArray, "{{4,NULL,5}}", INTEGER, new Integer[][] {{4, null, 5}}),
				Arguments.of(PG, nullableTwoDimIntArray, "{NULL,{4},NULL}", INTEGER, new Integer[][] {null, {4}, null}),
				Arguments.of(PG, nullableTwoDimIntArray, "{{4},NULL,{5,NULL,6}}", INTEGER, new Integer[][] {{4}, null, {5,null,6}}),
				Arguments.of(PG, nullableTwoDimIntArray, "{{NULL,7}}", INTEGER, new Integer[][] {{null,7}}),
				// 3 dim integer
				Arguments.of(PG, threeDimLongArray, "{{{1,2,3}}}", BIG_INT, new Long[][][] {{{1L, 2L, 3L}}}),
				Arguments.of(PG, threeDimLongArray, "{{{10}},{{1},{2,3},{4,5}},{{20}}}", BIG_INT, new Long[][][] {{{10L}}, {{1L}, {2L, 3L}, {4L, 5L}}, {{20L}}}),
				Arguments.of(PG, threeDimLongArray, "{NULL,{NULL,{1,2,3},NULL},NULL}", BIG_INT, new Long[][][] {null, {null, {1L, 2L, 3L}, null}, null}),
				Arguments.of(PG, threeDimLongArray, "{{{}}}", BIG_INT, new Long[][][] {{{}}}),
				// text array
				Arguments.of(PG, textArray, "{Hello,Bye}", TEXT, new String[] {"Hello", "Bye"}),
				Arguments.of(PG, textArray, "{\"What's up\",ok,a[1]}", TEXT, new String[] {"What's up", "ok", "a[1]"}),
				Arguments.of(PG, nullableTwoDimTextArray, "{{one,two},{three}}", TEXT, new String[][] {{"one", "two"}, {"three"}})
		);
	}

	@ParameterizedTest(name = "{0}:{1}:{2}")
	@MethodSource({"biDimensionalIntArray", "biDimensionalIntArrayPostgresCompliant"})
	void shouldTransformBiDimensionalIntArraySeveralElements(String format, String type, String input, FireboltDataType expectedType, Object[] expected) throws SQLException {
		shouldTransformArray(type, input, expectedType, expected);
	}


	void shouldTransformArray(String typeDef, String value, FireboltDataType expectedType, Object expectedValue) throws SQLException {
		FireboltArray expectedArray = new FireboltArray(expectedType, expectedValue);
		Array result = SqlArrayUtil.transformToSqlArray(value, ColumnType.of(typeDef));

		assertJdbcType(expectedArray.getBaseType(), result.getBaseType());
		assertArrayEquals((Object[])expectedArray.getArray(), (Object[])result.getArray());
	}

	private void assertJdbcType(int expected, int actual) {
		assertEquals(expected, actual, () -> format("Wrong type: expected %s(%d) but was %s(%d)", JDBCType.valueOf(expected), expected, JDBCType.valueOf(actual), actual));
	}

}
