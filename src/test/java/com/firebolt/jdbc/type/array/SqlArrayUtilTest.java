package com.firebolt.jdbc.type.array;

import com.firebolt.jdbc.resultset.column.ColumnType;
import com.firebolt.jdbc.type.FireboltDataType;
//import org.junit.jupiter.api.Test;
//import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.sql.Array;
import java.sql.JDBCType;
import java.sql.SQLException;
import java.util.stream.Stream;

import static com.firebolt.jdbc.type.FireboltDataType.BIG_INT;
import static com.firebolt.jdbc.type.FireboltDataType.INTEGER;
import static com.firebolt.jdbc.type.FireboltDataType.TEXT;
import static com.firebolt.jdbc.type.FireboltDataType.TUPLE;
import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SqlArrayUtilTest {
	private static final String nullableTwoDimIntArray = "Array(Array(int null) null) null";
	private static final String threeDimLongArray = "Array(Array(Array(long)))";
	private static final String textArray = "Array(TEXT)";
	private static final String nullableTwoDimTextArray = "Array(Array(TEXT null) null) null";
	private static final String FB1 = "Firebolt.1";
	private static final String PG = "PostreSQL compliant";

//	@Test
	void shouldTransformToEmptyArray() throws SQLException {
		shouldTransformArray("Array(INT32)", "[]", INTEGER, new Integer[] {});
	}

//	@ParameterizedTest
	@CsvSource(value = {
			// old format
			"Array(INT32);[1,2,3,null,5]",
			"Array(int);[1,2,3,null,5]",
			"Array(int null);[1,2,3,null,5]",
			"Array(int) null;[1,2,3,null,5]",
			"Array(int null) null;[1,2,3,null,5]",
	},
			delimiter = ';')
	void shouldTransformIntArray(String type, String value) throws SQLException {
		shouldTransformArray(type, value, INTEGER, new Integer[] { 1, 2, 3, null, 5 });
	}

//	@ParameterizedTest
	@ValueSource(strings = {
			"['1','2','3','',null,'5']",
			"['1','2','3','',NULL,'5']",
			"{1,2,3,\"\",null,5}",
			"{1,2,3,\"\",NULL,5}",
	})
	void shouldTransformStringArray(String value) throws SQLException {
		shouldTransformArray("Array(TEXT)", value, TEXT, new String[] { "1", "2", "3", "", null, "5" });
	}

//	@ParameterizedTest
	@ValueSource(strings = {
			"[' a','b ',' c ',' ','a b',' c d ']",
			"{\" a\",\"b \",\" c \",\" \",\"a b\",\" c d \"}"
	})
	void shouldTransformStringArrayWithSpaces(String value) throws SQLException {
		shouldTransformArray("Array(TEXT)", value, TEXT, new String[] { " a", "b ", " c ", " ", "a b", " c d " });
	}

//	@ParameterizedTest
	@ValueSource(strings = {
			"['1','2,','3','',null,'5']",
			"{1,\"2,\",3,\"\",null,5}"
	})
	void shouldTransformStringArrayWithComma(String value) throws SQLException {
		shouldTransformArray("Array(TEXT)", value, TEXT, new String[] { "1", "2,", "3", "", null, "5" });
	}

//	@ParameterizedTest
	@ValueSource(strings = {
			"[(1,'a'),(2,'b'),(3,'c')]",
			"{\"('1','a')\",\"('2','b')\",\"('3','c')\"}"
	})
	void shouldTransformArrayOfTuples(String value) throws SQLException {
		Object[][] expectedArray = new Object[][] { { 1, "a" }, { 2, "b" }, { 3, "c" } };
		shouldTransformArray("Array(TUPLE(int,string))", value, TUPLE, expectedArray);
	}

//	@ParameterizedTest
	@ValueSource(strings = {
			"[[(1,'(a))'),(2,'[b]'),(3,'[]c[')],[(4,'d')]]",
			"{{\"(1,'(a))')\",\"(2,'[b]')\",\"(3,'[]c[')\"},{\"(4,'d')\"}}"
	})
	void shouldTransformArrayOfArrayTuples(String value) throws SQLException {
		Object[][][] expectedArray = new Object[][][] {
				{ { 1, "(a))" }, { 2, "[b]" }, { 3, "[]c[" } },
				{ { 4, "d" } }
		};
		shouldTransformArray("Array(Array(TUPLE(int,string)))", value, TUPLE, expectedArray);
	}

//	@ParameterizedTest
	@ValueSource(strings = {
			"[(1,'a','1a'),(2,'b','2b'),(3,'[c]','3c')]",
			"{\"(1,'a','1a')\",\"(2,'b','2b')\",\"(3,'[c]','3c')\"}"
	})
	void shouldTransformArrayOfTuplesWithSpecialCharacters(String value) throws SQLException {
		Object[][] expectedArray = new Object[][] { { 1, "a", "1a" }, { 2, "b", "2b" }, { 3, "[c]", "3c" } };
		shouldTransformArray("Array(TUPLE(int,string,string))", value, TUPLE, expectedArray);
	}

//	@ParameterizedTest
	@ValueSource(strings = {"Array(Array(int))", "Array(Array(int null))", "Array(Array(int null) null)", "Array(Array(int null) null) null"})
	void shouldTransformBiDimensionalEmptyIntArray(String type) throws SQLException {
		shouldTransformArray(type, "[[]]", INTEGER, new Integer[][]{new Integer[]{}});
	}

//	@ParameterizedTest
	@ValueSource(strings = {"Array(Array(int))", "Array(Array(int null))", "Array(Array(int null) null)", "Array(Array(int null) null) null"})
	void shouldTransformBiDimensionalIntArrayOneElement(String type) throws SQLException {
		shouldTransformArray(type, "[[1,2,3]]", INTEGER, new Integer[][]{{1, 2, 3}});
	}

//	@ParameterizedTest
	@ValueSource(strings = {"Array(double) null", "Array(Array(int null) null) null"})
	void shouldTransformNullArrayToNull(String type) throws SQLException {
		assertNull(SqlArrayUtil.transformToSqlArray("NULL", ColumnType.of(type)));
	}

//	@ParameterizedTest
	@ValueSource(booleans = {true, false})
	void nullByteArrayToString(boolean separateEachByte) {
		assertNull(SqlArrayUtil.byteArrayToHexString(null, separateEachByte));
	}

//	@ParameterizedTest
	@CsvSource(value = {
			"ABC;false;\\x414243",
			"abc;true;\\x61\\x62\\x63",
			"Hello, world!;false;\\x48656c6c6f2c20776f726c6421",
			"Hello, world!;true;\\x48\\x65\\x6c\\x6c\\x6f\\x2c\\x20\\x77\\x6f\\x72\\x6c\\x64\\x21",
			"6/3=2;false;\\x362f333d32",
			"6/3=2;true;\\x36\\x2f\\x33\\x3d\\x32",
			"x\\y;false;\\x785c79",
			"x\\y;true;\\x78\\x5c\\x79"
	}, delimiter = ';')
	void byteArrayToHexString(String str, boolean separateEachByte, String expectedHex) {
		assertEquals(expectedHex, SqlArrayUtil.byteArrayToHexString(str.getBytes(), separateEachByte));
	}

//	@ParameterizedTest
	@CsvSource(value = {
			"false;\\x4d756c74690a6c696e650a74657874",
			"true;\\x4d\\x75\\x6c\\x74\\x69\\x0a\\x6c\\x69\\x6e\\x65\\x0a\\x74\\x65\\x78\\x74"
	}, delimiter = ';')
	void byteArrayWithNewLineToHexString(boolean separateEachByte, String expectedHex) {
		byteArrayToHexString("Multi\nline\ntext", separateEachByte, expectedHex);
	}

//	@Test
	void nullHexStringToByteArray() {
		assertNull(SqlArrayUtil.hexStringToByteArray(null));
	}

//	@ParameterizedTest
	@CsvSource({
			"\\x78797A,xyz",
			"\\x4a4B4c,JKL",
			"\\x20,' '",
			"\\x30,0",
			"\\x2A,*",
			"\\x2F,/",
			"hello,hello" // not hex string
	})
	void hexStringToByteArray(String hex, String expected) {
		assertArrayEquals(expected.getBytes(), SqlArrayUtil.hexStringToByteArray(hex));
	}

//	@Test
	void notHexStringToByteArray() {
		assertArrayEquals("nothing".getBytes(), SqlArrayUtil.hexStringToByteArray("nothing"));
	}

//	@ParameterizedTest
	@ValueSource(strings = {
			"ABC", "abc", "Hello, world!", "Multi\nlinentext"
	})
	@NullSource
	void byteArrayToHexStringAndBack(String str) {
		byte[] bytes = SqlArrayUtil.hexStringToByteArray(SqlArrayUtil.byteArrayToHexString(str == null ? null : str.getBytes(), false));
		assertEquals(str, bytes == null ? null : new String(bytes));
	}

//	@ParameterizedTest
	@CsvSource({"\\x2G,G", "\\xH0,H"})
	void wrongHexStringToByteArray(String hex, String expectedWrongCharacter) {
		@SuppressWarnings("java:S5778") // "Refactor the code of the lambda to have only one invocation possibly throwing a runtime exception" - this is the purpose of this test
		IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> new String(SqlArrayUtil.hexStringToByteArray(hex)));
		assertEquals(format("Illegal character %s in hex string", expectedWrongCharacter), e.getMessage());
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

//	@ParameterizedTest(name = "{0}:{1}:{2}")
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
