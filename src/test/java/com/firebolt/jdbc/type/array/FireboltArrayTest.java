package com.firebolt.jdbc.type.array;

import com.firebolt.jdbc.type.FireboltDataType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class FireboltArrayTest {

	private static final String[] ARRAY = new String[] { "Hello" };
	private final FireboltArray fireboltArray = new FireboltArray(FireboltDataType.TEXT, ARRAY);

	@Test
	void shouldReturnBaseTypeName() {
		assertEquals("text", fireboltArray.getBaseTypeName());
	}

	@Test
	void shouldReturnBaseType() {
		assertEquals(FireboltDataType.TEXT.getSqlType(), fireboltArray.getBaseType());
	}

	@Test
	void shouldThrowExceptionIfGettingArrayWhenItIsFree() {
		fireboltArray.free();
		assertThrows(SQLException.class, fireboltArray::getArray);
		assertThrows(SQLException.class, () -> fireboltArray.getArray(null));
		assertThrows(SQLException.class, fireboltArray::getResultSet);
		assertThrows(SQLException.class, () -> fireboltArray.getResultSet(null));
	}

	@Test
	void shouldReturnArrayWhenNoMapIsGiven() throws SQLException {
		assertEquals(ARRAY, fireboltArray.getArray(null));
	}

	@ParameterizedTest
	@ValueSource(ints = {1, 2})
	void shouldReturnArray1SliceStart1(int length) throws SQLException {
		assertEquals(ARRAY, fireboltArray.getArray(1, length));
	}

	@ParameterizedTest
	@ValueSource(ints = {0, -1})
	void shouldThrowExceptionIfIndexIsWrong(int index) {
		assertThrows(SQLException.class, () -> fireboltArray.getArray(index, 1));
	}

	@ParameterizedTest
	@CsvSource(
			delimiter = ',',
			value = {
					"'',1,1,''", "'',1,2,''", "'',1,100,''",
					"'one',1,1,'one'", "'one',1,2,'one'",
					"'one,two',1,1,'one'", "'one,two',1,2,'one,two'", "'one,two',1,3,'one,two'",
					"'one,two',2,1,'two'", "'one,two',2,2,'two'", "'one,two',2,0,''",
					"'one,two',1,2,'one,two'", "'one,two',1,3,'one,two'",
					"'one,two,three',2,2,'two,three'", "'one,two,three',2,5,'two,three'",
					"'one,two,three,four,five',3,1,'three'", "'one,two,three,four,five',3,3,'three,four,five'", "'one,two,three,four,five',3,4,'three,four,five'"
			}
	)
	void shouldReturnSlice(String in, int index, int length, String exp) throws SQLException {
		String[] inArray = "".equals(in) ? new String[0] : in.split("\\s*,\\s*");
		String[] expArray = "".equals(exp) ? new String[0] : exp.split("\\s*,\\s*");
		FireboltArray fireboltArray = new FireboltArray(FireboltDataType.TEXT, inArray);
		assertArrayEquals(expArray, (String[])fireboltArray.getArray(index, length));

		ResultSet rs = fireboltArray.getResultSet(index, length);
		ResultSetMetaData md = rs.getMetaData();
		assertEquals(2, md.getColumnCount());
		assertEquals("INDEX", md.getColumnName(1));
		assertEquals("VALUE", md.getColumnName(2));
		for (int i = 0; rs.next(); i++) {
			assertEquals(i + 1, rs.getInt(1));
			assertEquals(expArray[i], rs.getObject(2));
		}
	}

	@Test
	void shouldThrowExceptionIfAMapIsGiven() {
		Map<String, Class<?>> map = Map.of("test", String.class);
		assertThrows(SQLException.class, () -> fireboltArray.getArray(map));
		assertThrows(SQLException.class, () -> fireboltArray.getArray(1, 100, map));
	}
}
