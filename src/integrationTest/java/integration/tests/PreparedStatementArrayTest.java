package integration.tests;

import com.firebolt.jdbc.type.FireboltDataType;
import com.firebolt.jdbc.type.array.FireboltArray;
import integration.IntegrationTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.sql.Array;
import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

class PreparedStatementArrayTest extends IntegrationTest {
	enum PreparedStatementValueSetter {
		ARRAY {
			Object create(FireboltDataType type, Object data) {
				return data == null ? null : new FireboltArray(type, data);
			}

			@Override
			void set(PreparedStatement ps, int index, Object value) throws SQLException {
				ps.setArray(index, (Array)value);
			}
		},

		OBJECT {
			Object create(FireboltDataType type, Object data) {
				return data;
			}

			@Override
			void set(PreparedStatement ps, int index, Object value) throws SQLException {
				ps.setObject(index, value);
			}
		},
		;

		abstract Object create(FireboltDataType type, Object data);

		abstract void set(PreparedStatement ps, int index, Object value) throws SQLException;
	}
	private static final String[] STRING_ARRAY = new String[] {"hello", "bye"};
	private static final Integer[] INTEGER_ARRAY = new Integer[] {1, 2, 3};
	private static final int[] INT_ARRAY = new int[] {1, 2, 3};

	@BeforeEach
	void beforeEach() {
		executeStatementFromFile("/statements/prepared-statement-array/ddl.sql");
	}

	@AfterEach
	void afterEach() {
		executeStatementFromFile("/statements/prepared-statement-array/cleanup.sql");
	}


	@ParameterizedTest(name = "set {0}")
	@EnumSource(value = PreparedStatementValueSetter.class)
	void integerAndStringValues(PreparedStatementValueSetter setter) throws SQLException {
		arrays(setter, INT_ARRAY, STRING_ARRAY, INTEGER_ARRAY, STRING_ARRAY);
	}

	@ParameterizedTest(name = "set {0}")
	@EnumSource(value = PreparedStatementValueSetter.class)
	void intAndStringValues(PreparedStatementValueSetter setter) throws SQLException {
		arrays(setter, INTEGER_ARRAY, STRING_ARRAY, INTEGER_ARRAY, STRING_ARRAY);
	}

	@ParameterizedTest(name = "set {0}")
	@EnumSource(value = PreparedStatementValueSetter.class)
	void emptyIntegerAndStringValues(PreparedStatementValueSetter setter) throws SQLException {
		arrays(setter, new int[0], new String[0], new Integer[0], new String[0]);
	}

	@ParameterizedTest(name = "set {0}")
	@EnumSource(value = PreparedStatementValueSetter.class)
	void emptyIntAndStringValues(PreparedStatementValueSetter setter) throws SQLException {
		arrays(setter, new Integer[0], new String[0], new Integer[0], new String[0]);
	}

	@ParameterizedTest(name = "set {0}")
	@EnumSource(value = PreparedStatementValueSetter.class)
	void integerAndStringWithNullValues(PreparedStatementValueSetter setter) throws SQLException {
		arrays(setter, new Integer[] {5, null, 6}, new String[] {"hi", null}, new Integer[] {5, null, 6}, new String[] {"hi", null});
	}

	@ParameterizedTest(name = "set {0}")
	@EnumSource(value = PreparedStatementValueSetter.class)
	void nullArrays(PreparedStatementValueSetter setter) throws SQLException {
		arrays(setter, null, null, null, null);
	}

	private void arrays(PreparedStatementValueSetter setter, Object intArray, Object stringArray, Integer[] expectedIntArray, String[] expectedStringArray) throws SQLException {
		try (Connection connection = createConnection()) {

			try (PreparedStatement insert = connection.prepareStatement("INSERT INTO prepared_statement_test_array(intarray, textarray) VALUES (?, ?)")) {
				setter.set(insert, 1, setter.create(FireboltDataType.INTEGER, intArray));
				setter.set(insert, 2, setter.create(FireboltDataType.TEXT, stringArray));
				assertFalse(insert.execute());
			}

			try (PreparedStatement select = connection.prepareStatement("SELECT intarray, textarray FROM prepared_statement_test_array")) {
				try (ResultSet rs = select.executeQuery()) {
					while(rs.next()) {
						validateArrayUsingGetObject(rs, 1, expectedIntArray);
						validateArrayUsingGetArray(rs, 1, Types.INTEGER, JDBCType.INTEGER.getName().toLowerCase(), expectedIntArray);
						validateArrayUsingGetObject(rs, 2, expectedStringArray);
						validateArrayUsingGetArray(rs, 2, Types.VARCHAR, "text", expectedStringArray);
					}
				}
			}
		}
	}

	@Test
	void nullableBiDimensionalArray() throws SQLException {
		try (Connection connection = createConnection(); Statement statement = connection.createStatement()) {
			try {
				statement.executeUpdate("create table b6 (i int, x array(array(int NULL) NULL) NULL)");
				statement.executeUpdate("insert into b6 (i, x) values (0, [[1, 2], [3]]), (1, [[4, NULL, 5]]), (2, [[4], NULL, [5, NULL, 6]]), (3, NULL), (4, [[NULL,7]])");
				try (ResultSet rs = statement.executeQuery("select i, x from  b6")) {
					List<Integer[][]> result = new ArrayList<>();
					while(rs.next()) {
						int index = rs.getInt(1);
						Array value = rs.getArray(2);
						Integer[][] values = value == null ? null : (Integer[][])value.getArray();
						result.add(index, values);
					}
					List<Integer[][]> expected = Arrays.asList(new Integer[][] {{1, 2}, {3}}, new Integer[][] {{4, null, 5}}, new Integer[][] {{4}, null, {5, null, 6}}, null, new Integer[][] {{null, 7}});
					assertEquals(expected.size(), result.size());
					for (int i = 0; i < expected.size(); i++) {
						assertArrayEquals(expected.get(i), result.get(i));
					}
				}
			} finally {
				statement.executeUpdate("drop table b6");
			}
		}
	}

	private <T> void validateArrayUsingGetObject(ResultSet rs, int index, T[] expected) throws SQLException {
		assertSqlArray(rs.getObject(index), expected);
	}

	@SuppressWarnings("unchecked")
	private <T> void assertSqlArray(Object actual, T[] expected) {
		if (expected == null) {
			assertNull(actual);
			return;
		}
		assertEquals(expected.getClass(), actual.getClass());
		assertArrayEquals(expected, (T[]) actual);
	}

	private <T> void validateArrayUsingGetArray(ResultSet rs, int index, int expectedBaseType, String expectedBaseTypeName, T[] expected) throws SQLException {
		Array array = rs.getArray(index);
		if (expected == null) {
			assertNull(array);
			return;
		}
		assertEquals(expectedBaseType, array.getBaseType());
		assertEquals(expectedBaseTypeName, array.getBaseTypeName());
		assertSqlArray(array.getArray(), expected);
	}
}
