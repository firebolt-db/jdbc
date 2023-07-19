package integration.tests;

import integration.IntegrationTest;
import lombok.CustomLog;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.sql.Array;
import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

@CustomLog
class PreparedStatementArrayTest extends IntegrationTest {
	enum PreparedStatementValueSetter {
		ARRAY {
			@Override
			void set(PreparedStatement ps, int index, Object value) throws SQLException {
				ps.setArray(index, (Array)value);
			}
		},
		OBJECT {
			@Override
			void set(PreparedStatement ps, int index, Object value) throws SQLException {
				ps.setObject(index, value);
			}
		},
		;

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


	@ParameterizedTest
	@EnumSource(value = PreparedStatementValueSetter.class)
	void integerAndStringValues() throws SQLException {
		arrays(PreparedStatementValueSetter.OBJECT, INT_ARRAY, STRING_ARRAY, INTEGER_ARRAY, STRING_ARRAY);
	}

	@ParameterizedTest
	@EnumSource(value = PreparedStatementValueSetter.class)
	void intAndStringValues() throws SQLException {
		arrays(PreparedStatementValueSetter.OBJECT, INTEGER_ARRAY, STRING_ARRAY, INTEGER_ARRAY, STRING_ARRAY);
	}


	@ParameterizedTest
	@EnumSource(value = PreparedStatementValueSetter.class)
	void emptyIntegerAndStringValues() throws SQLException {
		arrays(PreparedStatementValueSetter.OBJECT, new int[0], new String[0], new Integer[0], new String[0]);
	}

	@ParameterizedTest
	@EnumSource(value = PreparedStatementValueSetter.class)
	void emptyIntAndStringValues() throws SQLException {
		arrays(PreparedStatementValueSetter.OBJECT, new Integer[0], new String[0], new Integer[0], new String[0]);
	}

	@ParameterizedTest
	@EnumSource(value = PreparedStatementValueSetter.class)
	void integerAndStringWithNullValues() throws SQLException {
		arrays(PreparedStatementValueSetter.OBJECT, new Integer[] {5, null, 6}, new String[] {"hi", null}, new Integer[] {5, null, 6}, new String[] {"hi", null});
	}


	@ParameterizedTest
	@EnumSource(value = PreparedStatementValueSetter.class)
	void nullArrays(PreparedStatementValueSetter setter) throws SQLException {
		arrays(setter, null, null, null, null);
	}

	private void arrays(PreparedStatementValueSetter setter, Object intArray, Object stringArray, Integer[] expectedIntArray, String[] expectedStringArray) throws SQLException {
		try (Connection connection = createConnection()) {

			try (PreparedStatement insert = connection.prepareStatement("INSERT INTO prepared_statement_test(intarray, textarray) VALUES (?, ?)")) {
				setter.set(insert, 1, intArray);
				setter.set(insert, 2, stringArray);
				assertFalse(insert.execute());
			}

			try (PreparedStatement select = connection.prepareStatement("SELECT intarray, textarray FROM prepared_statement_test")) {
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
