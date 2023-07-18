package integration.tests;

import integration.IntegrationTest;
import lombok.CustomLog;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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

@CustomLog
class PreparedStatementArrayTest extends IntegrationTest {

	@BeforeEach
	void beforeEach() {
		executeStatementFromFile("/statements/prepared-statement-array/ddl.sql");
	}

	@AfterEach
	void afterEach() {
		executeStatementFromFile("/statements/prepared-statement-array/cleanup.sql");
	}


	@Test
	void shouldInsertAndSelect() throws SQLException {
		try (Connection connection = createConnection()) {

			try (PreparedStatement insert = connection.prepareStatement("INSERT INTO prepared_statement_test(intarray) VALUES (?)")) {
				insert.setObject(1, new int[] {1,2,3});
				assertFalse(insert.execute());
			}

			try (PreparedStatement select = connection.prepareStatement("SELECT intarray FROM prepared_statement_test")) {
				try (ResultSet rs = select.executeQuery()) {
					while(rs.next()) {
						validateArrayUsingGetObject(rs, 1, new Integer[]{1, 2, 3});
						validateArrayUsingGetArray(rs, 1, Types.INTEGER, JDBCType.INTEGER.getName().toLowerCase(), new Integer[]{1, 2, 3});
					}
				}
			}
		}
	}

	private void validateArrayUsingGetObject(ResultSet rs, int index, Integer[] expected) throws SQLException {
		Object intArray = rs.getObject(index);
		assertEquals(Integer[].class, intArray.getClass());
		assertArrayEquals(expected, (Integer[]) intArray);
	}

	private void validateArrayUsingGetArray(ResultSet rs, int index, int expectedBaseType, String expectedBaseTypeName, Integer[] expected) throws SQLException {
		Array array = rs.getArray(index);
		assertEquals(expectedBaseType, array.getBaseType());
		assertEquals(expectedBaseTypeName, array.getBaseTypeName());
		Object intArray = array.getArray();
		assertEquals(Integer[].class, intArray.getClass());
		assertArrayEquals(expected, (Integer[]) intArray);
	}

}
