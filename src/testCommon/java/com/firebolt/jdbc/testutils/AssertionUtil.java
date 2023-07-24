package com.firebolt.jdbc.testutils;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.ResultSet;
import java.sql.SQLException;

public abstract class AssertionUtil {

	public static void assertResultSetValuesEquality(ResultSet expected, ResultSet actual) throws SQLException {
		while (expected.next()) {
			assertTrue(actual.next(), "The actual resultSet has less rows than expected");
			for (int i = 0; i < expected.getMetaData().getColumnCount(); i++) {
				assertEquals(expected.getObject(i + 1), actual.getObject(i + 1));
			}
		}
		assertFalse(actual.next());
	}

	public static void assertResultSetEquality(ResultSet expected, ResultSet actual) throws SQLException {
		assertEquals(expected.getMetaData(), actual.getMetaData());
		assertResultSetValuesEquality(expected, actual);
	}
}
