package com.firebolt.jdbc.type.array;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.firebolt.jdbc.type.FireboltDataType;

class FireboltArrayTest {

	private static final String[] ARRAY = new String[] { "Hello" };

	@Test
	void shouldReturnBaseTypeName() {
		FireboltArray fireboltArray = FireboltArray.builder().type(FireboltDataType.STRING).array(ARRAY).build();
		assertEquals("String", fireboltArray.getBaseTypeName());
	}

	@Test
	void shouldReturnBaseType() {
		FireboltArray fireboltArray = FireboltArray.builder().type(FireboltDataType.STRING).array(ARRAY).build();
		assertEquals(FireboltDataType.STRING.getSqlType(), fireboltArray.getBaseType());
	}

	@Test
	void shouldThrowExceptionIfGettingArrayWhenItIsFree() {
		FireboltArray fireboltArray = FireboltArray.builder().type(FireboltDataType.STRING).array(ARRAY).build();
		fireboltArray.free();
		assertThrows(SQLException.class, fireboltArray::getArray);
	}

	@Test
	void shouldReturnArrayWhenNoMapIsGiven() throws SQLException {
		FireboltArray fireboltArray = FireboltArray.builder().type(FireboltDataType.STRING).array(ARRAY).build();
		assertEquals(ARRAY, fireboltArray.getArray(null));
	}

	@Test
	void shouldThrowExceptionIfAMapIsGiven() throws SQLException {
		FireboltArray fireboltArray = FireboltArray.builder().type(FireboltDataType.STRING).array(ARRAY).build();
		Map<String, Class<?>> map = new HashMap<>();
		map.put("test", String.class);
		assertThrows(SQLException.class, () -> fireboltArray.getArray(map));
	}
}
