package com.firebolt.jdbc.connection;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Properties;

import org.junit.jupiter.api.Test;

class FireboltJdbcUrlUtilTest {

	@Test
	void shouldGetAllPropertiesFromUri() {
		String uri = "jdbc:firebolt://api.dev.firebolt.io:123/Tutorial_11_05?use_standard_sql=0&account=firebolt";
		Properties properties = FireboltJdbcUrlUtil.extractProperties(uri);

		Properties expectedProperties = new Properties();
		expectedProperties.put("path", "/Tutorial_11_05");
		expectedProperties.put("host", "api.dev.firebolt.io");
		expectedProperties.put("port", "123");
		expectedProperties.put("use_standard_sql", "0");
		expectedProperties.put("account", "firebolt");

		assertEquals(expectedProperties, properties);
	}
}
